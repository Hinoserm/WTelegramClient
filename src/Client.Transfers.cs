using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using TL;

namespace WTelegram
{
	/// <summary>How file parts (uploads and downloads) are spread over multipath transport connections.</summary>
	public enum PathTransferMode
	{
		/// <summary>Every part goes on the path <see cref="Client.SendMode"/> picks for any request, at most
		/// <see cref="Client.ParallelTransfers"/> parts at once in all, with no speed measurement or slow-part copies
		/// (the old behaviour; frames are still written per path, outside the send lock).</summary>
		FollowSendMode,
		/// <summary>Upload and download speed are measured per WAN (local address), separately, from the parts
		/// themselves. Each part goes to the WAN expected to finish it first, counting what that WAN already has
		/// in flight, so every healthy WAN carries parts at once (up to <see cref="Client.TransferPartsPerPath"/>
		/// each) and a slow one gets none. A WAN that is not in use is re-measured now and then by giving it a
		/// part; a part that is slow on its WAN gets a copy on the fastest one. FLOOD_WAIT is honoured per WAN.</summary>
		Throughput,
	}

	public partial class Client
	{
		/// <summary>How file parts are spread over the paths (<see cref="SendMode"/> still routes every other request).</summary>
		public PathTransferMode TransferMode { get; set; } = PathTransferMode.Throughput;
		/// <summary>Throughput mode: file parts in flight at once per WAN, per direction.</summary>
		public int TransferPartsPerPath { get; set; } = 4;
		/// <summary>Throughput mode: seconds after which a WAN that carried no part in a direction is measured again.</summary>
		public int TransferProbeInterval { get; set; } = 30;
		/// <summary>Throughput mode: a part answered FLOOD_WAIT_X waits out X (that WAN takes nothing in that direction
		/// meanwhile) and goes again, unless X is above this many seconds: the transfer then fails with the error.</summary>
		public int TransferMaxFloodWait { get; set; } = 60;

		internal const int TransferUp = 0, TransferDown = 1;
		private const int MaxTransferFloods = 5; // FLOOD_WAITs honoured for one part before its transfer fails with the error
		private const int MinSampleBytes = 64 * 1024; // smaller parts (a file's last one) time mostly latency
		private const double SampleWeight = 0.3; // EWMA weight of a new speed sample
		private const double SlowSampleWeight = 0.6; // ... of one slower than the average
		private const double BenchRatio = 0.5; // a WAN under this share of the best one's speed takes no parts (but probes)
		private const double RescueRatio = 0.75; // a part on a WAN under this share of the best one's speed may get a copy

		/// <summary>The lease of the part being sent from this async flow: Invoke reads it (see UploadFileAsync).</summary>
		private static readonly AsyncLocal<TransferLease> CurrentTransfer = new();

		/// <summary>One part's place on a WAN, from the scheduler until its answer. Released exactly once.</summary>
		private sealed class TransferLease
		{
			internal Client Client; // whose path PathIndex is
			internal int PathIndex = -1; // -1: no path chosen (no live path, or not multipath)
			internal int Dir;
			internal WanStats Wan;
			internal long StallExtraMs;
			internal long StartTicks; // when it was handed out
			internal int Bytes;
			internal int released;
			internal WanStats CopyWan; // a copy of this part counted in that WAN's InFlight (one at most), until released
			internal long QueuedTicks; // when its part's current attempt was queued on a path (0: not yet); Volatile
		}

		/// <summary>Per WAN (local address), per direction: measured speed, parts in flight, FLOOD_WAIT state.
		/// Kept on the main client and shared by its media-DC clients: a WAN's speed is the WAN's, whatever the DC.</summary>
		private sealed class WanStats
		{
			internal readonly IPAddress Address;
			internal WanStats(IPAddress address) => Address = address;
			internal readonly double[] Bps = new double[2]; // EWMA bytes/s; 0 = not measured yet
			internal readonly int[] Samples = new int[2];
			internal readonly long[] LastAckTicks = new long[2]; // last answer that gave a sample (queue start)
			internal readonly long[] LastSampleTicks = new long[2];
			internal readonly long[] LastProbeTicks = new long[2];
			internal readonly int[] InFlight = new int[2];
			// the parts in flight, per direction: the oldest one's progress so far bounds the speed estimate
			internal readonly System.Collections.Generic.List<TransferLease>[] Active = [new(), new()];
			internal readonly long[] FloodUntilTicks = new long[2];
			internal readonly int[] CapCut = new int[2]; // parts taken off TransferPartsPerPath after a FLOOD_WAIT
			internal readonly long[] CapCutUntilTicks = new long[2];
			// for the once-a-minute summary
			internal readonly long[] StatParts = new long[2], StatBytes = new long[2], StatProbes = new long[2], StatFloods = new long[2];
		}

		private readonly ConcurrentDictionary<IPAddress, WanStats> _wanStats = new();
		private readonly object _transferLock = new();
		private TaskCompletionSource<bool> _transferPulse = new(TaskCreationOptions.RunContinuationsAsynchronously);
		private long _lastTransferStatsTicks;

		private Client RootClient => _parentClient?.RootClient ?? this;
		private static IPAddress WanKey(TransportPath path) => path?.LocalEndPoint?.Address ?? IPAddress.None;
		private WanStats Wan(IPAddress address) => RootClient._wanStats.GetOrAdd(address, a => new WanStats(a));

		/// <summary>Measured speed of a WAN in a direction (bytes/s), -1 if not measured yet.</summary>
		private double MeasuredBps(TransportPath path, int dir)
			=> RootClient._wanStats.TryGetValue(WanKey(path), out var w) && w.Bps[dir] > 0 ? w.Bps[dir] : -1;

		/// <summary>Picks the WAN for the next file part of <paramref name="client"/> (waiting while the best one is
		/// full, or every WAN is under FLOOD_WAIT). Release the lease with <see cref="ReleaseTransfer"/>.</summary>
		private async Task<TransferLease> AcquireTransferAsync(Client client, int dir, int bytes)
		{
			var root = RootClient;
			while (true)
			{
				Task pulse;
				lock (root._transferLock)
					pulse = root._transferPulse.Task; // before evaluating: a release after it wakes us up
				TransportPath[] paths;
				lock (client._pathsLock)
					paths = client._paths.Where(p => p.IsAlive && p.NetworkStream != null).ToArray();
				long now = Environment.TickCount64, waitMs = 250;
				lock (root._transferLock)
				{
					if (paths.Length == 0)
					{
						// no live path (single connection without multipath, or every path reconnecting):
						// no choice to make, only the cap; Invoke's own retries deal with the connection
						var none = Wan(IPAddress.None);
						if (none.InFlight[dir] < Cap(none, dir, now))
						{
							none.InFlight[dir]++;
							return new TransferLease { Client = client, Dir = dir, Wan = none };
						}
					}
					else
					{
						var cands = paths.Select(p => (Path: p, Wan: Wan(WanKey(p)))).Where(c => c.Wan.FloodUntilTicks[dir] <= now).ToArray();
						if (cands.Length == 0)
							waitMs = Math.Max(50, paths.Min(p => Wan(WanKey(p)).FloodUntilTicks[dir]) - now);
						else
						{
							double bestKnown = cands.Max(c => c.Wan.Bps[dir]);
							(TransportPath Path, WanStats Wan) best = default;
							double bestEta = double.MaxValue;
							foreach (var c in cands)
							{
								// not measured yet: assumed fast (twice the best known), so it carries parts at once and
								// gets measured; if it is slow, its first part in flight shows it within a second
								double bps = LiveBps(c.Wan, dir, now, c.Wan.Bps[dir] > 0 ? c.Wan.Bps[dir] : bestKnown * 2);
								// benched: under half the best WAN's speed, its parts would set the end of every file
								// (the probe below still measures it now and then)
								if (bps <= 0 || (c.Wan.Bps[dir] > 0 && c.Wan.Bps[dir] < bestKnown * BenchRatio))
									continue;
								double eta = (c.Wan.InFlight[dir] + 1) * (double)bytes / bps * 1000;
								if (eta < bestEta)
									(best, bestEta) = (c, eta);
							}
							if (best.Path == null) // nothing measured yet: spread (fewest parts in flight, then lowest latency),
							{                      // so every WAN carries one and is measured from the first file
								best = cands.OrderBy(c => c.Wan.InFlight[dir])
									.ThenBy(c => Volatile.Read(ref c.Path.LatencyEwmaMs) == long.MaxValue ? long.MaxValue
										: Volatile.Read(ref c.Path.LatencyEwmaMs) + Volatile.Read(ref c.Path.PenaltyMs)).First();
								bestEta = 0;
							}
							// Probe: a WAN not measured for TransferProbeInterval, with nothing in flight, carries this part
							// itself (a copy's answer cannot be told from the original's: Telegram answers on both
							// connections). If it is slow, TransferCopyDue copies the part to the fast one in short order.
							long probeMs = Math.Max(1, TransferProbeInterval) * 1000L;
							foreach (var c in cands)
								if (c.Wan != best.Wan && c.Wan.InFlight[dir] == 0 && now - c.Wan.LastSampleTicks[dir] >= probeMs
									&& now - c.Wan.LastProbeTicks[dir] >= probeMs)
								{
									c.Wan.LastProbeTicks[dir] = now;
									c.Wan.StatProbes[dir]++;
									(best, bestEta) = (c, 0);
									break;
								}
							if (best.Wan.InFlight[dir] < Cap(best.Wan, dir, now))
							{
								best.Wan.InFlight[dir]++;
								var lease = new TransferLease { Client = client, PathIndex = best.Path.PathIndex, Dir = dir, Wan = best.Wan,
									StallExtraMs = (long)Math.Min(2 * bestEta, 120_000), StartTicks = now, Bytes = bytes };
								best.Wan.Active[dir].Add(lease);
								return lease;
							}
							// the best WAN is full: wait for one of its parts (or for a better estimate)
						}
					}
				}
				using var delayCts = new CancellationTokenSource(); // the timer is dropped as soon as a pulse comes
				await Task.WhenAny(pulse, Task.Delay((int)Math.Min(waitMs, 5000), delayCts.Token));
				delayCts.Cancel();
			}
		}

		/// <summary>The WAN's speed (<paramref name="bps"/>: measured, or assumed), unless its oldest part in flight shows
		/// it slower right now (bytes / age so far): a WAN that just slowed down stops getting parts within a second, not
		/// after several slow answers. Only the oldest queued one, aged from its queuing: the others wait behind it, so
		/// their age is queueing, not slowness. 0 = no estimate. Caller holds _transferLock.</summary>
		private static double LiveBps(WanStats wan, int dir, long now, double bps)
		{
			if (bps <= 0 || wan.Active[dir].Count == 0)
				return bps;
			var oldest = OldestQueued(wan, dir, out long queuedAt); // not yet queued: no time on the WAN
			if (oldest == null)
				return bps;
			long age = now - queuedAt;
			return age > 250 ? Math.Min(bps, oldest.Bytes * 1000.0 / age) : bps;
		}

		/// <summary>The WAN's part in flight queued first (null: none queued), and when. Caller holds _transferLock.</summary>
		private static TransferLease OldestQueued(WanStats wan, int dir, out long queuedAt)
		{
			TransferLease oldest = null;
			queuedAt = 0;
			foreach (var l in wan.Active[dir])
				if (Volatile.Read(ref l.QueuedTicks) is long q and > 0 && (oldest == null || q < queuedAt))
					(oldest, queuedAt) = (l, q);
			return oldest;
		}

		/// <summary>A copy of a file part queued on <paramref name="path"/> (it is now the request's carrier copy): counted in
		/// that WAN's parts in flight (so the scheduler and the rescue rule see its load), one copy per part: a newer copy
		/// on another WAN moves the count there. Until the part's lease is released, or the counted copy fails.</summary>
		private void CountTransferCopy(Rpc rpc, TransportPath path)
		{
			if (rpc.transferDir < 0 || rpc.lease is not TransferLease lease)
				return;
			var root = RootClient;
			lock (root._transferLock)
			{
				if (Volatile.Read(ref lease.released) != 0)
					return;
				var w = Wan(WanKey(path));
				if (lease.CopyWan == w)
					return;
				UncountTransferCopyLocked(lease); // the previous copy no longer carries it
				if (w == lease.Wan)
					return; // a copy on its own WAN adds nothing to count
				w.InFlight[lease.Dir]++;
				lease.CopyWan = w;
			}
		}

		/// <summary>A file part's frame was queued on a path: from now on its lease's age is transfer time. Set again for each
		/// attempt (Invoke retries with a new request under the same lease: a failed attempt is no time on the WAN).</summary>
		private static void MarkTransferQueued(Rpc rpc, long now)
		{
			if (rpc?.lease is TransferLease lease)
				Volatile.Write(ref lease.QueuedTicks, now); // read under _transferLock; no lock needed to publish it
		}

		/// <summary>Undoes <see cref="CountTransferCopy"/> (caller holds no transfer lock).</summary>
		private void UncountTransferCopy(Rpc rpc)
		{
			if (rpc.lease is not TransferLease lease)
				return;
			lock (RootClient._transferLock)
				UncountTransferCopyLocked(lease);
		}

		private static void UncountTransferCopyLocked(TransferLease lease)
		{
			if (lease.CopyWan is WanStats w)
			{
				w.InFlight[lease.Dir]--;
				lease.CopyWan = null;
			}
		}

		private int Cap(WanStats wan, int dir, long now)
			=> Math.Max(1, TransferPartsPerPath - (wan.CapCutUntilTicks[dir] > now ? wan.CapCut[dir] : 0));

		private void ReleaseTransfer(TransferLease lease)
		{
			if (lease == null || Interlocked.Exchange(ref lease.released, 1) != 0)
				return;
			var root = RootClient;
			TaskCompletionSource<bool> pulse;
			lock (root._transferLock)
			{
				lease.Wan.InFlight[lease.Dir]--;
				lease.Wan.Active[lease.Dir].Remove(lease);
				UncountTransferCopyLocked(lease);
				pulse = root._transferPulse;
				root._transferPulse = new(TaskCreationOptions.RunContinuationsAsynchronously);
			}
			pulse.TrySetResult(true);
		}

		/// <summary>A part was answered FLOOD_WAIT_<paramref name="seconds"/> on its lease's WAN: that WAN takes no part in
		/// that direction for that long, and one part fewer at a time for 5 minutes.</summary>
		private void OnTransferFlood(TransferLease lease, int seconds)
		{
			var root = RootClient;
			long now = Environment.TickCount64;
			lock (root._transferLock)
			{
				var w = lease.Wan;
				w.FloodUntilTicks[lease.Dir] = Math.Max(w.FloodUntilTicks[lease.Dir], now + Math.Max(1, seconds) * 1000L);
				w.CapCut[lease.Dir] = Math.Min(Math.Max(0, TransferPartsPerPath - 1), (w.CapCutUntilTicks[lease.Dir] > now ? w.CapCut[lease.Dir] : 0) + 1);
				w.CapCutUntilTicks[lease.Dir] = now + 300_000;
				w.StatFloods[lease.Dir]++;
			}
			Helpers.Log(3, $"{_dcSession?.DcID}>FLOOD_WAIT {seconds}s on {(lease.Dir == TransferUp ? "upload" : "download")} via {lease.Wan.Address}: " +
				$"that WAN rests {seconds}s, then takes {Cap(lease.Wan, lease.Dir, now)} part(s) at a time for 5 min");
		}

		/// <summary>A file part (at least 750 ms after queuing) on a WAN measured under <see cref="RescueRatio"/> of the best
		/// other one (or not measured itself), older than three times what that one would need for it (with what it has
		/// in flight): it gets a copy there (see CopyStalledRpcsAsync), so a slow WAN never holds up the end of a file.
		/// The other WANs are ranked by <see cref="BestCopyTarget"/> (one not measured yet counts as twice this part's
		/// WAN, if that one is measured). With another live WAN but nothing measured on either, a part 2 s old is
		/// copied; with no other WAN, never.</summary>
		private bool TransferCopyDue(Rpc rpc, long now)
		{
			if (rpc.transferDir < 0 || rpc.transferBytes <= 0)
				return false;
			// from its queuing: on a slow uplink a part can wait seconds behind the path's earlier writes
			long age = now - Volatile.Read(ref rpc.queuedTicks);
			if (age < 750)
				return false;
			TransportPath[] paths;
			lock (_pathsLock)
				paths = _paths.Where(p => p.IsAlive && p.NetworkStream != null).ToArray();
			var own = paths.FirstOrDefault(p => p.PathIndex == rpc.sentPathIndex);
			if (own == null)
				return false; // its path is gone: the rescue on path loss deals with it
			int dir = rpc.transferDir;
			var (best, bestOther, otherInFlight, ownBps) = BestCopyTarget(paths, own, dir, now);
			if (best == null)
				return false; // no other WAN to copy to
			if (bestOther <= 0) // nothing measured anywhere (right after a start): a part 2 s old is copied
				return age > 2000;
			if (ownBps > 0 && ownBps >= bestOther * RescueRatio)
				return false;
			// By age alone, against what the fast WAN would need (with what it has in flight). Not against its own WAN's
			// estimate: that reads several times too high on a lossy link (bursts get through), and a part queued behind
			// others on a slow WAN is exactly one the fast WAN should take (tested: trusting it, capped sends went from
			// ~1.5 s to 17-45 s).
			double fastMs = (otherInFlight + 1) * rpc.transferBytes * 1000.0 / bestOther;
			return age > 3 * fastMs;
		}

		/// <summary>FLOOD_WAIT_X / FLOOD_PREMIUM_WAIT_X (error 420) on a file part.</summary>
		internal static bool IsTransferFlood(RpcException ex) => ex.Code == 420 && ex.Message.Contains("WAIT");

		/// <summary>Honours a FLOOD_WAIT on a part: that WAN rests (see <see cref="OnTransferFlood"/>) and the part gets a
		/// new lease, which waits until some WAN may take it. A wait above <see cref="TransferMaxFloodWait"/> is the
		/// transfer's error.</summary>
		private async Task<TransferLease> AfterTransferFloodAsync(Client client, TransferLease lease, RpcException ex, int bytes)
		{
			int seconds = Math.Max(1, ex.X);
			if (seconds > TransferMaxFloodWait)
				System.Runtime.ExceptionServices.ExceptionDispatchInfo.Throw(ex); // keeps its stack
			OnTransferFlood(lease, seconds);
			ReleaseTransfer(lease);
			return await AcquireTransferAsync(client, lease.Dir, bytes);
		}

		/// <summary>A file part was answered (its first answer, on <paramref name="recvPathIndex"/>): a speed sample for the
		/// WAN that unambiguously carried it, if any (see below). Time runs from its queuing, or from that WAN's previous
		/// answer if it was still busy then (parts queue behind each other on one connection).</summary>
		private void RecordTransferSample(Rpc rpc, int recvPathIndex, int bytes)
		{
			int dir = rpc.transferDir;
			if (dir < 0 || bytes < MinSampleBytes)
				return;
			// Telegram may answer on any connection, and answers a copied request on all of them, so only an
			// unambiguous carrier gives a sample: an upload part never copied (the WAN it was written on), or
			// download data (the WAN it arrived on, which is the one that carried it).
			int carrier = dir == TransferUp ? (rpc.copyAttempted ? -1 : rpc.sentPathIndex)
				: recvPathIndex >= 0 ? recvPathIndex : rpc.sentPathIndex;
			// timed from its queuing (or the WAN's previous answer, if later: see AddTransferSample), not from the end of
			// its write: with a send buffer smaller than the part, the write covers most of the transfer
			long queued = Volatile.Read(ref rpc.queuedTicks);
			AddTransferSample(carrier, dir, bytes, queued > 0 ? queued : Volatile.Read(ref rpc.writtenTicks), done: true);
		}

		/// <summary>A file part being copied because its WAN is slow: if it is that WAN's oldest part in flight, its size over
		/// the time since it was queued is a ceiling of the WAN's speed, which only ever lowers the estimate (see
		/// AddTransferSample), so the scheduler learns the WAN is slow even though no answer will say so. (Not from the
		/// write: on a slow uplink even the first part's write takes seconds.)</summary>
		private void RecordSlowTransfer(Rpc rpc)
		{
			if (rpc.transferDir < 0 || rpc.transferBytes < MinSampleBytes || Volatile.Read(ref rpc.queuedTicks) <= 0 || rpc.lease is not TransferLease lease)
				return;
			TransportPath sent;
			lock (_pathsLock)
				sent = _paths.FirstOrDefault(p => p.PathIndex == rpc.sentPathIndex);
			if (sent == null)
				return;
			lock (RootClient._transferLock)
			{
				// only on the WAN its lease counts it on (it may have gone out elsewhere if that path died), and only
				// that WAN's oldest part actually queued: its age is its own time, not a wait behind earlier parts
				if (Wan(WanKey(sent)) != lease.Wan)
					return;
				if (OldestQueued(lease.Wan, rpc.transferDir, out _) != lease)
					return;
			}
			AddTransferSample(rpc.sentPathIndex, rpc.transferDir, rpc.transferBytes, Volatile.Read(ref rpc.queuedTicks), done: false);
		}

		/// <param name="done">An answered part: its time runs from <paramref name="start"/> (its queuing), or from the WAN's
		/// previous answer if that came later (parts queue behind each other on one connection). Otherwise a ceiling taken
		/// mid-way, which only ever lowers the estimate.</param>
		private void AddTransferSample(int pathIndex, int dir, int bytes, long start, bool done)
		{
			if (pathIndex < 0 || start <= 0)
				return;
			TransportPath path;
			lock (_pathsLock)
				path = _paths.FirstOrDefault(p => p.PathIndex == pathIndex);
			if (path == null)
				return;
			var root = RootClient;
			var w = Wan(WanKey(path));
			long now = Environment.TickCount64;
			double sample;
			lock (root._transferLock)
			{
				long from = done ? Math.Max(start, w.LastAckTicks[dir]) : start;
				if (done)
					w.LastAckTicks[dir] = now;
				sample = bytes * 1000.0 / Math.Max(1, now - from);
				// a bound taken mid-way is a ceiling: it can only lower an estimate, never raise one. It may be the first:
				// a WAN whose parts are always rescued gives no other sample (copied parts are ambiguous), and unmeasured
				// it would count as fast and keep getting parts (tested: 68 rescues a minute, sends 4x slower)
				if (!done && w.Samples[dir] > 0 && sample >= w.Bps[dir])
					return;
				// a slowdown counts at once, a recovery gradually: a WAN that just got slow must not keep taking parts
				double weight = sample < w.Bps[dir] ? SlowSampleWeight : SampleWeight;
				w.Bps[dir] = w.Samples[dir] == 0 ? sample : w.Bps[dir] * (1 - weight) + sample * weight;
				w.Samples[dir]++;
				w.LastSampleTicks[dir] = now;
				if (done)
				{
					w.StatParts[dir]++;
					w.StatBytes[dir] += bytes;
				}
			}
			Helpers.Log(1, $"{_dcSession?.DcID}>{(dir == TransferUp ? "Up" : "Down")} {bytes / 1024} KB via {w.Address} [P{pathIndex}]" +
				$"{(done ? "" : " (still going, copied)")}: {sample / 1e6 * 8:F1} Mbit/s (EWMA {w.Bps[dir] / 1e6 * 8:F1})");
		}

		/// <summary>For a stalled file part: the live path on the WAN fastest in its direction right now, other than the one
		/// of <paramref name="avoidIndex"/> (see <see cref="BestCopyTarget"/>: a WAN whose parts are stuck, gone dark and
		/// not yet detected, is not picked).</summary>
		private TransportPath PickTransferCopyPath(int avoidIndex, int dir)
		{
			TransportPath[] paths;
			lock (_pathsLock)
				paths = _paths.Where(p => p.IsAlive && p.NetworkStream != null).ToArray();
			var own = paths.FirstOrDefault(p => p.PathIndex == avoidIndex);
			var best = BestCopyTarget(paths, own, dir, Environment.TickCount64).Path;
			// none on another WAN (or not under FLOOD_WAIT): any other live path, lowest latency first
			return best ?? paths.Where(p => p != own).OrderBy(p => Volatile.Read(ref p.LatencyEwmaMs)).FirstOrDefault();
		}

		/// <summary>The one ranking of copy targets, shared by <see cref="TransferCopyDue"/> and
		/// <see cref="PickTransferCopyPath"/>: among live paths on another WAN than <paramref name="own"/>'s, not under
		/// FLOOD_WAIT, the fastest right now (measured speed bounded by its oldest part in flight; a WAN not measured
		/// yet counts as twice the own WAN's speed, as in the scheduler), then the lowest latency.</summary>
		/// <returns>The path (null: none), its speed (0: nothing measured anywhere), its WAN's parts in flight, and the
		/// own WAN's measured speed</returns>
		private (TransportPath Path, double Bps, int InFlight, double OwnBps) BestCopyTarget(TransportPath[] paths, TransportPath own, int dir, long now)
		{
			var root = RootClient;
			lock (root._transferLock)
			{
				var ownWan = own != null ? Wan(WanKey(own)) : null;
				double ownBps = ownWan?.Bps[dir] ?? 0;
				(TransportPath Path, double Bps, int InFlight) best = (null, -1, 0);
				foreach (var p in paths)
				{
					var w = Wan(WanKey(p));
					if (p == own || w == ownWan || w.FloodUntilTicks[dir] > now)
						continue;
					bool measured = w.Bps[dir] > 0;
					double bps = measured ? LiveBps(w, dir, now, w.Bps[dir]) : ownBps * 2;
					if (best.Path == null || bps > best.Bps || (bps == best.Bps
						&& Volatile.Read(ref p.LatencyEwmaMs) < Volatile.Read(ref best.Path.LatencyEwmaMs)))
						best = (p, bps, measured ? w.InFlight[dir] : 0); // an assumed speed has no load to go with it
				}
				return (best.Path, Math.Max(0, best.Bps), best.InFlight, ownBps);
			}
		}

		/// <summary>Once a minute on the main client, if any part moved: per WAN and direction, parts, bytes, measured
		/// speed, probes and FLOOD_WAITs.</summary>
		private void LogTransferStats()
		{
			var root = RootClient;
			long now = Environment.TickCount64;
			long last = Volatile.Read(ref root._lastTransferStatsTicks);
			if (now - last < 60_000 || Interlocked.CompareExchange(ref root._lastTransferStatsTicks, now, last) != last)
				return;
			var sb = new StringBuilder();
			lock (root._transferLock)
			{
				foreach (var dir in new[] { TransferUp, TransferDown })
				{
					var wans = root._wanStats.Values.Where(w => w.StatParts[dir] + w.StatProbes[dir] + w.StatFloods[dir] > 0)
						.OrderBy(w => w.Address.ToString()).ToArray();
					if (wans.Length == 0)
						continue;
					sb.Append(sb.Length == 0 ? "" : " | ").Append(dir == TransferUp ? "up:" : "down:");
					foreach (var w in wans)
					{
						sb.Append($" {w.Address} {w.StatParts[dir]} part(s) {w.StatBytes[dir] / 1e6:F1} MB @ {(w.Bps[dir] > 0 ? $"{w.Bps[dir] / 1e6 * 8:F1} Mbit/s" : "unmeasured")}");
						if (w.StatProbes[dir] > 0) sb.Append($", {w.StatProbes[dir]} probe(s)");
						if (w.StatFloods[dir] > 0) sb.Append($", {w.StatFloods[dir]} FLOOD_WAIT");
						sb.Append(';');
						w.StatParts[dir] = w.StatBytes[dir] = w.StatProbes[dir] = w.StatFloods[dir] = 0;
					}
				}
			}
			if (sb.Length > 0)
				Helpers.Log(2, $"File transfers in the last minute: {sb}");
		}
	}
}
