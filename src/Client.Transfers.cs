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
		/// <summary>Every part goes on the path <see cref="Client.SendMode"/> picks for any request (the old behaviour),
		/// at most <see cref="Client.ParallelTransfers"/> parts at once in all.</summary>
		FollowSendMode,
		/// <summary>Upload and download speed are measured per WAN (local address), separately, from the parts
		/// themselves. Each part goes to the WAN expected to finish it first, counting what that WAN already has
		/// in flight, so every healthy WAN carries parts at once (up to <see cref="Client.TransferPartsPerPath"/>
		/// each) and a slow one gets none. A WAN that is not in use is re-measured now and then by sending a copy
		/// of a part on it as well (the part itself never waits for it). FLOOD_WAIT is honoured per WAN.</summary>
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
		private const int MinSampleBytes = 64 * 1024; // smaller parts (a file's last one) time mostly latency
		private const double SampleWeight = 0.3; // EWMA weight of a new speed sample
		private const double SlowSampleWeight = 0.6; // ... of one slower than the average

		/// <summary>The lease of the part being sent from this async flow: Invoke reads it (see UploadFileAsync).</summary>
		private static readonly AsyncLocal<TransferLease> CurrentTransfer = new();

		/// <summary>One part's place on a WAN, from the scheduler until its answer. Released exactly once.</summary>
		private sealed class TransferLease
		{
			internal Client Client; // whose path PathIndex is
			internal int PathIndex = -1; // -1: no path chosen (no live path, or not multipath)
			internal int Dir;
			internal WanStats Wan;
			internal TransportPath ProbePath; // also send a copy here, to measure it
			internal long StallExtraMs;
			internal long StartTicks; // when it was handed out
			internal int Bytes;
			internal int released;
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
			internal readonly int[] ProbesUnsampled = new int[2]; // probes since the last sample
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
								if (bps <= 0)
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
							if (best.Wan.InFlight[dir] < Cap(best.Wan, dir, now))
							{
								best.Wan.InFlight[dir]++;
								var lease = new TransferLease { Client = client, PathIndex = best.Path.PathIndex, Dir = dir, Wan = best.Wan,
									StallExtraMs = (long)Math.Min(2 * bestEta, 120_000), StartTicks = now, Bytes = bytes };
								best.Wan.Active[dir].Add(lease);
								long probeMs = Math.Max(1, TransferProbeInterval) * 1000L;
								foreach (var c in cands)
									if (c.Wan != best.Wan && now - c.Wan.LastSampleTicks[dir] >= probeMs && now - c.Wan.LastProbeTicks[dir] >= probeMs)
									{
										c.Wan.LastProbeTicks[dir] = now;
										c.Wan.ProbesUnsampled[dir]++;
										c.Wan.StatProbes[dir]++;
										lease.ProbePath = c.Path;
										break;
									}
								return lease;
							}
							// the best WAN is full: wait for one of its parts (or for a better estimate)
						}
					}
				}
				await Task.WhenAny(pulse, Task.Delay((int)Math.Min(waitMs, 5000)));
			}
		}

		/// <summary>The WAN's speed (<paramref name="bps"/>: measured, or assumed), unless a part in flight shows it slower
		/// right now (bytes / age so far): a WAN that just slowed down stops getting parts within a second, not after
		/// several slow answers. 0 = no estimate. Caller holds _transferLock.</summary>
		private static double LiveBps(WanStats wan, int dir, long now, double bps)
		{
			if (bps <= 0)
				return bps;
			foreach (var lease in wan.Active[dir])
			{
				long age = now - lease.StartTicks;
				if (age > 250 && lease.Bytes * 1000.0 / age < bps)
					bps = lease.Bytes * 1000.0 / age;
			}
			return bps;
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

		/// <summary>A file part still unanswered on a WAN measured at under half the best other one, after three times
		/// what that one would need (and at least 750 ms): it gets a copy there (see CopyStalledRpcsAsync), so a slow
		/// WAN never holds up the end of a file.</summary>
		private bool TransferCopyDue(Rpc rpc, long now)
		{
			if (rpc.transferDir < 0 || rpc.transferBytes <= 0)
				return false;
			long age = now - Volatile.Read(ref rpc.writtenTicks);
			if (age < 750)
				return false;
			TransportPath[] paths;
			lock (_pathsLock)
				paths = _paths.Where(p => p.IsAlive && p.NetworkStream != null).ToArray();
			var own = paths.FirstOrDefault(p => p.PathIndex == rpc.sentPathIndex);
			if (own == null)
				return false; // its path is gone: the rescue on path loss deals with it
			var root = RootClient;
			int dir = rpc.transferDir;
			double ownBps, bestOther = 0;
			lock (root._transferLock)
			{
				ownBps = Wan(WanKey(own)).Bps[dir];
				foreach (var p in paths)
					if (p != own && Wan(WanKey(p)) is var w && w != Wan(WanKey(own)) && w.FloodUntilTicks[dir] <= now)
						bestOther = Math.Max(bestOther, w.Bps[dir]);
			}
			if (bestOther <= 0 || (ownBps > 0 && ownBps >= bestOther / 2))
				return false;
			return age > 3 * (rpc.transferBytes * 1000.0 / bestOther);
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
				throw ex;
			OnTransferFlood(lease, seconds);
			ReleaseTransfer(lease);
			return await AcquireTransferAsync(client, lease.Dir, bytes);
		}

		/// <summary>A file part's answer arrived on <paramref name="recvPathIndex"/>: if that is a path this request was
		/// written on (the original or its copy), a speed sample for its WAN. Time runs from that write, or from the
		/// WAN's previous answer if it was still busy then (parts queue behind each other on one connection).</summary>
		private void RecordTransferSample(Rpc rpc, int recvPathIndex, int bytes)
		{
			int dir = rpc.transferDir;
			if (dir < 0 || recvPathIndex < 0 || bytes < MinSampleBytes)
				return;
			long start;
			if (recvPathIndex == rpc.sentPathIndex && Interlocked.Exchange(ref rpc.sampledSent, 1) == 0)
				start = Volatile.Read(ref rpc.writtenTicks);
			else if (recvPathIndex == rpc.hedgedPathIndex && Interlocked.Exchange(ref rpc.sampledHedge, 1) == 0)
				start = Volatile.Read(ref rpc.hedgedWrittenTicks);
			else
				return;
			if (start <= 0)
				return;
			TransportPath path;
			lock (_pathsLock)
				path = _paths.FirstOrDefault(p => p.PathIndex == recvPathIndex);
			if (path == null)
				return;
			var root = RootClient;
			var w = Wan(WanKey(path));
			long now = Environment.TickCount64;
			lock (root._transferLock)
			{
				long from = Math.Max(start, w.LastAckTicks[dir]);
				w.LastAckTicks[dir] = now;
				double sample = bytes * 1000.0 / Math.Max(1, now - from);
				// a slowdown counts at once, a recovery gradually: a WAN that just got slow must not keep taking parts
				double weight = sample < w.Bps[dir] ? SlowSampleWeight : SampleWeight;
				w.Bps[dir] = w.Samples[dir] == 0 ? sample : w.Bps[dir] * (1 - weight) + sample * weight;
				w.Samples[dir]++;
				w.LastSampleTicks[dir] = now;
				w.ProbesUnsampled[dir] = 0;
				w.StatParts[dir]++;
				w.StatBytes[dir] += bytes;
			}
			Helpers.Log(1, $"{_dcSession?.DcID}>{(dir == TransferUp ? "Up" : "Down")} {bytes / 1024} KB via {w.Address} [P{recvPathIndex}]: " +
				$"{bytes * 1000.0 / Math.Max(1, now - start) / 1e6 * 8:F1} Mbit/s (EWMA {w.Bps[dir] / 1e6 * 8:F1})");
		}

		/// <summary>For a stalled file part: the live path (other than <paramref name="avoidIndex"/>) on the WAN measured
		/// fastest in its direction; not measured counts as slowest, then lowest latency.</summary>
		private TransportPath PickTransferCopyPath(int avoidIndex, int dir)
		{
			TransportPath[] paths;
			lock (_pathsLock)
				paths = _paths.Where(p => p.IsAlive && p.PathIndex != avoidIndex && p.NetworkStream != null).ToArray();
			return paths.OrderByDescending(p => MeasuredBps(p, dir))
				.ThenBy(p => Volatile.Read(ref p.LatencyEwmaMs)).FirstOrDefault();
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
