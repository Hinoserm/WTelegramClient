using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.ComponentModel;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
using System.Reflection;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;
using System.Web;
using TL;
using static WTelegram.Encryption;

// necessary for .NET Standard 2.0 compilation:
#pragma warning disable CA1835 // Prefer the 'Memory'-based overloads for 'ReadAsync' and 'WriteAsync'

namespace WTelegram
{
	/// <summary>Controls how send traffic is distributed across multipath transport connections.</summary>
	public enum PathSendMode
	{
		/// <summary>Cycle through alive paths on each send, distributing load evenly.</summary>
		RoundRobin,
		/// <summary>Prefer paths in configured order. Always use the highest-priority alive path; fail over to lower-priority paths and return to higher-priority ones as they recover.</summary>
		PreferredOrder,
		/// <summary>Use current path until it dies. Fail over to next alive path and stay there, even if the original recovers.</summary>
		StickyFailover,
		/// <summary>Always route sends through the alive path with the lowest measured round-trip latency.
		/// Latency is tracked per path using an EWMA of PingDelayDisconnect response times.
		/// Paths with no samples yet are used as fallback until measurements arrive.</summary>
		LowestLatency,
	}

	/// <summary>Snapshot of per-path transport statistics.</summary>
	public sealed class PathStats
	{
		/// <summary>Telegram DC ID this path is connected to.</summary>
		public int DcId { get; init; }
		/// <summary>True if this is a media-only DC (used for file transfers).</summary>
		public bool IsMediaDc { get; init; }
		/// <summary>Index of this path (matches LocalEndPoints order).</summary>
		public int PathIndex { get; init; }
		/// <summary>Local endpoint this path is bound to (null for legacy single-path).</summary>
		public IPEndPoint LocalEndPoint { get; init; }
		/// <summary>True if this path currently has an active connection.</summary>
		public bool IsAlive { get; init; }
		/// <summary>True if a reconnect is currently in progress for this path.</summary>
		public bool IsReconnecting { get; init; }
		/// <summary>Exponentially-weighted moving average round-trip time in milliseconds. -1 if no samples yet.</summary>
		public long LatencyEwmaMs { get; init; }
		/// <summary>Total bytes sent on this path since last (re)connect.</summary>
		public long BytesSent { get; init; }
		/// <summary>Total bytes received on this path since last (re)connect.</summary>
		public long BytesRecv { get; init; }
		/// <summary>Number of times this path has reconnected.</summary>
		public int ReconnectCount { get; init; }
		/// <summary>Reliability penalty added to latency score (ms). Higher = path is less preferred. 0 = no penalty.</summary>
		public long PenaltyMs { get; init; }
		/// <summary>File upload speed measured on this path's WAN (bytes/s, EWMA of parts), -1 if not measured yet.</summary>
		public double UploadBytesPerSecMeasured { get; init; } = -1;
		/// <summary>File download speed measured on this path's WAN (bytes/s, EWMA of parts), -1 if not measured yet.</summary>
		public double DownloadBytesPerSecMeasured { get; init; } = -1;
		/// <summary>When this path's current connection was established (UTC). MinValue if never connected.</summary>
		public DateTime ConnectedSince { get; init; }
		/// <summary>How long this path has been connected. Zero if not alive or never connected.</summary>
		public TimeSpan Uptime => IsAlive && ConnectedSince != DateTime.MinValue ? DateTime.UtcNow - ConnectedSince : TimeSpan.Zero;
		/// <summary>Average send throughput in bytes per second since last (re)connect. 0 if uptime is zero.</summary>
		public double SendBytesPerSec { get { var s = Uptime.TotalSeconds; return s > 0 ? BytesSent / s : 0; } }
		/// <summary>Average receive throughput in bytes per second since last (re)connect. 0 if uptime is zero.</summary>
		public double RecvBytesPerSec { get { var s = Uptime.TotalSeconds; return s > 0 ? BytesRecv / s : 0; } }
		/// <summary>Average total throughput (send + receive) in bytes per second since last (re)connect.</summary>
		public double TotalBytesPerSec { get { var s = Uptime.TotalSeconds; return s > 0 ? (BytesSent + BytesRecv) / s : 0; } }
	}

	public partial class Client : IDisposable
#if NETCOREAPP2_1_OR_GREATER
		, IAsyncDisposable
#endif
	{
		/// <summary>This event will be called when unsollicited updates/messages are sent by Telegram servers</summary>
		/// <remarks>Make your handler <see langword="async"/>, or return <see cref="Task.CompletedTask"/> or <see langword="null"/><br/>See <see href="https://github.com/wiz0u/WTelegramClient/blob/master/Examples/Program_ReactorError.cs?ts=4#L30">Examples/Program_ReactorError.cs</see> for how to use this<br/>or <see href="https://github.com/wiz0u/WTelegramClient/blob/master/Examples/Program_ListenUpdates.cs?ts=4#L21">Examples/Program_ListenUpdate.cs</see> using the UpdateManager class instead</remarks>
		public event Func<UpdatesBase, Task> OnUpdates;
		/// <summary>This event is called for other types of notifications (login states, reactor errors, ...)</summary>
		public event Func<IObject, Task> OnOther;
		/// <summary>Use this handler to intercept Updates that resulted from your own API calls</summary>
		public event Func<UpdatesBase, Task> OnOwnUpdates;
		/// <summary>Fired when any transport path changes state (alive, dead, reconnecting).
		/// On the main client, this also receives changes from alt DC sessions (media transfers).
		/// The handler receives a <see cref="PathStats"/> snapshot of the changed path.</summary>
		public event Action<PathStats> OnPathChanged;
		/// <summary>Used to create a TcpClient connected to the given address/port, or throw an exception on failure</summary>
		public TcpFactory TcpHandler { get; set; } = DefaultTcpHandler;
		public delegate Task<TcpClient> TcpFactory(string host, int port, IPEndPoint localEndPoint = null);
		/// <summary>Url for using a MTProxy. https://t.me/proxy?server=... </summary>
		public string MTProxyUrl { get; set; }
		/// <summary>Telegram configuration, obtained at connection time</summary>
		public Config TLConfig { get; private set; }
		/// <summary>Number of automatic reconnections on connection/reactor failure</summary>
		public int MaxAutoReconnects { get; set; } = 5;
		/// <summary>Number of attempts in case of wrong verification_code or password</summary>
		public int MaxCodePwdAttempts { get; set; } = 3;
		/// <summary>Number of seconds under which an error 420 FLOOD_WAIT_X will not be raised and your request will instead be auto-retried after the delay</summary>
		public int FloodRetryThreshold { get; set; } = 60;
		/// <summary>Number of seconds between each keep-alive ping. Increase this if you have a slow connection or you're debugging your code</summary>
		public int PingInterval { get; set; } = 60;
		/// <summary>Size of chunks when uploading/downloading files. Reduce this if you don't have much memory</summary>
		public int FilePartSize { get; set; } = 512 * 1024;
		/// <summary>Is this Client instance the main or a secondary DC session</summary>
		public bool IsMainDC => _dcSession?.DataCenter?.flags.HasFlag(DcOption.Flags.media_only) != true
			&& (_dcSession?.DataCenter?.id - _session.MainDC) is null or 0;
		/// <summary>Has this Client established connection been disconnected?</summary>
		public bool Disconnected => _paths.Count > 0
			? !_paths.Any(p => p.IsAlive)
			: (_tcpClient != null && !(_tcpClient.Client?.Connected ?? false));
		/// <summary>Returns a snapshot of per-path transport statistics for THIS client's DC. Empty array if not using multipath.</summary>
		public PathStats[] GetPathStatistics()
		{
			TransportPath[] snapshot;
			lock (_pathsLock)
				snapshot = _paths.ToArray();
			if (snapshot.Length == 0)
				return Array.Empty<PathStats>();
			var dcId = _dcSession?.DcID ?? 0;
			var isMedia = _dcSession?.DataCenter?.flags.HasFlag(DcOption.Flags.media_only) ?? false;
			var result = new PathStats[snapshot.Length];
			for (int i = 0; i < snapshot.Length; i++)
			{
				var p = snapshot[i];
				var connTicks = Volatile.Read(ref p.ConnectedSinceTicks);
				result[i] = new PathStats
				{
					DcId = dcId,
					IsMediaDc = isMedia,
					PathIndex = p.PathIndex,
					LocalEndPoint = p.LocalEndPoint,
					IsAlive = p.IsAlive,
					IsReconnecting = Volatile.Read(ref p._reconnecting) != 0,
					LatencyEwmaMs = p.LatencyEwmaMs == long.MaxValue ? -1 : Volatile.Read(ref p.LatencyEwmaMs),
					BytesSent = Interlocked.Read(ref p.BytesSent),
					BytesRecv = Interlocked.Read(ref p.BytesRecv),
					ReconnectCount = Volatile.Read(ref p.ReconnectCount),
					PenaltyMs = Volatile.Read(ref p.PenaltyMs),
					UploadBytesPerSecMeasured = MeasuredBps(p, TransferUp),
					DownloadBytesPerSecMeasured = MeasuredBps(p, TransferDown),
					ConnectedSince = connTicks > 0
						? DateTime.UtcNow - TimeSpan.FromMilliseconds(Environment.TickCount64 - connTicks)
						: DateTime.MinValue,
				};
			}
			return result;
		}

		/// <summary>Returns path statistics across ALL active DC sessions (main + media DCs).
		/// Call on the main client to see every connected DC including media transfer sessions.</summary>
		public PathStats[] GetAllPathStatistics()
		{
			// Snapshot the session list under lock, then iterate outside to avoid
			// nested lock ordering (_session → _pathsLock) which could deadlock
			// if future code ever acquires them in the opposite order.
			Session.DCSession[] sessions;
			lock (_session)
				sessions = _session.DCSessions.Values.ToArray();

			var all = new List<PathStats>();
			foreach (var dcSession in sessions)
			{
				var client = dcSession.Client;
				if (client == null || client.Disconnected)
					continue;
				all.AddRange(client.GetPathStatistics());
			}
			return all.ToArray();
		}

		/// <summary>Fires <see cref="OnPathChanged"/> for the given path, propagating to the parent client if this is an alt DC session.</summary>
		private void RaisePathChanged(TransportPath path)
		{
			var connTicks = Volatile.Read(ref path.ConnectedSinceTicks);
			var stats = new PathStats
			{
				DcId = _dcSession?.DcID ?? 0,
				IsMediaDc = _dcSession?.DataCenter?.flags.HasFlag(DcOption.Flags.media_only) ?? false,
				PathIndex = path.PathIndex,
				LocalEndPoint = path.LocalEndPoint,
				IsAlive = path.IsAlive,
				IsReconnecting = Volatile.Read(ref path._reconnecting) != 0,
				LatencyEwmaMs = path.LatencyEwmaMs == long.MaxValue ? -1 : Volatile.Read(ref path.LatencyEwmaMs),
				BytesSent = Interlocked.Read(ref path.BytesSent),
				BytesRecv = Interlocked.Read(ref path.BytesRecv),
				ReconnectCount = Volatile.Read(ref path.ReconnectCount),
				PenaltyMs = Volatile.Read(ref path.PenaltyMs),
				ConnectedSince = connTicks > 0
					? DateTime.UtcNow - TimeSpan.FromMilliseconds(Environment.TickCount64 - connTicks)
					: DateTime.MinValue,
			};
			// Alt DC clients propagate to the parent (main) client only.
			// Main clients fire locally. This avoids double-delivery if someone
			// subscribes to both a child and parent client's OnPathChanged.
			if (_parentClient != null)
			{
				try
				{
					_parentClient.OnPathChanged?.Invoke(stats);
				}
				catch { }
			}
			else
			{
				try
				{
					OnPathChanged?.Invoke(stats);
				}
				catch { }
			}
		}

		/// <summary>ID of the current logged-in user or 0</summary>
		public long UserId => _session.UserId;
		/// <summary>Info about the current logged-in user. This is only filled after a successful (re)login, not updated later</summary>
		public User User { get; private set; }
		/// <summary>Number of parallel transfers operations (uploads/downloads) allowed at the same time.</summary>
		/// <remarks>Don't use this property while transfers are ongoing!</remarks>
		public int ParallelTransfers
		{
			get => _parallelTransfers.CurrentCount;
			set
			{
				int delta = value - _parallelTransfers.CurrentCount;
				for (; delta < 0; delta++)
					_parallelTransfers.Wait();
				if (delta > 0)
					_parallelTransfers.Release(delta);
			}
		}

		private Func<string, string> _config;
		private readonly Session _session;
		private readonly Client _parentClient; // non-null for alt DC clients; used to propagate OnPathChanged to main client
		private string _apiHash;
		private Session.DCSession _dcSession;
		private TcpClient _tcpClient;
		private Stream _networkStream;
		private HttpClient _httpClient;
		private HttpWait _httpWait;
		private IObject _lastSentMsg;
		private long _lastRecvMsgId;
		private readonly List<long> _msgsToAck = [];
		private readonly Random _random = new();
		private int _saltChangeCounter;
		private Task _reactorTask;
		private Rpc _bareRpc;
		private readonly Dictionary<long, Rpc> _pendingRpcs = [];
		private SemaphoreSlim _sendSemaphore = new(0);
		private readonly SemaphoreSlim _semaphore = new(1);
		private Task _connecting;
		private CancellationTokenSource _cts;
		private int _reactorReconnects = 0;
		private const string ConnectionShutDown = "Could not read payload length : Connection shut down";
		private const long Ticks5Secs = 5 * TimeSpan.TicksPerSecond;
		private readonly SemaphoreSlim _parallelTransfers = new(2); // max parallel part uploads/downloads
		private readonly SHA256 _sha256 = SHA256.Create();
		private readonly SHA256 _sha256Recv = SHA256.Create();
#if OBFUSCATION
		private AesCtr _sendCtr, _recvCtr;
#endif
		private bool _paddedMode;
		private readonly List<TransportPath> _paths = new();
		private readonly object _pathsLock = new();
		private volatile int _primaryPathIndex;
		private volatile int _lastConnectedEPIndex;
		private bool _fullReconnectStarted;
		private Task _secondaryPathsTask;
		private readonly ConcurrentDictionary<long, (int PathIndex, long SentTicks)> _pendingPings = new();
		/// <summary>Optional local endpoints for multipath transport. Add two endpoints for redundant connections.</summary>
		public List<IPEndPoint> LocalEndPoints { get; set; } = new();
		/// <summary>Seconds between per-path liveness probes (PingDelayDisconnect). Lower values detect failures faster but increase traffic.</summary>
		public int PathProbeInterval { get; set; } = 3;
		/// <summary>Seconds of silence (after a probe) before a path is declared dead locally and force-closed.</summary>
		public int PathDeadTimeout { get; set; } = 5;
		/// <summary>Seconds the Telegram server waits before disconnecting a silent path. Server-side safety net; should be larger than PathDeadTimeout.</summary>
		public int PathDisconnectDelay { get; set; } = 15;
		/// <summary>Seconds to wait for a single endpoint connection attempt before timing out.</summary>
		public int PathConnectTimeout { get; set; } = 10;
		/// <summary>Maximum backoff delay in seconds between reconnect attempts (both per-path and full reconnect).</summary>
		public int PathReconnectMaxBackoff { get; set; } = 30;
		/// <summary>Seconds before an RPC on a specific path is considered stalled. The path is force-closed to trigger failover.
		/// Set to 0 to disable stall detection. Only applies when multiple paths exist.</summary>
		public int PathRpcStallTimeout { get; set; } = 3;
		/// <summary>Controls how send traffic is distributed across alive paths. Default: LowestLatency (use path with lowest measured RTT).</summary>
		public PathSendMode SendMode { get; set; } = PathSendMode.LowestLatency;
		/// <summary>Main client with several paths: send every request on the best path AND a copy on the next
		/// best one (same msg_id, in a container), so a slow or dead path never makes the caller wait.
		/// The server executes a msg_id once. File parts are only copied once they stall.</summary>
		public bool HedgeAllRequests { get; set; } = true;
		/// <summary>Seconds of silence after which an idle path of a media-DC (child) client gets a keepalive ping,
		/// so Telegram does not drop the connection the transfers are not currently using.</summary>
		public int ChildPathKeepAlive { get; set; } = 20;

		// msg_ids of requests already answered, handed back to Invoke, or forgotten (ForgetPending: the caller got an
		// exception): a second answer, to a copy or re-delivered by the server on another connection under a new
		// server msg_id, must not be raised again (OnOwnUpdates), and a BadMsgNotification 32/33 about one is no
		// reason for a session reset (nobody waits on it; the next live request resyncs). Every request, not only
		// copied ones. Pruned after 5 minutes.
		private readonly ConcurrentDictionary<long, long> _settledHedged = new();
		// container msg_id of each copy → msg_id of the request inside it, so a BadMsgNotification
		// about a copy is not mistaken for one about the original. Pruned with _settledHedged.
		private readonly ConcurrentDictionary<long, (long RpcMsgId, long Ticks)> _copyContainers = new();
		// ping_id → waiter, for registration pings that must be answered before a path is marked alive.
		private readonly ConcurrentDictionary<long, TaskCompletionSource<bool>> _pingWaiters = new();
		// msg_id of the frame (or container entry) being handled on this reactor, for Pong.msg_id.
		private readonly AsyncLocal<long> _frameMsgId = new();
		private const long CopyMaxAgeMs = 240_000; // a copy reuses the msg_id: keep it inside the server's 300 s window
		private const int CopyAnswerTimeoutMs = 10_000; // a copy sent because its path died gets this long before we ask the server about it
		// the server refuses client msg_ids older than 300 s: past this, a new msg_id can no longer race the old
		// one. It also bounds the state-check loop: a request still unanswered then is retried with a new msg_id
		// if the server never had it, or fails with a TimeoutException (never retried) if it did.
		private const long ServerMsgIdWindowMs = 330_000;
		// msg_id of each msgs_state_req we sent → the request it asks about
		private readonly ConcurrentDictionary<long, Rpc> _stateChecks = new();
		private const int MaxCopies = 8;
		private long _pathGeneration; // source of TransportPath.Generation
		// copies and state checks never wait longer than this for the send semaphore (a reconnect holds it)
		private static readonly TimeSpan SendLockWait = TimeSpan.FromSeconds(5);
		private long _lastReactorReconnectTicks = -60_000; // single-path/all-dead reconnect: only repeats back off
		// counters for the once-a-minute copy summary (LogCopyStats)
		private long _statHedged, _statStalled, _statRescued, _statDupAnswers, _statDupFrames, _lastCopyStatsTicks;

		public Client(int apiID, string apiHash, string sessionPathname = null, IEnumerable<string> localAddresses = null)
			: this(what => what switch
			{
				"api_id" => apiID.ToString(),
				"api_hash" => apiHash,
				"session_pathname" => sessionPathname,
				_ => null
			}, localAddresses: localAddresses)
		{ }

		public Client(Func<string, string> configProvider, byte[] startSession, Action<byte[]> saveSession, IEnumerable<string> localAddresses = null)
			: this(configProvider, new ActionStore(startSession, saveSession), localAddresses: localAddresses) { }

		/// <summary>Welcome to WTelegramClient! 🙂</summary>
		/// <param name="configProvider">Config callback, is queried for: <b>api_id</b>, <b>api_hash</b>, <b>session_pathname</b></param>
		/// <param name="sessionStore">if specified, must support initial Length &amp; Read() of a session, then calls to Write() the updated session. Other calls can be ignored</param>
		/// <param name="localAddresses">Optional list of local IP addresses to bind outgoing connections to. If two or more are provided, multipath transport is used for redundancy.</param>
		public Client(Func<string, string> configProvider = null, Stream sessionStore = null, IEnumerable<string> localAddresses = null)
		{
			_config = configProvider ?? DefaultConfigOrAsk;
			var session_key = _config("session_key") ?? (_apiHash = Config("api_hash"));
			sessionStore ??= new SessionStore(Config("session_pathname"));
			_session = Session.LoadOrCreate(sessionStore, Convert.FromHexString(session_key));
			if (_session.ApiId == 0)
				_session.ApiId = int.Parse(Config("api_id"));
			if (_session.MainDC != 0)
				_session.DCSessions.TryGetValue(_session.MainDC, out _dcSession);
			_dcSession ??= new();
			_dcSession.Client = this;
			if (localAddresses != null)
				foreach (var addr in localAddresses)
					if (IPAddress.TryParse(addr, out var ip))
						LocalEndPoints.Add(new IPEndPoint(ip, 0));
			var version = Assembly.GetExecutingAssembly().GetCustomAttribute<AssemblyInformationalVersionAttribute>().InformationalVersion;
			Helpers.Log(1, $"WTelegramClient {version} running under {System.Runtime.InteropServices.RuntimeInformation.FrameworkDescription}");
		}

		private Client(Client cloneOf, Session.DCSession dcSession)
		{
			_parentClient = cloneOf;
			_config = cloneOf._config;
			_session = cloneOf._session;
			TcpHandler = cloneOf.TcpHandler;
			MTProxyUrl = cloneOf.MTProxyUrl;
			PingInterval = cloneOf.PingInterval;
			MaxAutoReconnects = cloneOf.MaxAutoReconnects;
			TLConfig = cloneOf.TLConfig;
			// Copy (not share) LocalEndPoints so child clients have all interfaces
			// available for dynamic per-transfer path selection.
			LocalEndPoints = new List<IPEndPoint>(cloneOf.LocalEndPoints);
			PathProbeInterval = cloneOf.PathProbeInterval;
			PathDeadTimeout = cloneOf.PathDeadTimeout;
			PathDisconnectDelay = cloneOf.PathDisconnectDelay;
			PathConnectTimeout = cloneOf.PathConnectTimeout;
			PathReconnectMaxBackoff = cloneOf.PathReconnectMaxBackoff;
			PathRpcStallTimeout = cloneOf.PathRpcStallTimeout;
			SendMode = cloneOf.SendMode;
			TransferMode = cloneOf.TransferMode;
			TransferPartsPerPath = cloneOf.TransferPartsPerPath;
			TransferProbeInterval = cloneOf.TransferProbeInterval;
			TransferMaxFloodWait = cloneOf.TransferMaxFloodWait;
			_dcSession = dcSession;
		}

		internal Task<string> ConfigAsync(string what) => Task.Run(() => Config(what));
		internal string Config(string what)
			=> _config(what) ?? DefaultConfig(what) ?? throw new WTException("You must provide a config value for " + what);

		/// <summary>Default config values, used if your Config callback returns <see langword="null"/></summary>
		public static string DefaultConfig(string what) => what switch
		{
			"session_pathname" => Path.Combine(
				Path.GetDirectoryName(Path.GetDirectoryName(AppDomain.CurrentDomain.BaseDirectory.TrimEnd(Path.DirectorySeparatorChar)))
				?? AppDomain.CurrentDomain.BaseDirectory, "WTelegram.session"),
#if DEBUG
			"server_address" => "2>149.154.167.40:443",	// Test DC 2
#else
			"server_address" => "2>149.154.167.50:443",	// DC 2
#endif
			"device_model" => Environment.Is64BitOperatingSystem ? "PC 64bit" : "PC 32bit",
			"system_version" => Helpers.GetSystemVersion(),
			"app_version" => Helpers.GetAppVersion(),
			"system_lang_code" => CultureInfo.InstalledUICulture.TwoLetterISOLanguageName,
			"lang_pack" => "",
			"lang_code" => CultureInfo.CurrentUICulture.TwoLetterISOLanguageName,
			"user_id" => "-1",
			"verification_code" or "email_verification_code" or "password" => AskConfig(what),
			"init_params" => "{}",
			_ => null // api_id api_hash phone_number... it's up to you to reply to these correctly
		};

		internal static string DefaultConfigOrAsk(string config) => DefaultConfig(config) ?? AskConfig(config);

		private static string AskConfig(string config)
		{
			if (config == "session_key")
			{
				Console.WriteLine("Welcome! You can obtain your api_id/api_hash at https://my.telegram.org/apps");
				return null;
			}
			Console.Write($"Enter {config.Replace('_', ' ')}: ");
			return Console.ReadLine();
		}

		/// <summary>Builds a structure that is used to validate a 2FA password</summary>
		/// <param name="accountPassword">Password validation configuration. You can obtain this via <c>Account_GetPassword</c> or through OnOther as part of the login process</param>
		/// <param name="password">The password to validate</param>
		public static Task<InputCheckPasswordSRP> InputCheckPassword(Account_Password accountPassword, string password)
			=> Check2FA(accountPassword, () => Task.FromResult(password));

		[System.Diagnostics.CodeAnalysis.SuppressMessage("Usage", "CA1816")]
		public void Dispose() => DisposeAsync().AsTask().Wait();
		public async ValueTask DisposeAsync()
		{
			Helpers.Log(2, $"{_dcSession.DcID}>Disposing the client");
			await ResetAsync(false, IsMainDC).ConfigureAwait(false);
			var ex = new ObjectDisposedException("WTelegram.Client was disposed");
			Rpc[] aborted;
			lock (_pendingRpcs) // abort all pending requests
			{
				aborted = [.. _pendingRpcs.Values];
				foreach (var rpc in aborted)
				{
					rpc.tcs.TrySetException(ex);
					_settledHedged[rpc.msgId] = Environment.TickCount64; // a late answer: dropped as a second one
				}
				_pendingRpcs.Clear();
			}
			foreach (var rpc in aborted)
				SettledTransfer(rpc);
			_sendSemaphore.Dispose();
			_httpClient?.Dispose();
			lock (_pathsLock)
			{
				foreach (var path in _paths) path.Dispose();
				_paths.Clear();
			}
			_networkStream = null;
			if (IsMainDC)
				_session.Dispose();
			GC.SuppressFinalize(this);
		}

		public void DisableUpdates(bool disable = true) => _dcSession.DisableUpdates(disable);

		/// <summary>Enable connecting to Telegram via on-demand HTTP requests instead of permanent TCP connection</summary>
		/// <param name="httpClient">HttpClient to use. Leave <see langword="null"/> for a default one</param>
		/// <param name="defaultHttpWait">Default HttpWait parameters for requests.<para>⚠️ Telegram servers don't support this correctly at the moment.</para>So leave <see langword="null"/> for the default 25 seconds long poll</param>
		public void HttpMode(HttpClient httpClient = null, HttpWait defaultHttpWait = null)
		{
			if (_tcpClient != null)
				throw new InvalidOperationException("Cannot switch to HTTP after TCP connection");
			_httpClient = httpClient ?? new();
			_httpWait = defaultHttpWait;
			ParallelTransfers = 1;
			TransferMode = PathTransferMode.FollowSendMode; // one part at a time, as HTTP needs
		}

		/// <summary>Disconnect from Telegram <i>(shouldn't be needed in normal usage)</i></summary>
		/// <param name="resetUser">Forget about logged-in user</param>
		/// <param name="resetSessions">Disconnect secondary sessions with other DCs</param>
		public void Reset(bool resetUser = true, bool resetSessions = true) => ResetAsync(resetUser, resetSessions).Wait();

		/// <summary>Disconnect from Telegram <i>(shouldn't be needed in normal usage)</i></summary>
		/// <param name="resetUser">Forget about logged-in user</param>
		/// <param name="resetSessions">Disconnect secondary sessions with other DCs</param>
		public async Task ResetAsync(bool resetUser = true, bool resetSessions = true)
		{
			try
			{
				if (_httpClient == null && CheckMsgsToAck() is MsgsAck msgsAck)
					await SendAsync(msgsAck, false).WaitAsync(1000).ConfigureAwait(false);
			}
			catch { }
			_cts?.Cancel();
			_sendSemaphore = new(0);    // initially taken, first released during DoConnectAsync
			// Shut down all transport paths
			lock (_pathsLock)
			{
				foreach (var path in _paths)
				{
					path.Cts?.Cancel();
					path.IsAlive = false;
				}
			}
			// Wait for all path reactors to finish
			TransportPath[] pathsCopy;
			lock (_pathsLock)
				pathsCopy = [.. _paths];
			foreach (var path in pathsCopy)
			{
				try
				{
					if (path.ReactorTask != null)
						await path.ReactorTask.WaitAsync(1000).ConfigureAwait(false);
				}
				catch { }
			}
			// Also wait for legacy reactor if present
			try
			{
				if (_reactorTask != null)
					await _reactorTask.WaitAsync(1000).ConfigureAwait(false);
			}
			catch { }
			_reactorTask = resetSessions ? null : Task.CompletedTask;
			// Dispose all paths
			lock (_pathsLock)
			{
				foreach (var path in _paths) path.Dispose();
				_paths.Clear();
				_primaryPathIndex = 0;
				_fullReconnectStarted = false;
			}
			_pendingPings.Clear();
			_networkStream?.Close();
			_tcpClient?.Dispose();
#if OBFUSCATION
			_sendCtr?.Dispose();
			_recvCtr?.Dispose();
#endif
			_paddedMode = false;
			_connecting = null;
			_bareRpc = null;
			if (resetSessions)
			{
				foreach (var altSession in _session.DCSessions.Values)
					if (altSession.Client != null && altSession.Client != this)
					{
						await altSession.Client.DisposeAsync();
						altSession.Client = null;
					}
			}
			if (resetUser)
			{
				_loginCfg = default;
				_session.UserId = 0;
				User = null;
			}
		}

		private Session.DCSession GetOrCreateDCSession(int dcId, DcOption.Flags flags)
		{
			if (_session.DCSessions.TryGetValue(dcId, out var dcSession) && dcSession.Client != null)
				return dcSession; // we have already a connected session with this DC, use it
			if (dcSession == null && _session.DCSessions.TryGetValue(-dcId, out dcSession) && dcSession.AuthKey != null)
			{
				// we have already negociated an AuthKey with this DC
				if (dcSession.DataCenter.flags == flags && _session.DCSessions.Remove(-dcId))
					return _session.DCSessions[dcId] = dcSession; // we found a misclassed DC, change its sign
				dcSession = new Session.DCSession { // clone AuthKey for a session on the matching media_only DC
					authKeyID = dcSession.authKeyID, AuthKey = dcSession.AuthKey, UserId = dcSession.UserId };
			}
			// try to find the most appropriate DcOption for this DC
			if (dcSession?.AuthKey == null) // we'll need to negociate an AuthKey => can't use media_only DC
			{
				flags &= ~DcOption.Flags.media_only;
				dcId = Math.Abs(dcId);
			}
			var dcOptions = GetDcOptions(Math.Abs(dcId), flags);
			var dcOption = dcOptions.FirstOrDefault();
			dcSession ??= new(); // create new session only if not already existing
			if (dcOption != null)
				dcSession.DataCenter = dcOption;
			else if (dcSession.DataCenter == null)
				throw new WTException($"Could not find adequate dc_option for DC {dcId}");
			return _session.DCSessions[dcId] = dcSession;
		}

		/// <summary>Obtain/create a Client for a secondary session on a specific Data Center</summary>
		/// <param name="dcId">ID of the Data Center (use negative values for media_only)</param>
		/// <param name="connect">Connect immediately</param>
		/// <returns>Client connected to the selected DC</returns>
		/// <remarks>⚠️ You shouldn't have to use this method unless you know what you're doing</remarks>
		public async Task<Client> GetClientForDC(int dcId, bool connect = true)
		{
			if (_dcSession.DataCenter?.id == dcId)
				return this;
			Session.DCSession altSession;
			bool created;
			lock (_session)
			{
				var flags = _dcSession.DataCenter.flags;
				if (dcId < 0)
					flags = (flags & DcOption.Flags.ipv6) | DcOption.Flags.media_only;
				bool known = _session.DCSessions.ContainsKey(dcId);
				altSession = GetOrCreateDCSession(dcId, flags);
				if (altSession.Client?.Disconnected ?? false) { altSession.Client.Dispose(); altSession.Client = null; }
				created = altSession.Client is null;
				altSession.Client ??= new Client(this, altSession);
				// Saved when something changed (a new DC session or client), not on every file transfer.
				if (!known || created)
					_session.Save();
			}
			// Every media transfer asks for its DC's client: worth a line only when a connection is made.
			Helpers.Log(created ? 2 : 1, $"Requested connection to DC {dcId}{(created ? " (new connection)" : "")}...");
			if (connect)
			{
				await _semaphore.WaitAsync();
				try
				{
					Auth_ExportedAuthorization exported = null;
					if (_session.UserId != 0 && IsMainDC && altSession.UserId != _session.UserId && Math.Abs(altSession.DcID) != Math.Abs(_dcSession.DcID))
						exported = await this.Auth_ExportAuthorization(Math.Abs(dcId));
					await altSession.Client.ConnectAsync();
					if (exported != null)
					{
						var authorization = await altSession.Client.Auth_ImportAuthorization(exported.id, exported.bytes);
						if (authorization is not Auth_Authorization { user: User user })
							throw new WTException("Failed to get Authorization: " + authorization.GetType().Name);
						altSession.UserId = user.id;
						lock (_session) _session.Save();
					}
				}
				finally
				{
					_semaphore.Release();
				}
			}
			return altSession.Client;
		}

		private Task Reactor(Stream stream, CancellationToken ct) => Reactor(null, stream, ct);

		private async Task Reactor(TransportPath path, Stream stream, CancellationToken ct)
		{
			const int MinBufferSize = 1024;
			var data = new byte[MinBufferSize];
			var sha256Recv = path?.Sha256Recv ?? _sha256Recv;
			var paddedMode = path?.PaddedMode ?? _paddedMode;
#if OBFUSCATION
			var recvCtr = path?.RecvCtr ?? _recvCtr;
#endif
			while (!ct.IsCancellationRequested)
			{
				IObject obj = null;
				try
				{
					if (await stream.FullReadAsync(data, 4, ct) != 4)
						throw new WTException(ConnectionShutDown);
#if OBFUSCATION
					recvCtr.EncryptDecrypt(data.AsSpan(0, 4));
#endif
					int payloadLen = BinaryPrimitives.ReadInt32LittleEndian(data);
					if (payloadLen <= 0)
						throw new WTException("Could not read frame data : Invalid payload length");
					else if (payloadLen > data.Length)
						data = new byte[payloadLen];
					else if (Math.Max(payloadLen, MinBufferSize) < data.Length / 4)
						data = new byte[Math.Max(payloadLen, MinBufferSize)];
					if (await stream.FullReadAsync(data, payloadLen, ct) != payloadLen)
						throw new WTException("Could not read frame data : Connection shut down");
#if OBFUSCATION
					recvCtr.EncryptDecrypt(data.AsSpan(0, payloadLen));
#endif
					obj = ReadFrame(data, payloadLen, sha256Recv, paddedMode, path?.PathIndex ?? -1);
					if (path != null)
					{
						path.LastRecvTicks = Environment.TickCount64;
						Interlocked.Add(ref path.BytesRecv, 4 + payloadLen);
					}
				}
				catch (Exception ex) // an exception in RecvAsync is always fatal
				{
					if (ct.IsCancellationRequested)
						return;
					if (path?.Cts?.IsCancellationRequested == true)
						return;

					// Multi-path error handling
					if (path != null && _paths.Count > 1)
					{
						bool otherPathsAlive, wasAlive;
						bool shouldFullReconnect = false;
						lock (_pathsLock)
						{
							wasAlive = path.IsAlive;
							path.IsAlive = false;
							otherPathsAlive = _paths.Any(p => p != path && p.IsAlive);
							if (!otherPathsAlive && !_fullReconnectStarted)
							{
								_fullReconnectStarted = true;
								shouldFullReconnect = true;
							}
						}
						if (wasAlive) // whoever force-closed it already reported it down
							RaisePathChanged(path);

						if (otherPathsAlive)
						{
							Helpers.Log(wasAlive ? 3 : 2, $"{_dcSession.DcID}>Path {path.PathIndex} error ({ex.Message}), other paths alive. Reconnecting path in background.");
							_ = ReconnectPathAsync(path, ex); // also re-sends what only this path carried
							return; // exit this reactor, other paths continue
						}
						else if (!shouldFullReconnect)
						{
							Helpers.Log(3, $"{_dcSession.DcID}>Path {path.PathIndex} error, another path is handling full reconnect.");
							return; // another reactor is handling the full reconnect
						}

						// ALL paths dead — fall through to full reconnect below
						Helpers.Log(4, $"{_dcSession.DcID}>All paths dead. Starting full reconnect.");
					}

					// Single-path or all-paths-dead reconnect (existing logic)
					bool disconnectedAltDC = !IsMainDC && ex is WTException { Message: ConnectionShutDown } or IOException { InnerException: SocketException };
					if (disconnectedAltDC)
						Helpers.Log(3, $"{_dcSession.DcID}>Alt DC disconnected: {ex.Message}");
					else if (path == null || _paths.Count <= 1)
						Helpers.Log(5, $"{_dcSession.DcID}>An exception occured in the reactor: {ex}");
					var oldSemaphore = _sendSemaphore;
					await oldSemaphore.WaitAsync(ct); // prevent any sending while we reconnect
					var reactorError = new ReactorError { Exception = ex };
					try
					{
						lock (_msgsToAck) _msgsToAck.Clear();
						await ResetAsync(false, false);
						_reactorReconnects = (_reactorReconnects + 1) % MaxAutoReconnects;
						if (disconnectedAltDC && _pendingRpcs.Count <= 1)
							if (_pendingRpcs.Values.FirstOrDefault() is not Rpc rpc || rpc.type == typeof(Pong))
								_reactorReconnects = 0;
						if (_reactorReconnects == 0)
							throw;
						// first reconnect in a minute is immediate (pending requests wait on it); repeats back off 5 s
						var sinceLast = Environment.TickCount64 - _lastReactorReconnectTicks;
						_lastReactorReconnectTicks = Environment.TickCount64;
#pragma warning disable CA2016
						if (sinceLast < 60_000)
							await Task.Delay(5000);
						try
						{
							await ConnectAsync(); // start a new reactor
						}
						catch (Exception) when (sinceLast >= 60_000)
						{
							await Task.Delay(5000); // the network may still be coming back: the old 5 s grace, once
							await ConnectAsync();
						}
#pragma warning restore CA2016
						// same session: re-send pending requests with their own msg_id (executed once);
						// the rest get a ReactorError, which Invoke<T> retries with a new msg_id
						await ResendPendingAfterReconnectAsync(reactorError);
						if (IsMainDC)
						{
							var updatesState = await this.Updates_GetState(); // this call reenables incoming Updates
							RaiseUpdates(updatesState);
						}
					}
					catch (Exception e) when (e is not ObjectDisposedException)
					{
						if (IsMainDC)
							RaiseUpdates(reactorError);
						Rpc[] aborted;
						lock (_pendingRpcs) // abort all pending requests
						{
							aborted = [.. _pendingRpcs.Values];
							foreach (var rpc in aborted)
								rpc.tcs.TrySetException(ex);
							_pendingRpcs.Clear();
							_bareRpc = null;
						}
						foreach (var rpc in aborted)
							SettledTransfer(rpc);
					}
					finally
					{
						_fullReconnectStarted = false;
						oldSemaphore.Release();
					}
				}
				if (obj != null)
				{
					await HandleMessageAsync(obj); // Pongs (top-level or in a container) reach OnPongReceived
				}
			}
		}

		private TransportPath AlivePathByIndex(int pathIndex)
		{
			lock (_pathsLock)
				return _paths.FirstOrDefault(p => p.PathIndex == pathIndex && p.IsAlive && p.NetworkStream != null);
		}

		private TransportPath GetPrimaryAlivePath()
		{
			// Pre-snapshot parent latency AND penalty by local address BEFORE acquiring our
			// own lock to avoid lock-ordering inversion (child._pathsLock → parent._pathsLock).
			Dictionary<System.Net.IPAddress, (long Latency, long Penalty)> parentLatencyByAddr = null;
			if (_parentClient != null && SendMode == PathSendMode.LowestLatency)
			{
				parentLatencyByAddr = new();
				lock (_parentClient._pathsLock)
				{
					foreach (var p in _parentClient._paths)
						if (p.IsAlive && p.LocalEndPoint != null)
							parentLatencyByAddr[p.LocalEndPoint.Address] = (Volatile.Read(ref p.LatencyEwmaMs), Volatile.Read(ref p.PenaltyMs));
				}
			}

			lock (_pathsLock)
			{
				if (_paths.Count == 0)
					return null;
				int count = _paths.Count;

				switch (SendMode)
				{
					case PathSendMode.PreferredOrder:
						// Prefer paths in configured address order: LocalEndPoints[0] > [1] > [2] ...
						// After a full reconnect, _paths order may differ from LocalEndPoints order,
						// so we search by endpoint address rather than list index.
						var orderedEPs = LocalEndPoints;
						if (orderedEPs != null && orderedEPs.Count > 0)
						{
							for (int ep = 0; ep < orderedEPs.Count; ep++)
							{
								var targetAddr = orderedEPs[ep].Address;
								for (int i = 0; i < count; i++)
								{
									if (_paths[i].LocalEndPoint?.Address?.Equals(targetAddr) == true && _paths[i].IsAlive)
									{
										_primaryPathIndex = i;
										return _paths[i];
									}
								}
							}
						}
						else if (_paths[0].IsAlive)
						{
							_primaryPathIndex = 0;
							return _paths[0];
						}
						// No configured endpoint alive — fallback to any alive path
						for (int i = 0; i < count; i++)
						{
							if (_paths[i].IsAlive)
							{
								_primaryPathIndex = i;
								return _paths[i];
							}
						}
						return null;

					case PathSendMode.StickyFailover:
						// Stay on current path if alive
						if (_primaryPathIndex < count && _paths[_primaryPathIndex].IsAlive)
							return _paths[_primaryPathIndex];
						// Current path dead — find next alive one
						for (int i = 1; i < count; i++)
						{
							int idx = (_primaryPathIndex + i) % count;
							if (_paths[idx].IsAlive)
							{
								_primaryPathIndex = idx;
								return _paths[idx];
							}
						}
						return null;

					case PathSendMode.LowestLatency:
					// Pick the alive path with the lowest EFFECTIVE score = latency + penalty.
					// Latency alone doesn't capture reliability — a fast but unstable path
					// (e.g. 51ms but reconnecting every 20s) would always win over a slower
					// but rock-solid path (72ms, 0 reconnects). The penalty adds virtual
					// latency based on reconnect history and stall events.
					// Paths with no samples yet (LatencyEwmaMs == long.MaxValue) are used as
					// fallback if no measured paths are alive yet (e.g. right after startup).
					// Child clients (media DCs) consult the parent's measurements + penalty.
					TransportPath bestPath = null;
					long bestScore = long.MaxValue;
					for (int i = 0; i < count; i++)
					{
						if (!_paths[i].IsAlive)
						continue;
						long lat = Volatile.Read(ref _paths[i].LatencyEwmaMs);
						long penalty = Volatile.Read(ref _paths[i].PenaltyMs);
						if (lat == long.MaxValue && parentLatencyByAddr != null
							&& _paths[i].LocalEndPoint != null
							&& parentLatencyByAddr.TryGetValue(_paths[i].LocalEndPoint.Address, out var parentData))
						{
							lat = parentData.Latency;
							// Use the worse of local vs parent penalty
							if (penalty < parentData.Penalty)
								penalty = parentData.Penalty;
						}
						long score = (lat == long.MaxValue) ? long.MaxValue : lat + penalty;
						if (bestPath == null || score < bestScore)
						{
							bestPath = _paths[i];
							bestScore = score;
							_primaryPathIndex = i;
						}
					}
					return bestPath;

				case PathSendMode.RoundRobin:
					default:
						// Advance to next alive path each call (original behavior)
						int start = (_primaryPathIndex + 1) % count;
						for (int i = 0; i < count; i++)
						{
							int idx = (start + i) % count;
							if (_paths[idx].IsAlive)
							{
								_primaryPathIndex = idx;
								return _paths[idx];
							}
						}
						return null;
				}
			}
		}

		/// <summary>Adds penalty (in virtual milliseconds) to a path's reliability score.
		/// Uses additive increase — each event (reconnect, stall, error) adds to the penalty.
		/// Penalty is capped at 10000ms and decayed by the health monitor over time.</summary>
		private static void AddPenalty(TransportPath path, long penaltyMs)
		{
			long current, newVal;
			do
			{
				current = Volatile.Read(ref path.PenaltyMs);
				newVal = Math.Min(current + penaltyMs, 10000);
			} while (Interlocked.CompareExchange(ref path.PenaltyMs, newVal, current) != current);
		}

		/// <summary>Best alive path other than <paramref name="avoidIndex"/>: lowest latency + penalty, unmeasured paths last.</summary>
		private TransportPath PickOtherAlivePath(int avoidIndex)
		{
			lock (_pathsLock)
			{
				TransportPath best = null;
				long bestScore = 0;
				foreach (var p in _paths)
				{
					if (!p.IsAlive || p.PathIndex == avoidIndex || p.NetworkStream == null)
						continue;
					long lat = Volatile.Read(ref p.LatencyEwmaMs);
					long score = lat == long.MaxValue ? long.MaxValue : lat + Volatile.Read(ref p.PenaltyMs);
					if (best == null || score < bestScore)
					{
						best = p;
						bestScore = score;
					}
				}
				return best;
			}
		}

		/// <summary>Sends a copy of a pending request on an alive path other than <paramref name="avoidIndex"/>:
		/// the original message with its own msg_id and seqno, wrapped in a new container (MTProto does not
		/// allow re-sending it bare under its old msg_id). The server executes a msg_id once, so a copy can
		/// only bring the answer sooner, never repeat the action.</summary>
		/// <param name="sole">The copy is now the only live one (its original's path died): if no answer comes
		/// within <see cref="CopyAnswerTimeoutMs"/>, fail it so Invoke retries with a new msg_id.</param>
		/// <returns>true if the copy is out and carries the request; false when nothing was sent (already answered, too old,
		/// copy budget spent, no other path, already hedged with <paramref name="onlyIfUnhedged"/>), or when it was sent but
		/// refused before it went live</returns>
		/// <param name="afterWritten">Run once the copy is written (not when queued)</param>
		/// <param name="onlyIfUnhedged">Not if another copy already carries it (judged again under the send semaphore, so two
		/// such callers never both send one)</param>
		private async Task<bool> SendCopyAsync(Rpc rpc, int avoidIndex, string reason, bool sole, Action afterWritten = null, bool onlyIfUnhedged = false)
		{
			if (rpc.query == null || rpc.msgId == 0 || rpc == _bareRpc)
				return false;
			if (Environment.TickCount64 - rpc.sentTicks > CopyMaxAgeMs || Volatile.Read(ref rpc.copies) >= MaxCopies)
				return false;
			var path = rpc.transferDir >= 0 ? PickTransferCopyPath(avoidIndex, rpc.transferDir) : PickOtherAlivePath(avoidIndex);
			if (path == null)
				return false;
			PruneCopyState();
			// before the write: the first answer can arrive before the copy is out, and the second one
			// must then be dropped (see ReadRpcResult). Never reset: a stray mark only costs a dict entry.
			rpc.copyAttempted = true;
			long containerId = 0, generation = -1, registeredId = 0;
			bool live = false;
			Task written = Task.CompletedTask;
			var sem = _sendSemaphore;
			try
			{
				if (!await sem.WaitAsync(SendLockWait, _cts.Token))
					return false; // reconnecting (semaphore held or swapped): the caller's loop tries again
				try
				{
					lock (_pendingRpcs)
						if (!_pendingRpcs.TryGetValue(rpc.msgId, out var current) || current != rpc)
							return false; // answered meanwhile
					if (Environment.TickCount64 - rpc.sentTicks > CopyMaxAgeMs)
						return false; // aged while waiting for the semaphore (the server would refuse it: BadMsg 16)
					lock (rpc) // judged again here: callers decided outside the semaphore
						if (rpc.copies >= MaxCopies || (onlyIfUnhedged && CarriedByCopy(rpc)))
							return false;
					// Registered right before the frame is queued (QueueOnPath calls this after everything that can fail): a
					// refusal (BadMsgNotification) about it can arrive before the write returns. Pending (a carrier for
					// HasLiveCarrier, which can look meanwhile from another thread), and counted against the budget, until
					// marked live just below. With the generation of the connection it really goes out on.
					containerId = QueueOnPath(path, new MsgContainer { messages = [new(rpc.msgId, rpc.seqno, rpc.query)] },
						out written, beforeWrite: (id, connection) =>
						{
							registeredId = id;
							generation = connection;
							_copyContainers[id] = (rpc.msgId, Environment.TickCount64);
							lock (rpc)
							{
								rpc.pendingCopies[id] = (path, generation);
								rpc.copies++;
							}
						});
					// Queued: it carries the request from now on (in order on its path), unless refused meanwhile. Marked
					// still under the semaphore (a state change that cannot fail): the next caller sees it as a carrier.
					if (containerId != 0)
						live = MarkCopyLive(rpc, containerId);
				}
				finally
				{
					sem.Release();
				}
			}
			catch (Exception ex)
			{
				Helpers.Log(2, $"{_dcSession?.DcID}>{reason}: copy of #{(short)rpc.msgId.GetHashCode():X4} on P{path.PathIndex} failed: {ex.Message}");
				if (registeredId != 0 && containerId == 0) // registered, then failed before its frame was queued: never sent
				{
					_copyContainers.TryRemove(registeredId, out _);
					lock (rpc)
						if (rpc.pendingCopies.Remove(registeredId))
						{
							rpc.refusedPending.Remove(registeredId);
							rpc.copies--;
						}
				}
				if (containerId == 0)
					return false;
				// queued, then something after failed: the frame is out, so its write is still followed below, with its
				// real state
				lock (rpc)
					live = rpc.liveCopies.ContainsKey(containerId);
			}
			if (containerId == 0)
				return false;
			if (live) // the file part's copy load: after the semaphore (three locks, not to be held up by every send)
				SyncTransferCopySafe(rpc);
			// not awaited: the callers include the path monitors, which serve every path and must not wait on a slow one
			_ = CopyWrittenAsync(written, rpc, path, generation, containerId, reason, sole, avoidIndex, afterWritten, live);
			return live;
		}

		/// <summary>A queued copy becomes live (a carrier of its request), unless the server refused it while it was being
		/// queued. All copy state changes under lock(rpc), here and in <see cref="CopyGone"/>, so no refusal is ever lost
		/// between the two. A state change only (nothing that can fail); the caller syncs the file part's copy load
		/// (<see cref="SyncTransferCopy"/>) afterwards, outside the send semaphore.</summary>
		/// <returns>false if it was refused before it went live</returns>
		private static bool MarkCopyLive(Rpc rpc, long containerId)
		{
			lock (rpc)
			{
				if (!rpc.pendingCopies.Remove(containerId, out var queuedOn))
					return false; // gone already (refused and accounted for)
				if (rpc.refusedPending.Remove(containerId))
				{
					rpc.copies--; // refused before it went live: it never carried anything
					return false;
				}
				rpc.liveCopies[containerId] = (queuedOn.Path, queuedOn.Gen, ++rpc.copySeq);
				rpc.hedged = true;
				rpc.copySyncVersion++;
				return true;
			}
		}

		/// <summary>A copy carries the request, or is being queued to (and not refused). Caller holds lock(rpc).</summary>
		private static bool CarriedByCopy(Rpc rpc) => rpc.hedged || rpc.pendingCopies.Count > rpc.refusedPending.Count;

		/// <summary>Live copies that went out on a connection now gone (its path down, reconnected since, or replaced by a full
		/// reset) carry nothing: they stop counting as carriers and give their budget slots back. Run before each look at
		/// whether a request is carried. (A pending copy exists only inside SendCopyAsync's semaphore section.)</summary>
		private void DropDeadCopies(Rpc rpc)
		{
			if (!rpc.hedged)
				return; // no live copy (hedged mirrors liveCopies): nothing to look at, no snapshot, no lock
			bool changed = false;
			lock (rpc)
			{
				if (rpc.liveCopies.Count == 0)
					return;
				lock (_pathsLock) // nested inside lock(rpc): no _pathsLock section ever touches a request's copies
					foreach (var (id, copy) in rpc.liveCopies.ToArray())
						if (!copy.Path.IsAlive || copy.Path.Generation != copy.Gen || !_paths.Contains(copy.Path))
						{
							rpc.liveCopies.Remove(id);
							rpc.copies--;
							changed = true;
						}
				if (changed)
				{
					rpc.hedged = rpc.liveCopies.Count > 0;
					rpc.copySyncVersion++;
				}
			}
			if (changed)
				SyncTransferCopySafe(rpc);
		}

		/// <summary>A copy carries nothing (any more): its write failed, its queuing failed, or the server refused it. A live
		/// one stops counting as carrier and gives its budget slot back; a pending one (refused before it went live) is
		/// noted, for <see cref="MarkCopyLive"/>. Other copies, older or newer, keep their state.</summary>
		/// <returns>true if it was a live carrier</returns>
		private bool CopyGone(Rpc rpc, long containerId)
		{
			bool wasLive;
			lock (rpc)
			{
				wasLive = rpc.liveCopies.Remove(containerId);
				if (wasLive)
				{
					rpc.copies--;
					rpc.hedged = rpc.liveCopies.Count > 0;
					rpc.copySyncVersion++;
				}
				else if (rpc.pendingCopies.ContainsKey(containerId))
					rpc.refusedPending.Add(containerId);
			}
			if (wasLive)
				SyncTransferCopySafe(rpc);
			return wasLive;
		}

		/// <summary>The copies carrying a request (path, connection): the live ones and the ones being queued (not refused),
		/// read together.</summary>
		private static (TransportPath Path, long Gen)[] CarrierCopies(Rpc rpc)
		{
			lock (rpc)
				return [.. rpc.liveCopies.Values.Select(c => (c.Path, c.Gen)),
					.. rpc.pendingCopies.Where(p => !rpc.refusedPending.Contains(p.Key)).Select(p => p.Value)];
		}

		/// <summary>Once a copy is written: counted, logged, and (a copy that is now the only carrier) watched for its answer
		/// from then on, not from its queuing. If its write failed it carries nothing: the request is open to another copy
		/// (its budget slot back), and the connection (its CTR state past a partial frame) is closed for its reactor to
		/// reconnect, unless a newer connection was swapped in meanwhile.</summary>
		/// <param name="live">false: refused before it went live (only its write is followed, for the connection's sake)</param>
		private async Task CopyWrittenAsync(Task written, Rpc rpc, TransportPath path, long generation, long containerId, string reason,
			bool sole, int avoidIndex, Action afterWritten, bool live)
		{
			try
			{
				await written;
			}
			catch (Exception ex)
			{
				Helpers.Log(2, $"{_dcSession?.DcID}>{reason}: copy of #{(short)rpc.msgId.GetHashCode():X4} on P{path.PathIndex} failed: {ex.Message}");
				CopyGone(rpc, containerId); // this copy only: others, older or newer, keep their state
				lock (_pathsLock)
					if (path.Generation == generation)
						path.NetworkStream?.Close();
				return;
			}
			if (!live)
				return; // refused before it went live: carries nothing, nothing to count or watch
			if (sole) // first: nothing below may keep the only carrier from being watched
				_ = CopyAnswerWatchdog(rpc, Interlocked.Increment(ref rpc.soleCopies));
			try
			{
				afterWritten?.Invoke();
			}
			catch (Exception ex)
			{
				Helpers.Log(3, $"{_dcSession?.DcID}>{reason}: after copying #{(short)rpc.msgId.GetHashCode():X4}: {ex.Message}");
			}
			if (sole)
				Interlocked.Increment(ref _statRescued);
			else if (reason == "Hedge")
				Interlocked.Increment(ref _statHedged);
			else
				Interlocked.Increment(ref _statStalled);
			Helpers.Log(sole ? 2 : 1, $"{_dcSession?.DcID}>{reason}: copied #{(short)rpc.msgId.GetHashCode():X4} {rpc.query?.GetType().Name.TrimEnd('_')} to P{path.PathIndex}{(avoidIndex >= 0 ? $" (was P{avoidIndex})" : "")}");
		}

		/// <summary>A copy that replaced a lost original gets <see cref="CopyAnswerTimeoutMs"/> to be answered,
		/// then we ask the server what it knows about the msg_id (never a blind new-msg_id retry).</summary>
		private async Task CopyAnswerWatchdog(Rpc rpc, int soleCopy)
		{
			await Task.Delay(CopyAnswerTimeoutMs);
			if (Volatile.Read(ref rpc.soleCopies) != soleCopy)
				return; // a later sole copy has its own watchdog (non-sole copies never cancel this one)
			await CheckStateOrRetryAsync(rpc, $"no answer {CopyAnswerTimeoutMs / 1000}s after re-sending");
		}

		/// <summary>Asks the server (msgs_state_req) whether it has a still-pending request's msg_id; the answer
		/// is handled in <see cref="OnMsgStateAsync"/>. Only when we cannot even ask (no path up) and the msg_id is
		/// too old to copy does the request go back to Invoke for a new-msg_id retry.</summary>
		private async Task CheckStateOrRetryAsync(Rpc rpc, string why)
		{
			lock (_pendingRpcs)
				if (!_pendingRpcs.TryGetValue(rpc.msgId, out var current) || current != rpc)
					return; // answered meanwhile
			if (Interlocked.CompareExchange(ref rpc.stateCheckInFlight, 1, 0) != 0)
				return; // one question at a time (the sweep runs every second)
			if (Environment.TickCount64 - rpc.sentTicks > ServerMsgIdWindowMs)
			{
				// The old msg_id can no longer run. A new one is safe only for a request that never reached the
				// wire (never written, never copied). Anything that may have reached the server (confirmed, or
				// just written: its answer may be what got lost) gets an error, never a second execution.
				if (rpc.serverHasIt || Volatile.Read(ref rpc.writtenTicks) != 0 || rpc.copyAttempted)
					FailPending(rpc, null, $"{why}; received by Telegram but never answered", new TimeoutException($"Telegram did not answer {rpc.query?.GetType().Name.TrimEnd('_')} (msg_id {rpc.msgId}); not retried, it may have been executed"));
				else
					FailForRetry(rpc, new IOException($"{why}; Telegram never confirmed receiving it"));
				return;
			}
			var path = PickOtherAlivePath(-1);
			long reqId = 0, generation = -1;
			if (path != null)
			{
				var sem = _sendSemaphore;
				try
				{
					Task written = Task.CompletedTask;
					if (await sem.WaitAsync(SendLockWait, _cts.Token)) // reconnecting otherwise: reqId stays 0, asked again in 10 s
						try
						{
							Interlocked.Increment(ref rpc.stateChecks);
							// registered before the write: the answer can arrive before the write completes; with the
							// generation of the connection it really goes out on (for the close below, on failure)
							reqId = QueueOnPath(path, new MsgsStateReq { msg_ids = [rpc.msgId] }, out written, beforeWrite: (id, connection) =>
							{
								generation = connection;
								_stateChecks[id] = rpc;
							});
						}
						finally
						{
							sem.Release();
						}
					await written;
				}
				catch (Exception ex)
				{
					Helpers.Log(1, $"{_dcSession?.DcID}>State check of #{(short)rpc.msgId.GetHashCode():X4} failed to send: {ex.Message}");
					if (ex is IOException) // CTR state past a partial frame: let its reactor fail and reconnect it
						lock (_pathsLock)
							if (path.Generation == generation)
								path.NetworkStream?.Close();
					reqId = 0;
				}
			}
			if (reqId == 0)
			{
				foreach (var kvp in _stateChecks) // a write that failed after registering
					if (kvp.Value == rpc)
						_stateChecks.TryRemove(kvp.Key, out _);
				_ = RecheckLaterAsync(rpc, why); // keeps driving it (the age bound above ends the loop)
				return;
			}
			Helpers.Log(2, $"{_dcSession?.DcID}>{why}: asking Telegram about #{(short)rpc.msgId.GetHashCode():X4} {rpc.query?.GetType().Name.TrimEnd('_')}");
			_ = StateCheckTimeoutAsync(reqId, rpc, why);
		}

		/// <summary>A state request that gets no answer is asked again.</summary>
		private async Task StateCheckTimeoutAsync(long reqId, Rpc rpc, string why)
		{
			await Task.Delay(CopyAnswerTimeoutMs);
			if (_stateChecks.TryRemove(reqId, out _))
			{
				Volatile.Write(ref rpc.stateCheckInFlight, 0);
				await CheckStateOrRetryAsync(rpc, why);
			}
		}

		/// <summary>The server's status byte for a pending request's msg_id (msgs_state_info): low bits 1 = unknown
		/// (too old), 2/3 = not received, 4 = received; +32 = being processed or done, +64 = answer already
		/// generated. Every non-final outcome sends a copy (a duplicate msg_id never runs twice; for an answered
		/// request it makes the server point us at the lost answer via msg_detailed_info) and asks again in 10 s.
		/// A new msg_id only once the old one is past the server's acceptance window (checked when asking again):
		/// "not received" is a snapshot, and the original may still sit in a stalled connection's buffer.</summary>
		private async Task OnMsgStateAsync(Rpc rpc, int state)
		{
			// stateCheckInFlight is still 1 here; RecheckLaterAsync holds it through its delay so the sweep does not re-ask
			string query = rpc.query?.GetType().Name.TrimEnd('_');
			try
			{
				switch (state & 7)
				{
					case 2 or 3:
						Helpers.Log(2, $"{_dcSession.DcID}>Telegram has not got #{(short)rpc.msgId.GetHashCode():X4} {query}; re-sending it");
						await SendCopyAsync(rpc, -1, "not received", sole: false);
						break;
					case 4:
						rpc.serverHasIt = true;
						Helpers.Log(2, (state & 64) != 0
							? $"{_dcSession.DcID}>Telegram answered #{(short)rpc.msgId.GetHashCode():X4} {query} but the answer was lost; fetching it"
							: $"{_dcSession.DcID}>Telegram is still processing #{(short)rpc.msgId.GetHashCode():X4} {query}; waiting");
						if ((state & 64) != 0 || Volatile.Read(ref rpc.stateChecks) % 3 == 0) // processing: a copy now and then, not every time
							await SendCopyAsync(rpc, -1, (state & 64) != 0 ? "answer lost" : "still processing", sole: false);
						break;
					default: // 1 (forgotten/too old) or an undefined value: ask again; the age bound decides
						Helpers.Log(2, $"{_dcSession.DcID}>State {state} for #{(short)rpc.msgId.GetHashCode():X4} {query}; asking again");
						break;
				}
			}
			catch (Exception ex)
			{
				Helpers.Log(4, $"{_dcSession?.DcID}>OnMsgStateAsync: {ex}");
			}
			_ = RecheckLaterAsync(rpc, "re-check");
		}

		/// <summary>Asks again after a delay; holds the in-flight mark meanwhile.</summary>
		private async Task RecheckLaterAsync(Rpc rpc, string why)
		{
			await Task.Delay(CopyAnswerTimeoutMs);
			Volatile.Write(ref rpc.stateCheckInFlight, 0);
			await CheckStateOrRetryAsync(rpc, why);
		}

		/// <summary>True while a connection that carries this request (its original or a copy) is still up.
		/// Compares connection generations, not path indices: a reconnected path is a new connection.</summary>
		private bool HasLiveCarrier(Rpc rpc)
		{
			DropDeadCopies(rpc);
			var copies = CarrierCopies(rpc); // under lock(rpc), released before _pathsLock below (see the LOCK ORDER on Rpc)
			lock (_pathsLock)
			{
				if (IsLive(rpc.sentPathIndex, rpc.sentGen))
					return true;
				foreach (var (path, generation) in copies)
					if (path.IsAlive && path.Generation == generation && _paths.Contains(path))
						return true;
				return false;
			}
			bool IsLive(int index, long generation) => index >= 0 && index < _paths.Count && _paths[index].IsAlive && _paths[index].Generation == generation;
		}

		/// <summary>Hands a still-pending request back to Invoke, which retries it with a new msg_id.
		/// A late answer to the old msg_id is then dropped instead of being raised as an unknown result.</summary>
		private bool FailForRetry(Rpc rpc, Exception ex) => FailPending(rpc, new ReactorError { Exception = ex }, ex.Message);

		/// <summary>Completes a still-pending request with <paramref name="result"/> (a ReactorError or -503
		/// RpcError, both of which Invoke retries with a new msg_id).</summary>
		/// <param name="exception">Instead of a retryable result: fail the caller with this (no retry at all)</param>
		private bool FailPending(Rpc rpc, object result, string why, Exception exception = null)
		{
			lock (_pendingRpcs) // marked settled under the same lock as the removal (see ReadRpcResult)
			{
				if (!_pendingRpcs.TryGetValue(rpc.msgId, out var current) || current != rpc)
					return false;
				_pendingRpcs.Remove(rpc.msgId);
				_settledHedged[rpc.msgId] = Environment.TickCount64;
			}
			if (exception != null)
			{
				Helpers.Log(4, $"{_dcSession?.DcID}>Giving up on #{(short)rpc.msgId.GetHashCode():X4} {rpc.query?.GetType().Name.TrimEnd('_')}: {why}");
				rpc.tcs.TrySetException(exception);
			}
			else
			{
				Helpers.Log(3, $"{_dcSession?.DcID}>Retrying #{(short)rpc.msgId.GetHashCode():X4} {rpc.query?.GetType().Name.TrimEnd('_')} with a new msg_id: {why}");
				rpc.tcs.TrySetResult(result);
			}
			SettledTransfer(rpc); // after the caller has its result (never throws)
			return true;
		}

		/// <summary>The server refused a copy (BadMsgNotification about its container). That copy carries
		/// nothing any more: if the original's connection is still up, the stall check may copy again;
		/// otherwise a fresh copy goes out now, so a request whose only live copy was refused never waits
		/// for the 30 s watchdog. Code 20 (too old to verify) means a new msg_id, per the protocol.</summary>
		/// <param name="containerId">The refused copy's container: only that copy stops counting as a carrier</param>
		private void HandleRefusedCopy(long rpcMsgId, long containerId, int errorCode)
		{
			Rpc rpc;
			lock (_pendingRpcs)
				_pendingRpcs.TryGetValue(rpcMsgId, out rpc);
			if (rpc == null)
				return;
			// Only the copy was refused; the original may well have run. Never a new msg_id from here:
			// copy again if nothing else carries it, else (or if that fails) ask the server about it.
			CopyGone(rpc, containerId); // this copy only (one still being queued is noted, and never goes live)
			if (errorCode == 20 || HasLiveCarrier(rpc))
			{
				if (errorCode == 20)
					_ = CheckStateOrRetryAsync(rpc, $"copy refused ({errorCode})");
				return;
			}
			_ = CopyOrCheckAsync(rpc, $"copy refused ({errorCode})");
		}

		private async Task CopyOrCheckAsync(Rpc rpc, string why)
		{
			if (!await SendCopyAsync(rpc, -1, why, sole: true))
				await CheckStateOrRetryAsync(rpc, why);
		}

		/// <summary>If nothing answers a request within <paramref name="delayMs"/>, asks the server about it
		/// (a new msg_id only if the server does not have the old one).</summary>
		private async Task CheckIfUnansweredAsync(Rpc rpc, int delayMs, string why)
		{
			await Task.Delay(delayMs);
			await CheckStateOrRetryAsync(rpc, why);
		}

		/// <summary>The server refused the message for its salt only: re-send it under the same msg_id
		/// (a copy that got through already makes this a no-op server-side). If that is impossible: a new
		/// msg_id only when no copy ever went out (the refused original never ran); otherwise ask the server.</summary>
		private async Task ResendRefusedAsync(Rpc rpc, RpcError fallback)
		{
			if (await SendCopyAsync(rpc, -1, fallback.error_message, sole: true))
				return;
			if (rpc.copyAttempted)
				await CheckStateOrRetryAsync(rpc, fallback.error_message);
			else
				FailPending(rpc, fallback, fallback.error_message);
		}

		/// <summary>Every written request whose connections (original and copy) are all gone gets a copy on a
		/// live path. One that cannot be copied stays pending while it is young and has copies left (the path
		/// monitors call this every second, and a full reconnect re-sends everything); otherwise it goes back
		/// to Invoke for a new-msg_id retry.</summary>
		private async Task RescueStrandedRpcsAsync(string reason, Exception ex)
		{
			try
			{
				Rpc[] pending;
				lock (_pendingRpcs)
					pending = [.. _pendingRpcs.Values];
				List<Rpc> stranded = null;
				foreach (var rpc in pending)
					if (rpc.sentPathIndex >= 0 && rpc.query != null && !HasLiveCarrier(rpc)) // -1: not written yet, its sender picks a live path
						(stranded ??= []).Add(rpc);
				if (stranded == null)
					return;
				int copied = 0;
				foreach (var rpc in stranded)
					if (await SendCopyAsync(rpc, -1, reason, sole: true))
						copied++;
					else if (Environment.TickCount64 - rpc.sentTicks > CopyMaxAgeMs || Volatile.Read(ref rpc.copies) >= MaxCopies)
						_ = CheckStateOrRetryAsync(rpc, $"{reason}, cannot copy"); // never a blind new msg_id; single-flight, logs itself
				if (copied > 0)
					Helpers.Log(2, $"{_dcSession.DcID}>{reason}: {copied} of {stranded.Count} stranded RPC(s) re-sent with their own msg_id.");
			}
			catch (Exception e)
			{
				Helpers.Log(4, $"{_dcSession?.DcID}>RescueStrandedRpcsAsync: {e}");
			}
		}

		/// <summary>After a reconnect within the same session: re-send each pending request with its own
		/// msg_id; those that cannot be (too old, bare, nothing to send on) get <paramref name="reactorError"/>.</summary>
		private async Task ResendPendingAfterReconnectAsync(ReactorError reactorError)
		{
			List<Rpc> pending;
			lock (_pendingRpcs)
			{
				if (_bareRpc != null)
				{
					_pendingRpcs.Remove(_bareRpc.msgId);
					_bareRpc.tcs.TrySetResult(reactorError);
					_bareRpc = null;
				}
				pending = [.. _pendingRpcs.Values];
			}
			int resent = 0, retried = 0;
			foreach (var rpc in pending)
				if (rpc.sentPathIndex < 0 && _paths.Count > 0)
					continue; // not written yet: its sender sends it on the new connection
				else if (await SendCopyAsync(rpc, -1, "reconnected", sole: true))
					resent++;
				else if (_paths.Count == 0) // legacy single connection (MTProxy/HTTP): no copies possible
				{
					if (FailForRetry(rpc, reactorError.Exception))
						retried++;
				}
				else
				{
					retried++;
					_ = CheckStateOrRetryAsync(rpc, "reconnected, cannot copy"); // never a blind new msg_id
				}
			if (resent + retried > 0)
				Helpers.Log(2, $"{_dcSession?.DcID}>After reconnect: {resent} RPC(s) re-sent with their own msg_id, {retried} retried or being asked about.");
		}

		/// <summary>Requests unanswered after <see cref="PathRpcStallTimeout"/> get one copy on another path
		/// (and, on the main client, their path gets a probe now). A stall never kills a path: a slow
		/// answer is not a dead path. Probes and transport errors decide that.</summary>
		private async Task CopyStalledRpcsAsync(bool probe)
		{
			long stallMs = (long)PathRpcStallTimeout * 1000;
			if (stallMs <= 0)
				return;
			var now = Environment.TickCount64;
			List<Rpc> stalled = null, transfers = null;
			Rpc[] hedgedNow;
			lock (_pendingRpcs)
				hedgedNow = [.. _pendingRpcs.Values.Where(r => r.hedged)];
			foreach (var rpc in hedgedNow) // a copy on a connection gone since carries nothing: not hedged any more
				DropDeadCopies(rpc);
			lock (_pendingRpcs)
				foreach (var rpc in _pendingRpcs.Values)
					if (!rpc.hedged && rpc.query != null && rpc.sentPathIndex >= 0)
						if (rpc.writtenTicks > 0 && now - rpc.writtenTicks > stallMs + Volatile.Read(ref rpc.stallExtraMs))
							(stalled ??= []).Add(rpc); // from the write, not the registration: a part queued behind others is not stalled
						else if (rpc.transferDir >= 0 && Volatile.Read(ref rpc.queuedTicks) > 0)
							(transfers ??= []).Add(rpc); // judged from its queuing: it may be stuck behind its path's slow writes
			if (transfers != null) // judged outside the lock: it takes the path and transfer locks
				foreach (var rpc in transfers)
					if (TransferCopyDue(rpc, now))
						(stalled ??= []).Add(rpc);
			if (stalled == null)
				return;
			foreach (var rpc in stalled)
			{
				bool timedOut = rpc.writtenTicks > 0 && now - rpc.writtenTicks > stallMs + Volatile.Read(ref rpc.stallExtraMs);
				// what a copy says about the original's path counts once the copy is written, not when queued
				await SendCopyAsync(rpc, rpc.sentPathIndex, timedOut ? $"Stalled >{PathRpcStallTimeout}s" : "Slow WAN", sole: false,
					afterWritten: () => AfterStallCopy(rpc, timedOut, probe), onlyIfUnhedged: true);
			}
		}

		private void AfterStallCopy(Rpc rpc, bool timedOut, bool probe)
		{
			RecordSlowTransfer(rpc); // a file part: its WAN's speed so far is a sample (no answer will be one)
			if (!timedOut)
				return; // a slow WAN for file parts: its latency (what the penalty steers) is fine
			lock (_pathsLock)
				if (_paths.FirstOrDefault(p => p.PathIndex == rpc.sentPathIndex) is TransportPath path)
				{
					AddPenalty(path, 1000); // steer new traffic away while it is slow; decays
					if (probe)
						path.LastProbeTicks = 0; // probe it on the next monitor pass
				}
		}

		/// <summary>Once a minute, if anything happened: how many requests were copied and why, and how many
		/// second answers were dropped. Called from the path monitors.</summary>
		private void LogCopyStats()
		{
			LogTransferStats(); // its own minute, on the main client's counters
			var now = Environment.TickCount64;
			if (now - _lastCopyStatsTicks < 60_000)
				return;
			_lastCopyStatsTicks = now;
			long hedged = Interlocked.Exchange(ref _statHedged, 0), stalled = Interlocked.Exchange(ref _statStalled, 0);
			long rescued = Interlocked.Exchange(ref _statRescued, 0), dupAnswers = Interlocked.Exchange(ref _statDupAnswers, 0);
			long dupFrames = Interlocked.Exchange(ref _statDupFrames, 0);
			if (hedged + stalled + rescued + dupAnswers + dupFrames > 0)
				Helpers.Log(2, $"{_dcSession?.DcID}>Copies in the last minute: {hedged} hedged, {stalled} after a stall, {rescued} re-sent after a path loss; duplicates dropped: {dupAnswers} answers, {dupFrames} frames");
		}

		private long _lastCopyPruneTicks;
		private void PruneCopyState()
		{
			var now = Environment.TickCount64;
			if (now - Volatile.Read(ref _lastCopyPruneTicks) < 10_000)
				return;
			Volatile.Write(ref _lastCopyPruneTicks, now);
			foreach (var kvp in _settledHedged)
				if (now - kvp.Value > 300_000)
					_settledHedged.TryRemove(kvp.Key, out _);
			foreach (var kvp in _copyContainers)
				if (now - kvp.Value.Ticks > 300_000)
					_copyContainers.TryRemove(kvp.Key, out _);
		}

		/// <summary>Per-path RTT and liveness from a Pong. Telegram routes answers to ANY connection of the
		/// session, so the Pong may arrive on another path: _pendingPings names the path that SENT the ping.</summary>
		private void OnPongReceived(Pong pong)
		{
			if (_pingWaiters.TryRemove(pong.ping_id, out var waiter))
				waiter.TrySetResult(true);
			if (!_pendingPings.TryRemove(pong.ping_id, out var probe))
				return;
			long rttMs = Environment.TickCount64 - probe.SentTicks;
			if (rttMs < 0 || rttMs >= 30_000) // sanity-check: discard stale/wrapped measurements
				return;
			TransportPath senderPath;
			lock (_pathsLock)
				senderPath = _paths.FirstOrDefault(p => p.PathIndex == probe.PathIndex);
			if (senderPath == null)
				return;
			long prev = Volatile.Read(ref senderPath.LatencyEwmaMs);
			long next = prev == long.MaxValue ? rttMs : (long)(0.8 * prev + 0.2 * rttMs);
			Volatile.Write(ref senderPath.LatencyEwmaMs, next);
			// the sender path is reachable even though the Pong may have come in on another connection
			Volatile.Write(ref senderPath.LastRecvTicks, Environment.TickCount64);
			Helpers.Log(1, $"{_dcSession.DcID}>Path {probe.PathIndex} RTT {rttMs}ms (EWMA {next}ms)");
		}

		/// <summary>Registration ping on a fresh connection; true only once Telegram has answered it,
		/// so a path is never marked alive before the server knows it.</summary>
		private async Task<bool> RegisterPathAsync(TransportPath path)
		{
			long pingId = Random.Shared.NextInt64();
			var waiter = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
			_pingWaiters[pingId] = waiter;
			try
			{
				long sentTicks = Environment.TickCount64;
				_pendingPings[pingId] = (path.PathIndex, sentTicks); // first RTT sample for this path
				var sem = _sendSemaphore;
				Task written;
				await sem.WaitAsync(_cts.Token);
				try
				{
					if (QueueOnPath(path, new TL.Methods.Ping { ping_id = pingId }, out written, registering: true) == 0)
						return false;
				}
				finally
				{
					sem.Release();
				}
				await written;
				var timeout = Task.Delay(Math.Max(PathDeadTimeout, 1) * 1000); // not path.Cts: a reconnect may null/dispose it
				return await Task.WhenAny(waiter.Task, timeout) == waiter.Task;
			}
			finally
			{
				_pingWaiters.TryRemove(pingId, out _);
				_pendingPings.TryRemove(pingId, out _);
			}
		}

		/// <summary>Watcher for child (media DC) clients: copies stalled requests to another path and keeps
		/// idle paths open with a ping (Telegram drops a connection that carries nothing for ~80 s).
		/// No liveness probing: Telegram delays Pongs while it processes file parts.</summary>
		private async Task ChildStallWatcher(CancellationToken ct)
		{
			while (!ct.IsCancellationRequested)
			{
				await Task.Delay(1000, ct);
				if (_paths.Count <= 1)
					continue;
				await CopyStalledRpcsAsync(probe: false);
				await RescueStrandedRpcsAsync("No live connection", new IOException("No live connection carries the request"));
				LogCopyStats();
				TransportPath[] snapshot;
				lock (_pathsLock)
					snapshot = [.. _paths];
				DecayPenalties(snapshot); // stalls add penalty here too

				long idleMs = (long)ChildPathKeepAlive * 1000;
				if (idleMs <= 0)
					continue;
				var now = Environment.TickCount64;
				TransportPath[] idle;
				lock (_pathsLock)
					idle = _paths.Where(p => p.IsAlive && now - Math.Max(Math.Max(Volatile.Read(ref p.LastSendTicks),
						Volatile.Read(ref p.LastQueuedTicks)), p.ConnectedSinceTicks) > idleMs).ToArray();
				foreach (var path in idle)
				{
					var sem = _sendSemaphore;
					try
					{
						Task written;
						await sem.WaitAsync(ct);
						try
						{
							if (QueueOnPath(path, new TL.Methods.Ping { ping_id = Random.Shared.NextInt64() }, out written) == 0)
								continue; // not queued (path down meanwhile): nothing to report
						}
						finally
						{
							sem.Release();
						}
						// not awaited: it may queue behind this path's own (slow) writes, and this loop serves every path
						var idlePath = path;
						_ = written.ContinueWith(t => Helpers.Log(t.IsFaulted ? 2 : 1, t.IsFaulted
							? $"{_dcSession?.DcID}>Child: keepalive on path {idlePath.PathIndex} failed: {t.Exception?.InnerException?.Message}"
							: $"{_dcSession?.DcID}>Child: keepalive ping on idle path {idlePath.PathIndex}"), TaskScheduler.Default);
					}
					catch (OperationCanceledException) { throw; }
					catch (Exception ex)
					{
						Helpers.Log(2, $"{_dcSession?.DcID}>Child: keepalive on path {path.PathIndex} failed: {ex.Message}");
					}
				}
			}
		}

		private async Task ReconnectPathAsync(TransportPath path, Exception cause = null)
		{
			lock (_pathsLock)
				if (!_paths.Contains(path))
					return; // replaced by a full reset meanwhile (a late failure on its old connection): nothing to report
			// Prevent multiple concurrent reconnect loops for the same path (atomic guard)
			if (Interlocked.CompareExchange(ref path._reconnecting, 1, 0) != 0)
				return;
			// Notify subscribers that reconnect is now in progress (IsAlive=false, IsReconnecting=true).
			// The IsAlive=false transition was already reported by whichever code path set it
			// (Reactor error, health monitor, or send failure).
			RaisePathChanged(path);
			// Requests that only this path carried are copied to a live one with their own msg_id
			// (those also on a live path just wait for their answer). Done here, not in the reactor:
			// the teardown below cancels the reactor quietly.
			_ = RescueStrandedRpcsAsync($"P{path.PathIndex} down", cause ?? new IOException($"Path {path.PathIndex} down"));

			try
			{
				var endpoint = _dcSession?.EndPoint;
				if (endpoint == null)
					return;
				int dcId = _dcSession?.DcID ?? 0;

				for (int attempt = 1; ; attempt++)
				{
					// Tear down the previous connection (or the previous failed attempt). Cancelling its
					// token first makes its reactor exit quietly instead of reporting a path error, and
					// disposing the linked source unregisters it from _cts.
					CancellationTokenSource oldCts;
					lock (_pathsLock) // ResetAsync cancels path.Cts under this lock: never hand it a disposed one
					{
						oldCts = path.Cts;
						path.Cts = null;
					}
					oldCts?.Cancel();
					Stream oldStream;
					lock (_pathsLock) // unpublished first: no frame is encrypted for it from now on (see QueueFrameWrite)
					{
						oldStream = path.NetworkStream;
						path.NetworkStream = null;
					}
					oldStream?.Close();
					path.TcpClient?.Dispose();
					path.Sha256Send?.Dispose();
					path.Sha256Recv?.Dispose();
#if OBFUSCATION
					path.SendCtr?.Dispose();
					path.RecvCtr?.Dispose();
#endif
					oldCts?.Dispose();

					if (_cts?.IsCancellationRequested == true)
						return;
					// Abort if a full reconnect has removed this path from _paths
					// (ResetAsync clears _paths before ConnectAsync creates new ones)
					lock (_pathsLock)
					{
						if (!_paths.Contains(path))
							return;
					}
					try
					{
						Helpers.Log(2, $"{_dcSession.DcID}>Reconnecting path {path.PathIndex} (attempt {attempt})...");
						var tcpClient = await TcpHandler(endpoint.Address.ToString(), endpoint.Port, path.LocalEndPoint);

						// TcpHandler doesn't check CTS — re-check after it returns
						if (_cts?.IsCancellationRequested == true)
						{
							tcpClient.Dispose();
							return;
						}
						lock (_pathsLock)
						{
							if (!_paths.Contains(path))
							{
								tcpClient.Dispose();
								return;
							}
						}

						ConfigureKeepalive(tcpClient);
						var networkStream = (Stream)tcpClient.GetStream();
						byte[] preamble;
						byte protocolId = (byte)(path.PaddedMode ? 0xDD : 0xEE);
#if OBFUSCATION
						// the new connection's ciphers, published with its stream below (never beside the old stream)
						var (sendCtr, recvCtr, obfPreamble) = InitObfuscation(null, protocolId, dcId);
						preamble = obfPreamble;
#else
						preamble = new byte[] { protocolId, protocolId, protocolId, protocolId };
#endif
						await networkStream.WriteAsync(preamble, 0, preamble.Length);

						lock (_pathsLock) // a full reset may have dropped this path while we were connecting
						{
							if (!_paths.Contains(path))
							{
								tcpClient.Dispose();
#if OBFUSCATION
								sendCtr.Dispose();
								recvCtr.Dispose();
#endif
								return;
							}
#if OBFUSCATION
							(path.SendCtr, path.RecvCtr) = (sendCtr, recvCtr);
#endif
							path.TcpClient = tcpClient;
							path.NetworkStream = networkStream;
							path.Sha256Send = SHA256.Create();
							path.Sha256Recv = SHA256.Create();
							path.Cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
							path.Generation = Interlocked.Increment(ref _pathGeneration);
						}
						path.LastRecvTicks = Environment.TickCount64;
						path.LastProbeTicks = 0;
						path.LatencyEwmaMs = long.MaxValue; // stale RTT data — reset on reconnect
						// Clear any stale pending pings for this path from the global dictionary
						foreach (var kvp in _pendingPings)
							if (kvp.Value.PathIndex == path.PathIndex)
								_pendingPings.TryRemove(kvp.Key, out _);
						// Start reactor BEFORE ping (it needs to receive the Pong),
						// but do NOT set IsAlive until Telegram has answered the registration ping.
						path.ReactorTask = Reactor(path, path.NetworkStream, path.Cts.Token);
						if (!await RegisterPathAsync(path))
							throw new WTException($"registration ping unanswered after {Math.Max(PathDeadTimeout, 1)}s");

						// Now mark alive — server has acknowledged this path
						lock (_pathsLock)
						{
							if (!_paths.Contains(path))
								return;
							path.IsAlive = true;
						}
						path.ConnectedSinceTicks = Environment.TickCount64;
						Interlocked.Exchange(ref path.BytesSent, 0);
						Interlocked.Exchange(ref path.BytesRecv, 0);
						Interlocked.Increment(ref path.ReconnectCount);
						// Add reliability penalty: each reconnect adds 500ms effective latency.
						// This ensures LowestLatency mode steers away from frequently-failing paths.
						AddPenalty(path, 500);
						Helpers.Log(2, $"{_dcSession.DcID}>Path {path.PathIndex} reconnected and registered successfully.");
						RaisePathChanged(path);
						return;
					}
					catch (Exception ex)
					{
						Helpers.Log(3, $"{_dcSession.DcID}>Path {path.PathIndex} reconnect attempt {attempt} failed: {ex.Message}");
						if (_cts?.IsCancellationRequested == true)
							return;
						await Task.Delay(Math.Max(1000, Math.Min(attempt * 2000, PathReconnectMaxBackoff * 1000))); // backoff up to 30s (min 1s)
					}
				}
			}
			finally
			{
				Volatile.Write(ref path._reconnecting, 0);
			}
		}

		private async Task PathHealthMonitor(CancellationToken ct)
		{
			int probeIntervalMs = PathProbeInterval * 1000;
			int disconnectDelay = PathDisconnectDelay;
			int deadAfterMs = PathDeadTimeout * 1000;
			int ping_id = _random.Next();

			while (!ct.IsCancellationRequested)
			{
				await Task.Delay(1000, ct); // internal polling rate
				if (_paths.Count <= 1)
					continue;

				var now = Environment.TickCount64;
				TransportPath[] snapshot;
				lock (_pathsLock)
					snapshot = _paths.ToArray();

				// Collect alive paths that have been probed
				var alivePaths = snapshot.Where(p => p.IsAlive && p.LastRecvTicks > 0).ToArray();

				// GLOBAL CHECK: Are ALL alive paths unresponsive?
				// When this happens, per-path reconnects create zombie connections that
				// Telegram ignores. We must do a full reconnect (ResetAsync + ConnectAsync
				// + InitConnection + Updates_GetState) to properly re-register the session.
				if (alivePaths.Length > 0 && !alivePaths.Any(p =>
					(now - p.LastRecvTicks) <= deadAfterMs || p.LastProbeWrittenTicks <= p.LastRecvTicks))
				{
					// Check if the Reactor is already handling a full reconnect
					lock (_pathsLock)
					{
						if (_fullReconnectStarted)
							continue;
						_fullReconnectStarted = true;
					}
					Helpers.Log(4, $"{_dcSession.DcID}>All {alivePaths.Length} path(s) globally unresponsive. Triggering full reconnect.");
					// Fire-and-forget because ResetAsync will cancel our ct
					_ = Task.Run(async () =>
					{
						try
						{
							await PerformFullReconnectAsync();
						}
						catch (ObjectDisposedException) { } // client shutting down, expected
						catch (Exception ex)
						{
							Helpers.Log(5, $"{_dcSession.DcID}>Full reconnect from health monitor failed: {ex.Message}");
						}
					});
					return; // exit this health monitor; ConnectAsync starts a new one
				}

				// PER-PATH CHECKS: individual path failures while others are alive
				// NOTE: Telegram's MTProto can route responses (including Pong) to ANY
				// connection in the session, not necessarily the one that sent the request.
				// So a "silent" path isn't necessarily dead — the server may just be routing
				// all responses through another path. Only kill a path if the DC as a whole
				// has no recent receive activity, OR if this path has been silent significantly
				// longer than the dead timeout while other paths are still active.
				bool anyPathReceiving = alivePaths.Any(p => (now - p.LastRecvTicks) <= deadAfterMs);

				foreach (var path in snapshot)
				{
					if (!path.IsAlive || path.LastRecvTicks == 0)
						continue;
					var silentMs = now - path.LastRecvTicks;

					// probe WRITTEN after the last receive (one still queued behind slow writes proves nothing; a path
					// whose writes are stuck is closed by WriteFrameAsync's timeout instead)
					if (silentMs > deadAfterMs && path.LastProbeWrittenTicks > path.LastRecvTicks && !anyPathReceiving)
					{
						// This individual path is unresponsive AND no other paths are receiving
						// data either — the DC may be genuinely unreachable from this address.
						Helpers.Log(3, $"{_dcSession.DcID}>Path {path.PathIndex} unresponsive ({silentMs / 1000}s silent, probe unanswered, no other paths receiving). Force-closing.");
						path.IsAlive = false;
						RaisePathChanged(path);
						path.NetworkStream?.Close();
					}
					else if ((now - path.LastProbeTicks) >= probeIntervalMs)
					{
						// Time for a per-path PingDelayDisconnect (liveness probe + server-side keepalive).
						// Track the probe in the global _pendingPings dict so the Reactor can attribute
						// the Pong to the correct SENDING path even if Telegram routes it elsewhere.
						long thisPingId = ping_id++;
						path.LastProbeTicks = now;
						var sem = _sendSemaphore; // the same instance must be released (ResetAsync swaps it)
						try
						{
							Task written;
							long queued;
							await sem.WaitAsync(ct);
							try
							{
								queued = QueueOnPath(path, new TL.Methods.PingDelayDisconnect { ping_id = thisPingId, disconnect_delay = disconnectDelay }, out written);
							}
							finally
							{
								sem.Release();
							}
							// Not awaited here: the probe may wait behind this path's own writes (file parts on a slow
							// uplink), and this loop serves every path. Its clock starts when it is written, so time
							// spent queued is neither RTT nor a missed probe.
							if (queued != 0)
								_ = TrackProbeAsync(written, thisPingId, path);
						}
						catch { /* path might already be dead, next cycle will catch it */ }
					}
				}

				// INDIVIDUAL PATH DEATH from missed probes: a probe counts as missed once it has gone
				// unanswered for PathDeadTimeout (a probe sent a moment ago is not missed, it is in
				// flight). Two missed probes while other paths ARE receiving = the path is dead (not
				// just "server routing responses elsewhere" — the Pong handler updates the SENDER
				// path regardless of which connection receives the Pong). Killing it re-sends its
				// requests on the other path with their own msg_id (RescueStrandedRpcsAsync).
				if (anyPathReceiving)
				{
					var missedByPath = new Dictionary<int, int>();
					var staleKeys = new List<long>();
					foreach (var kvp in _pendingPings)
					{
						var (pathIdx, sentAt) = kvp.Value;
						if ((now - sentAt) > 60_000) // 60s >> default probe interval (3s); safe unless PathDisconnectDelay > 60s
							staleKeys.Add(kvp.Key);
						else if ((now - sentAt) > deadAfterMs)
							missedByPath[pathIdx] = missedByPath.GetValueOrDefault(pathIdx) + 1;
					}
					foreach (var key in staleKeys)
						_pendingPings.TryRemove(key, out _);

					var toNotifyMissed = new List<TransportPath>();
					lock (_pathsLock)
					{
						foreach (var (pathIdx, missed) in missedByPath)
						{
							if (missed < 2)
								continue;
							if (pathIdx >= _paths.Count)
								continue;
							var deadPath = _paths[pathIdx];
							if (!deadPath.IsAlive)
								continue;
							if (now - Volatile.Read(ref deadPath.LastRecvTicks) <= deadAfterMs)
								continue; // still receiving (e.g. answers to file parts while Telegram delays its Pongs): not dead
							if (_paths.Any(p => p.IsAlive && p.PathIndex != pathIdx))
							{
								Helpers.Log(3, $"{_dcSession.DcID}>Path {pathIdx} has {missed} unanswered probes while other paths are active. Force-closing.");
								deadPath.IsAlive = false;
								AddPenalty(deadPath, 500);
								toNotifyMissed.Add(deadPath);
								deadPath.NetworkStream?.Close();
							}
						}
					}
					foreach (var path in toNotifyMissed)
						RaisePathChanged(path);
				}

				// RPC STALL: a request not yet copied (file parts; everything else was copied when
				// sent) gets a copy on the other path, and its path is probed now. Never a kill.
				await CopyStalledRpcsAsync(probe: true);
				await RescueStrandedRpcsAsync("No live connection", new IOException("No live connection carries the request"));
				LogCopyStats();

				DecayPenalties(snapshot);
			}
		}

		/// <summary>Registers a liveness probe once its frame is written (see the health monitor).</summary>
		private async Task TrackProbeAsync(Task written, long pingId, TransportPath path)
		{
			try
			{
				await written;
				long now = Environment.TickCount64;
				_pendingPings[pingId] = (path.PathIndex, now);
				Volatile.Write(ref path.LastProbeWrittenTicks, now);
			}
			catch { /* write failed: the path's reactor fails and reconnects it */ }
		}

		/// <summary>Decay penalty for alive healthy paths — each 1s cycle reduces penalty by ~10%.
		/// This allows rehabilitated paths to regain trust after sustained stability.
		/// A path with 500ms penalty (1 reconnect) recovers to &lt;50ms in ~23 seconds.
		/// A path with 5000ms penalty (10 reconnects) recovers to &lt;50ms in ~46 seconds.
		/// Called once a second by both path monitors.</summary>
		private static void DecayPenalties(TransportPath[] snapshot)
		{
			foreach (var path in snapshot)
			{
				if (!path.IsAlive)
					continue;
				long penalty = Volatile.Read(ref path.PenaltyMs);
				if (penalty > 0)
				{
					// EWMA-style decay: new = old * 0.9 (drop ~10% per second)
					long decayed = penalty * 9 / 10;
					if (decayed < 10)
						decayed = 0; // snap to zero when negligible
					Volatile.Write(ref path.PenaltyMs, decayed);
				}
			}
		}

		/// <summary>
		/// Performs a full reconnect: tears down all connections and re-establishes
		/// from scratch (auth, InitConnection, Updates_GetState). Called by the health
		/// monitor when all paths are globally dead, bypassing the Reactor error handler
		/// to avoid races with per-path reconnects.
		/// </summary>
		private async Task PerformFullReconnectAsync()
		{
			Helpers.Log(2, $"{_dcSession?.DcID}>PerformFullReconnectAsync: starting...");

			for (int attempt = 1; ; attempt++)
			{
				try
				{
					lock (_msgsToAck)
						_msgsToAck.Clear();
					await ResetAsync(false, false);
					// ResetAsync resets _fullReconnectStarted = false (under _pathsLock).
					// Re-claim ownership immediately so no other thread can launch a second
					// PerformFullReconnectAsync between here and ConnectAsync completing.
					lock (_pathsLock)
						_fullReconnectStarted = true;
					// ResetAsync creates _sendSemaphore = new(0), blocking all external sends.
					// DoConnectAsync releases it on success (line 1475).
					// We do NOT acquire the semaphore here — that would deadlock because it starts at 0.

					await Task.Delay(Math.Max(1000, Math.Min(attempt * 2000, PathReconnectMaxBackoff * 1000))); // backoff: 2s, 4s, 6s, ... up to 30s (min 1s)
					await ConnectAsync();

					// Success — same session, so pending RPCs are re-sent with their own msg_id
					// (executed once); the rest are retried by Invoke with a new msg_id
					await ResendPendingAfterReconnectAsync(new ReactorError { Exception = new IOException("All paths dead — full reconnect") });
					if (IsMainDC)
					{
						var updatesState = await this.Updates_GetState();
						RaiseUpdates(updatesState);
					}
					Helpers.Log(2, $"{_dcSession?.DcID}>PerformFullReconnectAsync: completed successfully.");
					lock (_pathsLock)
						_fullReconnectStarted = false;
					return; // done — DoConnectAsync already released _sendSemaphore
				}
				catch (ObjectDisposedException)
				{
					// Client was genuinely disposed — propagate
					lock (_pathsLock)
						_fullReconnectStarted = false;
					throw;
				}
				catch (Exception ex)
				{
					Helpers.Log(4, $"{_dcSession?.DcID}>PerformFullReconnectAsync attempt {attempt} failed: {ex.Message}. Retrying...");
					// ConnectAsync failed, so _sendSemaphore is still at 0 (locked).
					// After ResetAsync, any callers waiting on the old semaphore are already unblocked
					// (it was a different instance). New callers block on the new semaphore until
					// ConnectAsync succeeds. No release needed here — releasing would let callers
					// through on a broken connection.
				}
			}
		}

		private static void ConfigureKeepalive(TcpClient tcp)
		{
			tcp.Client.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.KeepAlive, true);
			tcp.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveTime, 5);
			tcp.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveInterval, 2);
			tcp.Client.SetSocketOption(SocketOptionLevel.Tcp, SocketOptionName.TcpKeepAliveRetryCount, 3);
		}

		/// <summary>Queue a raw message directly on a specific transport path (bypassing primary path selection).
		/// Caller holds the send semaphore, and awaits <paramref name="written"/> only after releasing it.</summary>
		/// <param name="path">The path to queue it on</param>
		/// <param name="msg">The message (a service message: no seqno content bit)</param>
		/// <param name="written">Completes once the frame is written; fails only with an IOException</param>
		/// <param name="registering">The path is not alive yet: this is its registration ping</param>
		/// <param name="beforeWrite">Called with the msg_id and the generation of the connection it goes out on, right before
		/// the frame is queued, after everything that can fail (to register for its answer)</param>
		/// <returns>The msg_id it was sent with, or 0 when the path cannot take it (<paramref name="written"/> is then complete)</returns>
		private long QueueOnPath(TransportPath path, IObject msg, out Task written, bool registering = false, Action<long, long> beforeWrite = null)
		{
			written = Task.CompletedTask;
			// The connection's parts, read together (a reconnect publishes them together under _pathsLock); its cipher is
			// checked against the stream when the frame is queued (QueueFrameWrite)
			Stream stream;
			long connection;
			SHA256 sha256Send;
			bool paddedMode;
			lock (_pathsLock)
				(stream, connection, sha256Send, paddedMode) = (path.NetworkStream, path.Generation, path.Sha256Send, path.PaddedMode);
			if ((!path.IsAlive && !registering) || stream == null || _dcSession.authKeyID == 0)
				return 0;
			var (msgId, seqno) = NewMsgId(false);
			using var memStream = new MemoryStream(1024);
			using var writer = new BinaryWriter(memStream);
			writer.Write(0); // payload_len placeholder

			CheckSalt();
			using var clearStream = new MemoryStream(1024);
			using var clearWriter = new BinaryWriter(clearStream);
			clearWriter.Write(_dcSession.AuthKey, 88, 32);
			clearWriter.Write(_dcSession.Salt);
			clearWriter.Write(_dcSession.id);
			clearWriter.Write(msgId);
			clearWriter.Write(seqno);
			clearWriter.Write(0); // message_data_length placeholder
			Helpers.Log(1, $"{_dcSession.DcID}>Sending   {msg.GetType().Name.TrimEnd('_'),-40} {MsgIdToStamp(msgId):u} (svc) [path {path.PathIndex}]");
			clearWriter.WriteTLObject(msg);
			int clearLength = (int)clearStream.Length - 32;
			int padding = (0x7FFFFFF0 - clearLength) % 16;
			padding += _random.Next(2, 16) * 16;
			clearStream.SetLength(32 + clearLength + padding);
			byte[] clearBuffer = clearStream.GetBuffer();
			BinaryPrimitives.WriteInt32LittleEndian(clearBuffer.AsSpan(60), clearLength - 32);
			RNG.GetBytes(clearBuffer, 32 + clearLength, padding);
			var msgKeyLarge = sha256Send.ComputeHash(clearBuffer, 0, 32 + clearLength + padding);
			const int msgKeyOffset = 8;
			byte[] encrypted_data = EncryptDecryptMessage(clearBuffer.AsSpan(32, clearLength + padding), true, 0, _dcSession.AuthKey, msgKeyLarge, msgKeyOffset, sha256Send);

			writer.Write(_dcSession.authKeyID);
			writer.Write(msgKeyLarge, msgKeyOffset, 16);
			writer.Write(encrypted_data);

			if (paddedMode)
			{
				var pad = new byte[_random.Next(16)];
				RNG.GetBytes(pad);
				writer.Write(pad);
			}
			var buffer = memStream.GetBuffer();
			int frameLength = (int)memStream.Length;
			BinaryPrimitives.WriteInt32LittleEndian(buffer, frameLength - 4);
			// after everything that can fail (salt, serialisation, encryption), right before the frame is queued: what it
			// registers or marks is never left behind for a frame that was not sent
			beforeWrite?.Invoke(msgId, connection);
			written = WrittenAsync(QueueFrameWrite(path, stream, buffer, frameLength));
			return msgId;

			async Task WrittenAsync(Task write)
			{
				await write;
				Interlocked.Add(ref path.BytesSent, frameLength);
				Volatile.Write(ref path.LastSendTicks, Environment.TickCount64);
			}
		}

		/// <summary>Queues a built frame on a path: the obfuscation (CTR) is applied here, in queue order, and the
		/// write starts once the path's previous write is done. Called under the send semaphore (so frames keep
		/// the order they were built in); the caller awaits the returned task AFTER releasing it, so a path that
		/// is slow to drain holds up only its own frames. The task only ever fails with an IOException.</summary>
		private Task QueueFrameWrite(TransportPath path, Stream stream, byte[] buffer, int length)
		{
			lock (path.WriteChainLock)
			{
#if OBFUSCATION
				// Only for the connection the frame was built for, with that connection's cipher (a reconnect unpublishes the
				// old stream before disposing its cipher, and publishes the new stream with the new cipher, under _pathsLock):
				// a frame for a gone connection must never advance the CTR of the next one.
				AesCtr ctr;
				lock (_pathsLock) // nested in WriteChainLock: no _pathsLock section takes a WriteChainLock
				{
					if (!ReferenceEquals(path.NetworkStream, stream))
						return Task.FromException(new IOException($"Path {path.PathIndex}: connection replaced before the write"));
					ctr = path.SendCtr;
				}
				try
				{
					ctr?.EncryptDecrypt(buffer.AsSpan(0, length));
				}
				catch (Exception ex) // its cipher disposed meanwhile: the connection is gone
				{
					return Task.FromException(new IOException($"Path {path.PathIndex}: connection gone before the write ({ex.Message})", ex));
				}
#endif
				var task = WriteAfterAsync(path.WriteChain, stream, buffer, length);
				path.WriteChain = task;
				Volatile.Write(ref path.LastQueuedTicks, Environment.TickCount64);
				return task;
			}
		}

		private static async Task WriteAfterAsync(Task previous, Stream stream, byte[] buffer, int length)
		{
			try { await previous.ConfigureAwait(false); }
			catch { } // a failed write closed its stream: this one fails on its own if it is the same stream
			try
			{
				await WriteFrameAsync(stream, buffer, length).ConfigureAwait(false);
			}
			catch (Exception ex) when (ex is not IOException)
			{
				throw new IOException($"Write failed: {ex.Message}", ex); // ObjectDisposedException: closed meanwhile
			}
		}

		/// <summary>Writes one frame on a path connection. A write that cannot complete in time (peer
		/// blackholed, send buffer full) closes that connection and throws IOException, instead of holding
		/// the send semaphore, and with it every path, until TCP gives up.</summary>
		private static async Task WriteFrameAsync(Stream stream, byte[] buffer, int length)
		{
			int timeoutMs = Math.Max(5000, length / 32); // allows down to ~32 KB/s for big file parts
			using var timeout = new CancellationTokenSource(timeoutMs);
			try
			{
				await stream.WriteAsync(buffer.AsMemory(0, length), timeout.Token);
			}
			catch (Exception) when (timeout.IsCancellationRequested) // OperationCanceledException, or wrapped in an IOException
			{
				stream.Close();
				throw new IOException($"Write of {length} bytes timed out after {timeoutMs / 1000}s");
			}
		}

		internal DateTime MsgIdToStamp(long serverMsgId)
			=> new((serverMsgId >> 32) * 10000000 - _dcSession.serverTicksOffset + 621355968000000000L, DateTimeKind.Utc);

		internal IObject ReadFrame(byte[] data, int dataLen) => ReadFrame(data, dataLen, _sha256Recv, _paddedMode, -1);

		internal IObject ReadFrame(byte[] data, int dataLen, SHA256 sha256Recv, bool paddedMode, int pathIndex = -1)
		{
			var pathTag = pathIndex >= 0 ? $" [P{pathIndex}]" : "";
			if (dataLen < 8 && data[3] == 0xFF)
			{
				int error_code = -BinaryPrimitives.ReadInt32LittleEndian(data);
				throw new RpcException(error_code, TransportError(error_code));
			}
			if (dataLen < 24) // authKeyId+msgId+length+ctorNb | authKeyId+msgKey
				throw new WTException($"Packet payload too small: {dataLen}");

			long authKeyId = BinaryPrimitives.ReadInt64LittleEndian(data);
			if (authKeyId != _dcSession.authKeyID)
				throw new WTException($"Received a packet encrypted with unexpected key {authKeyId:X}");
			if (authKeyId == 0) // Unencrypted message
			{
				using var reader = new BinaryReader(new MemoryStream(data, 8, dataLen - 8));
				long msgId = _lastRecvMsgId = reader.ReadInt64();
				if ((msgId & 1) == 0)
					throw new WTException($"Invalid server msgId {msgId}");
				int length = reader.ReadInt32();
				dataLen -= 20;
				if (length > dataLen || dataLen - length > (paddedMode ? 256 : 0))
					throw new WTException($"Unexpected unencrypted/padding length {dataLen} - {length}");

				var obj = reader.ReadTLObject();
				Helpers.Log(1, $"{_dcSession.DcID}>Receiving {obj.GetType().Name,-40} {MsgIdToStamp(msgId):u} clear{((msgId & 2) == 0 ? "" : " NAR")}{pathTag}");
				if (_bareRpc == null)
					throw new WTException("Shouldn't receive unencrypted packet at this point");
				return obj;
			}
			else
			{
				byte[] decrypted_data = EncryptDecryptMessage(data.AsSpan(24, (dataLen - 24) & ~0xF), false, 8, _dcSession.AuthKey, data, 8, sha256Recv);
				if (decrypted_data.Length < 36) // header below+ctorNb
					throw new WTException($"Decrypted packet too small: {decrypted_data.Length}");
				sha256Recv.TransformBlock(_dcSession.AuthKey, 96, 32, null, 0);
				sha256Recv.TransformFinalBlock(decrypted_data, 0, decrypted_data.Length);
				if (!data.AsSpan(8, 16).SequenceEqual(sha256Recv.Hash.AsSpan(8, 16)))
					throw new WTException("Mismatch between MsgKey & decrypted SHA256");
				sha256Recv.Initialize();
				using var reader = new BinaryReader(new MemoryStream(decrypted_data));
				var serverSalt = reader.ReadInt64();    // int64 salt
				var sessionId = reader.ReadInt64();     // int64 session_id
				var msgId = reader.ReadInt64();         // int64 message_id
				var seqno = reader.ReadInt32();         // int32 msg_seqno
				var length = reader.ReadInt32();        // int32 message_data_length

				if (length < 0 || length % 4 != 0)
					throw new WTException($"Invalid message_data_length: {length}");
				if (decrypted_data.Length - 32 - length is < 12 or > 1024)
					throw new WTException($"Invalid message padding length: {decrypted_data.Length - 32}-{length}");
				if (sessionId != _dcSession.id)
					throw new WTException($"Unexpected session ID: {sessionId} != {_dcSession.id}");
				if ((msgId & 1) == 0)
					throw new WTException($"msg_id is not odd: {msgId}");
				bool newMsg;
				lock (_dcSession) newMsg = _dcSession.CheckNewMsgId(msgId);
				if (!newMsg)
				{
					// routine with several paths: the server answers a copied request on both connections
					Interlocked.Increment(ref _statDupFrames);
					Helpers.Log(_paths.Count > 1 ? 1 : 3, $"{_dcSession.DcID}>Ignoring duplicate or old msg_id {msgId}{pathTag}");
					return null;
				}
				var utcNow = DateTime.UtcNow;
				if (_lastRecvMsgId == 0) // resync ServerTicksOffset on first message
					_dcSession.serverTicksOffset = (msgId >> 32) * 10000000 - utcNow.Ticks + 621355968000000000L;
				var msgStamp = MsgIdToStamp(_lastRecvMsgId = msgId);
				_frameMsgId.Value = msgId; // flows to this reactor's HandleMessageAsync only (other reactors have their own)
				long deltaTicks = (msgStamp - utcNow).Ticks;
				if (deltaTicks is > 0)
					if (deltaTicks < Ticks5Secs) // resync if next message is less than 5 seconds in the future
						_dcSession.serverTicksOffset += deltaTicks;
					else if (_dcSession.serverTicksOffset < -Ticks5Secs && deltaTicks + _dcSession.serverTicksOffset < 0)
						_dcSession.serverTicksOffset += deltaTicks;
				if (serverSalt != _dcSession.Salt && serverSalt != _dcSession.OldSalt && serverSalt != _dcSession.Salts?.Values.ElementAtOrDefault(1))
				{
					Helpers.Log(3, $"{_dcSession.DcID}>Server salt has changed: {_dcSession.Salt:X} -> {serverSalt:X}");
					_dcSession.OldSalt = _dcSession.Salt;
					_dcSession.Salt = serverSalt;
					lock (_session) _session.Save();
					if (++_saltChangeCounter >= 10)
						throw new WTException("Server salt changed too often! Security issue?");
					CheckSalt();
				}
				if ((seqno & 1) != 0)
					lock (_msgsToAck)
						_msgsToAck.Add(msgId);

				var ctorNb = reader.ReadUInt32();
				if (ctorNb != Layer.BadMsgCtor && deltaTicks / TimeSpan.TicksPerSecond is > 30 or < -300)
				{   // msg_id values that belong over 30 seconds in the future or over 300 seconds in the past are to be ignored.
					Helpers.Log(1, $"{_dcSession.DcID}>Ignoring  0x{ctorNb:X8} because of wrong timestamp    {msgStamp:u} - {utcNow:u} Δ={new TimeSpan(_dcSession.serverTicksOffset):c}{pathTag}");
					return null;
				}
				try
				{
					if (ctorNb == Layer.MsgContainerCtor)
					{
						Helpers.Log(1, $"{_dcSession.DcID}>Receiving {"MsgContainer",-40} {msgStamp:u} (svc){pathTag}");
						return ReadMsgContainer(reader, pathIndex);
					}
					else if (ctorNb == Layer.RpcResultCtor)
					{
						Helpers.Log(1, $"{_dcSession.DcID}>Receiving {"RpcResult",-40} {msgStamp:u}{pathTag}");
						return ReadRpcResult(reader, pathIndex);
					}
					else
					{
						var obj = reader.ReadTLObject(ctorNb);
						Helpers.Log(1, $"{_dcSession.DcID}>Receiving {obj.GetType().Name,-40} {msgStamp:u} {((seqno & 1) != 0 ? "" : "(svc)")} {((msgId & 2) == 0 ? "" : "NAR")}{pathTag}");
						return obj;
					}
				}
				catch (Exception ex)
				{
					Helpers.Log(4, $"While deserializing frame #{ctorNb:x}: " + ex.ToString());
					return null;
				}
			}
		}

		static string TransportError(int error_code) => error_code switch
		{
			404 => "Auth key not found",
			429 => "Transport flood",
			444 => "Invalid DC",
			_ => Enum.GetName(typeof(HttpStatusCode), error_code) ?? "Transport error"
		};

		internal void CheckSalt()
		{
			lock (_session)
			{
				_dcSession.Salts ??= [];
				if (_dcSession.Salts.Count != 0)
				{
					var keys = _dcSession.Salts.Keys;
					if (keys[^1] == DateTime.MaxValue)
						return; // GetFutureSalts ongoing
					var now = DateTime.UtcNow.AddTicks(_dcSession.serverTicksOffset - TimeSpan.TicksPerMinute);
					bool removed = false;
					for (; keys.Count > 1 && keys[1] < now; _dcSession.OldSalt = _dcSession.Salt, _dcSession.Salt = _dcSession.Salts.Values[0], removed = true)
						_dcSession.Salts.RemoveAt(0);
					if (removed)
						_session.Save();
					if (_dcSession.Salts.Count > 48)
						return;
				}
				_dcSession.Salts[DateTime.MaxValue] = 0;
			}
			Task.Delay(5000).ContinueWith(_ => this.GetFutureSalts(128).ContinueWith(gfs =>
			{
				lock (_session)
				{
					_dcSession.Salts.Remove(DateTime.MaxValue);
					foreach (var entry in gfs.Result.salts)
						_dcSession.Salts[entry.valid_since] = entry.salt;
					_dcSession.OldSalt = _dcSession.Salt;
					_dcSession.Salt = _dcSession.Salts.Values[0];
					_session.Save();
				}
			}));
		}

		/// <param name="pathIndex">Path the frame arrived on (-1 = unknown)</param>
		internal MsgContainer ReadMsgContainer(BinaryReader reader, int pathIndex = -1)
		{
			int count = reader.ReadInt32();
			var messages = new List<_Message>(count);
			for (int i = 0; i < count; i++)
			{
				var msg = new _Message(reader.ReadInt64(), reader.ReadInt32(), null) { bytes = reader.ReadInt32() };
				messages.Add(msg);
				if ((msg.seqno & 1) != 0)
					lock (_msgsToAck)
						_msgsToAck.Add(msg.msg_id);
				var pos = reader.BaseStream.Position;
				try
				{
					var ctorNb = reader.ReadUInt32();
					if (ctorNb == Layer.RpcResultCtor)
					{
						Helpers.Log(1, $"             → {"RpcResult",-38} {MsgIdToStamp(msg.msg_id):u}");
						msg.body = ReadRpcResult(reader, pathIndex);
					}
					else
					{
						var obj = msg.body = reader.ReadTLObject(ctorNb);
						Helpers.Log(1, $"             → {obj.GetType().Name,-38} {MsgIdToStamp(msg.msg_id):u} {((msg.seqno & 1) != 0 ? "" : "(svc)")} {((msg.msg_id & 2) == 0 ? "" : "NAR")}");
					}
				}
				catch (Exception ex)
				{
					Helpers.Log(4, "While deserializing vector<%Message>: " + ex.ToString());
				}
				reader.BaseStream.Position = pos + msg.bytes;
			}
			return new MsgContainer { messages = messages };
		}

		/// <param name="pathIndex">Path the answer arrived on (-1 = unknown): a file part's answer is a speed sample</param>
		private RpcResult ReadRpcResult(BinaryReader reader, int pathIndex = -1)
		{
			long msgId = reader.ReadInt64();
			Rpc rpc;
			bool settledBefore = false;
			lock (_pendingRpcs) // pull + settle mark atomically: two reactors may get the two answers at once
			{
				if (_pendingRpcs.Remove(msgId, out rpc))
					_settledHedged[msgId] = Environment.TickCount64;
				else
					settledBefore = _settledHedged.ContainsKey(msgId);
			}
			PruneCopyState(); // at most every 10 s; this is the one path every client takes, single-path included
			object result;
			if (settledBefore)
			{
				// second answer (to a copy, or re-delivered on another connection): already delivered once
				Interlocked.Increment(ref _statDupAnswers);
				Helpers.Log(1, $"              → second answer for #{(short)msgId.GetHashCode():X4} dropped (already answered)");
				return new RpcResult { req_msg_id = msgId };
			}
			if (rpc != null)
			{
				try
				{
					if (!rpc.type.IsArray)
					{
						result = reader.ReadTLValue(rpc.type);
					}
					else
					{
						var peek = reader.ReadUInt32();
						if (peek == Layer.RpcErrorCtor)
							result = reader.ReadTLObject(Layer.RpcErrorCtor);
						else if (peek == Layer.GZipedCtor)
						{
							result = reader.ReadTLGzipped(rpc.type);
						}
						else
						{
							reader.BaseStream.Position -= 4;
							result = reader.ReadTLVector(rpc.type);
						}
					}
					if (rpc.type.IsEnum)
						result = Enum.ToObject(rpc.type, result);
					if (result is RpcError rpcError)
					{
						Helpers.Log(4, $"             → RpcError {rpcError.error_code,3} {rpcError.error_message,-24} #{(short)msgId.GetHashCode():X4}");
					}
					else
					{
						Helpers.Log(1, $"             → {result?.GetType().Name,-37} #{(short)msgId.GetHashCode():X4}");
						CheckRaiseOwnUpdates(result);
					}
					if (rpc.transferDir >= 0 && result is not RpcError)
					{
						if (result is Upload_File file)
							rpc.transferBytes = file.bytes?.Length ?? 0;
						RecordTransferSample(rpc, pathIndex, rpc.transferBytes);
					}

					rpc.tcs.TrySetResult(result); // Try, like every completion: removed from the pending requests above, only we complete it
				}
				catch (Exception ex)
				{
					rpc.tcs.TrySetException(ex);
					throw;
				}
				finally
				{
					SettledTransfer(rpc); // after the caller has its result or error (never throws)
				}
			}
			else
			{
				var ctorNb = reader.ReadUInt32();
				if (ctorNb == Layer.VectorCtor)
				{
					reader.BaseStream.Position -= 4;
					result = reader.ReadTLVector(typeof(IObject[]));
				}
				else if (ctorNb == (uint)Bool.False)
				{
					result = false;
				}
				else if (ctorNb == (uint)Bool.True)
				{
					result = true;
				}
				else
				{
					result = reader.ReadTLObject(ctorNb);
					CheckRaiseOwnUpdates(result);
				}

				var typeName = result?.GetType().Name;
				if (MsgIdToStamp(msgId) >= _session.SessionStart)
					Helpers.Log(4, $"              → {typeName,-37} for unknown msgId #{(short)msgId.GetHashCode():X4}");
				else
					Helpers.Log(1, $"              → {typeName,-37} for past msgId #{(short)msgId.GetHashCode():X4}");
			}
			return new RpcResult { req_msg_id = msgId, result = result };
		}

		private sealed class Rpc
		{
			internal Type type;
			internal TaskCompletionSource<object> tcs = new(TaskCreationOptions.RunContinuationsAsynchronously);
			internal long msgId;
			internal int sentPathIndex = -1; // which TransportPath this was sent on (-1 = legacy/unknown)
			internal long sentGen; // TransportPath.Generation of that connection (an index outlives its connections)
			internal long sentTicks; // Environment.TickCount64 when registered (its msg_id's age)
			internal long writtenTicks; // Environment.TickCount64 when its frame was written, for stall detection
			// The request itself and its seqno, kept so it can be re-sent with the SAME msg_id
			// (wrapped in a fresh container, as MTProto allows) on another path: the server
			// executes a msg_id once, so a copy can never become a duplicate action.
			internal IObject query;
			internal int seqno;
			internal long containerMsgId; // container the original went out in (BadMsgNotification may name it)
			internal bool bulk; // file part upload/download: copied only once it stalls, never up front
			internal volatile bool copyAttempted; // a copy may be out: a second answer must be dropped (set before the write)
			// Copies of this request, all changed under lock(rpc) (see MarkCopyLive / CopyGone): the live ones, queued and not
			// known to have failed or been refused (by container id, with the path and connection they went out on); the
			// pending ones, registered and being queued but not marked live yet (only inside SendCopyAsync's semaphore
			// section; HasLiveCarrier can see them from another thread); and of those the ones the server refused meanwhile.
			// LOCK ORDER: lock(rpc) may nest _pendingRpcs or _pathsLock (SyncTransferCopy, DropDeadCopies), never the other
			// way: no _pendingRpcs or _pathsLock section takes lock(rpc). _transferLock is taken after releasing lock(rpc).
			// The send semaphore may be held while taking lock(rpc), _pathsLock (QueueOnPath) or a path's WriteChainLock
			// (which nests _pathsLock: QueueFrameWrite); nothing waits on the semaphore while holding any lock.
			internal readonly Dictionary<long, (TransportPath Path, long Gen, long Seq)> liveCopies = [];
			internal readonly Dictionary<long, (TransportPath Path, long Gen)> pendingCopies = [];
			internal long copySeq; // order the copies went live in (msg_ids can go backwards after a time resync)
			internal readonly HashSet<long> refusedPending = [];
			internal volatile bool hedged; // liveCopies is not empty (kept in step with it)
			internal int copies; // copies counted against the budget: live ones and ones still being queued
			internal int copySyncVersion; // SyncTransferCopy: bumped under lock(rpc) at each change of liveCopies, and at settling
			internal int transferAttempt; // which attempt of its file part this request is (TransferLease.Attempts)
			internal int copySyncApplied; // ... and the last one applied, under _transferLock
			internal int soleCopies; // copies that were the only live carrier (each has a watchdog)
			internal int stateChecks; // msgs_state_req asked about this msg_id so far
			internal int stateCheckInFlight; // 1 while a state request (or its re-check delay) is outstanding
			internal volatile bool serverHasIt; // a state answer said "received": never a new msg_id after that
			// File transfer part (TransferMode Throughput): the scheduler's choice of path, and what is
			// needed to measure the transfer speed of whichever path(s) carried it
			internal TransferLease lease;
			internal int transferDir = -1; // TransferUp / TransferDown; -1 = not a file part
			internal int transferBytes; // part size (upload), or the requested limit (download)
			internal long stallExtraMs; // added to PathRpcStallTimeout: the time its path is expected to need
			internal long queuedTicks; // when its frame was queued on a path (it may wait there behind slow writes)
			internal int hedgeStarted; // 1 once HedgeIfWanted sent (or is sending) its copy
			public Task<object> Task => tcs.Task;
		}

		private class TransportPath : IDisposable
		{
			public TcpClient TcpClient;
			public Stream NetworkStream;
			public Task ReactorTask;
			public SHA256 Sha256Send = SHA256.Create();
			public SHA256 Sha256Recv = SHA256.Create();
#if OBFUSCATION
			public AesCtr SendCtr;
			public AesCtr RecvCtr;
#endif
			public bool PaddedMode;
			public IPEndPoint LocalEndPoint;
			public volatile bool IsAlive;
			public int _reconnecting; // 1 while ReconnectPathAsync is running (uses Interlocked for atomicity)
			public CancellationTokenSource Cts;
			public int PathIndex;
			public long LastRecvTicks;
			public long LastProbeTicks;
			// Latency tracking for LowestLatency mode (RTT measured via Client._pendingPings)
			public long LatencyEwmaMs = long.MaxValue; // EWMA RTT in ms; MaxValue = no samples yet
			// Reliability penalty: added to effective latency for path scoring.
			// Increased on reconnects/errors, decays naturally via EWMA.
			// Unit: milliseconds of virtual latency added. 0 = no penalty.
			public long PenaltyMs;
			// I/O counters (updated via Interlocked.Add for thread safety)
			public long BytesSent;
			public long BytesRecv;
			public int ReconnectCount;
			public long ConnectedSinceTicks; // Environment.TickCount64 when this path last became alive
			public long LastSendTicks; // Environment.TickCount64 of the last frame written on this path
			public long Generation; // unique per connection (Client._pathGeneration); 0 = never connected
			// Frames are written in order per path, each after the previous one, outside the send semaphore:
			// a slow path (a 512 KB part on a 1 Mbit uplink) then delays only its own frames, never the others'.
			public Task WriteChain = Task.CompletedTask;
			public readonly object WriteChainLock = new();
			public long LastQueuedTicks; // Environment.TickCount64 of the last frame queued (it may still be writing)
			public long FailedGeneration; // the connection a failed write was already acted on for (under _pathsLock)
			public long LastProbeWrittenTicks; // when the last liveness probe was actually written (not just queued)

			public void Dispose()
			{
				Sha256Send?.Dispose();
				Sha256Recv?.Dispose();
#if OBFUSCATION
				SendCtr?.Dispose();
				RecvCtr?.Dispose();
#endif
				NetworkStream?.Close();
				TcpClient?.Dispose();
				Cts?.Cancel();
				Cts?.Dispose();
			}
		}

		private Rpc PullPendingRequest(long msgId)
		{
			Rpc request;
			lock (_pendingRpcs)
				if (_pendingRpcs.TryGetValue(msgId, out request))
					_pendingRpcs.Remove(msgId);
			return request; // (bare requests, Pongs and FutureSalts only: never a file part, no transfer to settle)
		}

		private async Task HandleMessageAsync(IObject obj)
		{
			if (_bareRpc != null)
			{
				var rpc = PullPendingRequest(_bareRpc.msgId);
				if ((rpc?.type.IsAssignableFrom(obj.GetType())) == true)
				{
					_bareRpc = null;
					rpc.tcs.TrySetResult(obj);
					return;
				}
				else if (_dcSession.authKeyID == 0)
					throw new WTException($"Received a {obj.GetType()} incompatible with expected bare {rpc?.type}");
				lock (_pendingRpcs)
					_pendingRpcs[_bareRpc.msgId] = _bareRpc;
			}
			switch (obj)
			{
				case MsgContainer container:
					foreach (var msg in container.messages)
						if (msg.body != null)
						{
							_frameMsgId.Value = msg.msg_id;
							await HandleMessageAsync(msg.body);
						}
					break;
				case MsgCopy msgCopy:
					if (msgCopy?.orig_message?.body != null)
					{
						_frameMsgId.Value = msgCopy.orig_message.msg_id;
						await HandleMessageAsync(msgCopy.orig_message.body);
					}
					break;
				case TL.Methods.Ping ping: // msg_id of THIS ping's message: _lastRecvMsgId is shared by all path reactors
					_ = SendAsync(new Pong { msg_id = _frameMsgId.Value, ping_id = ping.ping_id }, false);
					break;
				case Pong pong:
					OnPongReceived(pong);
					SetResult(pong.msg_id, pong);
					RaiseUpdates(pong);
					break;
				case MsgsStateInfo stateInfo when _stateChecks.TryRemove(stateInfo.req_msg_id, out var checkedRpc):
					if (stateInfo.info?.Length > 0)
						_ = OnMsgStateAsync(checkedRpc, stateInfo.info[0]);
					else
						_ = RecheckLaterAsync(checkedRpc, "empty state answer");
					break;
				case MsgsStateInfo: // late (its check timed out and was re-asked) or the reply to a msg_resend_req
					Helpers.Log(1, $"{_dcSession.DcID}>Ignoring untracked msgs_state_info");
					break;
				case MsgDetailedInfo detailedInfo:
					// The server already answered this msg_id (typically: a copy arrived after the original
					// was answered). If we never got that answer, ask for it to be sent again.
					bool waiting;
					lock (_pendingRpcs)
						waiting = _pendingRpcs.ContainsKey(detailedInfo.msg_id);
					if (waiting)
					{
						Helpers.Log(2, $"{_dcSession.DcID}>Answer to #{(short)detailedInfo.msg_id.GetHashCode():X4} was lost; requesting it again");
						_ = SendAsync(new MsgResendReq { msg_ids = [detailedInfo.answer_msg_id] }, false);
					}
					else
						lock (_msgsToAck)
							_msgsToAck.Add(detailedInfo.answer_msg_id);
					break;
				case FutureSalts futureSalts:
					SetResult(futureSalts.req_msg_id, futureSalts);
					break;
				case RpcResult rpcResult:
					break; // SetResult was already done in ReadRpcResult
				case MsgsAck msgsAck:
					break; // we don't do anything with these, for now
				case BadMsgNotification badMsgNotification:
					bool retryRpcs = true, retryAll = false;
					long badMsgId = badMsgNotification.bad_msg_id;
					bool aboutCopy = _copyContainers.TryRemove(badMsgId, out var copyOf);
					var logLevel = badMsgNotification.error_code == 48 || aboutCopy ? 2 : 4;
					Helpers.Log(logLevel, $"BadMsgNotification {badMsgNotification.error_code} for {(aboutCopy ? "copy of " : "")}msg #{(short)(aboutCopy ? copyOf.RpcMsgId : badMsgId).GetHashCode():X4}");
					if (badMsgNotification.error_code is 32 or 33)
					{
						// A copy, or the original of a copied request, is never worth a session reset:
						// the other copy normally gets the request answered within a round trip.
						if (aboutCopy)
						{
							HandleRefusedCopy(copyOf.RpcMsgId, badMsgId, badMsgNotification.error_code);
							break;
						}
						Rpc copiedRpc;
						lock (_pendingRpcs)
							_pendingRpcs.TryGetValue(badMsgId, out copiedRpc);
						if (copiedRpc?.copyAttempted == true || _settledHedged.ContainsKey(badMsgId))
						{
							if (copiedRpc != null) // only if the copy does not answer it promptly: ask the server
								_ = CheckIfUnansweredAsync(copiedRpc, 3000, $"BadMsgNotification {badMsgNotification.error_code} on the original");
							break;
						}
					}
					switch (badMsgNotification.error_code)
					{
						case 16: // msg_id too low (most likely, client time is wrong; synchronize it using msg_id notifications and re-send the original message)
						case 17: // msg_id too high (similar to the previous case, the client time has to be synchronized, and the message re-sent with the correct msg_id)
							_dcSession.lastSentMsgId = 0;
							var localTime = DateTime.UtcNow;
							_dcSession.serverTicksOffset = (_lastRecvMsgId >> 32) * 10000000 - localTime.Ticks + 621355968000000000L;
							Helpers.Log(1, $"Time offset: {_dcSession.serverTicksOffset} | Server: {MsgIdToStamp(_lastRecvMsgId).AddTicks(_dcSession.serverTicksOffset).TimeOfDay} UTC | Local: {localTime.TimeOfDay} UTC");
							break;
						case 32: // msg_seqno too low (the server has already received a message with a lower msg_id but with either a higher or an equal and odd seqno)
						case 33: // msg_seqno too high (similarly, there is a message with a higher msg_id but with either a lower or an equal and odd seqno)
							if (_dcSession.seqno <= 1)
							{
								retryRpcs = false;
							}
							else
							{
								await ResetAsync(false, false);
								_dcSession.Renew();
								await ConnectAsync();
								retryAll = true; // new session: every pending msg_id is void
							}
							break;
						case 20: // message too old, and it cannot be verified whether the server has received it: re-send with a new msg_id
							break;
						case 48: // incorrect server salt (in this case, the bad_server_salt response is received with the correct salt, and the message is to be re-sent with it)
							_dcSession.OldSalt = _dcSession.Salt;
							_dcSession.Salt = ((BadServerSalt)badMsgNotification).new_server_salt;
							lock (_session)
								_session.Save();
							CheckSalt();
							break;
						default:
							retryRpcs = false;
							break;
					}
					if (!retryRpcs)
						break;
					var badMsgError = new RpcError { error_code = -503, error_message = $"BadMsgNotification {badMsgNotification.error_code}" };
					if (retryAll)
					{
						Rpc[] retried;
						lock (_pendingRpcs)
						{
							retried = [.. _pendingRpcs.Values];
							foreach (var rpc in retried)
								rpc.tcs.TrySetResult(badMsgError);
							_pendingRpcs.Clear();
						}
						foreach (var rpc in retried)
							SettledTransfer(rpc);
						RaiseUpdates(badMsgNotification);
					}
					else if (aboutCopy)
						HandleRefusedCopy(copyOf.RpcMsgId, badMsgId, badMsgNotification.error_code); // time/salt fixed above
					else
					{
						// Only the refused message (or the container it went out in) is re-sent. The other
						// pending requests were accepted: retrying them with new msg_ids would run them twice.
						List<Rpc> refused = [];
						lock (_pendingRpcs)
							foreach (var rpc in _pendingRpcs.Values)
								if (rpc.msgId == badMsgId || rpc.containerMsgId == badMsgId)
									refused.Add(rpc);
						foreach (var rpc in refused)
							if (badMsgNotification.error_code == 48)
								_ = ResendRefusedAsync(rpc, badMsgError); // msg_id is fine, only the salt was wrong
							else if (badMsgNotification.error_code == 20 && rpc.copyAttempted)
								_ = CheckStateOrRetryAsync(rpc, badMsgError.error_message); // "cannot verify": a copy may have run
							else
								FailPending(rpc, badMsgError, badMsgError.error_message); // msg_id itself refused: Invoke retries with a new one
						RaiseUpdates(badMsgNotification);
					}
					break;
				default:
					RaiseUpdates(obj);
					break;
			}

			void SetResult(long msgId, object result)
			{
				var rpc = PullPendingRequest(msgId);
				if (rpc != null)
					rpc.tcs.TrySetResult(result);
				else
					RaiseUpdates(obj);
			}
		}

		private async void RaiseUpdates(IObject obj)
		{
			try
			{
				var task = obj is UpdatesBase updates ? OnUpdates?.Invoke(updates) : OnOther?.Invoke(obj);
				if (task != null)
				await task;
			}
			catch (Exception ex)
			{
				Helpers.Log(4, $"{nameof(OnUpdates)}({obj?.GetType().Name}) raised {ex}");
			}
		}

		private void CheckRaiseOwnUpdates(object result)
		{
			if (OnOwnUpdates == null)
				return;
			if (result is UpdatesBase updates)
				RaiseOwnUpdates(updates);
			else if (result is Payments_PaymentResult ppr)
				RaiseOwnUpdates(ppr.updates);
			else if (result is Messages_InvitedUsers miu)
				RaiseOwnUpdates(miu.updates);
		}

		private async void RaiseOwnUpdates(UpdatesBase updates)
		{
			try
			{
				await OnOwnUpdates?.Invoke(updates);
			}
			catch (Exception ex)
			{
				Helpers.Log(4, $"{nameof(OnOwnUpdates)}({updates.GetType().Name}) raised {ex}");
			}
		}

		static async Task<TcpClient> DefaultTcpHandler(string host, int port, IPEndPoint localEndPoint = null)
		{
			var tcpClient = localEndPoint != null ? new TcpClient(localEndPoint) : new TcpClient();
			await tcpClient.ConnectAsync(host, port);
			return tcpClient;
		}

		private IPEndPoint GetDefaultEndpoint(out int dcId)
		{
			string addr = Config("server_address");
			dcId = addr.Length > 2 && addr[1] == '>' && addr[0] is > '0' and <= '9' ? addr[0] - '0' : 0;
			return Compat.IPEndPoint_Parse(dcId == 0 ? addr : addr[2..]);
		}

		/// <summary>Establish connection to Telegram servers without negociating a user session</summary>
		/// <param name="quickResume">Attempt to resume session immediately without issuing Layer/InitConnection/GetConfig <i>(not recommended by default)</i></param>
		/// <remarks>Usually you shouldn't need to call this method: Use <see cref="LoginUserIfNeeded">LoginUserIfNeeded</see> instead. <br/>Config callback is queried for: <b>server_address</b></remarks>
		/// <returns>Most methods of this class are async (Task), so please use <see langword="await"/></returns>
		public async Task ConnectAsync(bool quickResume = false)
		{
			Task task;
			lock (this)
				task = _connecting ??= DoConnectAsync(quickResume);
			try
			{
				await task;
			}
			catch
			{
				// Clear cached failure so subsequent calls retry instead of replaying the same error
				lock (this)
					if (_connecting == task)
						_connecting = null;
				throw;
			}
		}

		private IEnumerable<DcOption> GetDcOptions(int dcId, DcOption.Flags flags) => !flags.HasFlag(DcOption.Flags.media_only)
			? _session.DcOptions.Where(dc => dc.id == dcId && (dc.flags & (DcOption.Flags.cdn | DcOption.Flags.tcpo_only | DcOption.Flags.media_only)) == 0)
				.OrderBy(dc => dc.flags ^ flags)
			: _session.DcOptions.Where(dc => dc.id == dcId && (dc.flags & (DcOption.Flags.cdn | DcOption.Flags.tcpo_only)) == 0)
				.OrderBy(dc => ~dc.flags & DcOption.Flags.media_only).ThenBy(dc => dc.flags ^ flags)
				.Select(dc => dc.flags.HasFlag(DcOption.Flags.media_only) ? dc : new DcOption { id = dc.id, port = dc.port,
					ip_address = dc.ip_address, secret = dc.secret, flags = dc.flags | DcOption.Flags.media_only });

		private async Task DoConnectAsync(bool quickResume)
		{
			_cts = new();
			IPEndPoint endpoint = null;
			bool needMigrate = false;
			byte[] preamble, secret = null;
			int dcId = _dcSession?.DcID ?? 0;
			if (dcId == 0)
				dcId = 2;
			bool usingPaths = false;
			var localEPs = LocalEndPoints?.Count > 0 ? LocalEndPoints : null;
			int primaryEPIndex = 0;
			if (MTProxyUrl != null)
			{
#if OBFUSCATION
				if (TLConfig?.test_mode == true)
					dcId += dcId < 0 ? -10000 : 10000;
				var parms = HttpUtility.ParseQueryString(MTProxyUrl[MTProxyUrl.IndexOf('?')..]);
				var server = parms["server"];
				int port = int.Parse(parms["port"]);
				var str = parms["secret"]; // can be hex or base64
				var secretBytes = secret = str.All("0123456789ABCDEFabcdef".Contains) ? Convert.FromHexString(str) :
					System.Convert.FromBase64String(str.Replace('_', '/').Replace('-', '+') + new string('=', (2147483644 - str.Length) % 4));
				var tlsMode = secret.Length >= 21 && secret[0] == 0xEE;
				if (tlsMode || (secret.Length == 17 && secret[0] == 0xDD))
				{
					_paddedMode = true;
					secret = secret[1..17];
				}
				else if (secret.Length != 16)
					throw new ArgumentException("Invalid/unsupported secret");
				Helpers.Log(2, $"Connecting to DC {dcId} via MTProxy {server}:{port}...");
				_tcpClient = await TcpHandler(server, port);
				_networkStream = _tcpClient.GetStream();
				if (tlsMode)
					_networkStream = await TlsStream.HandshakeAsync(_networkStream, secret, secretBytes[17..], _cts.Token);
#else
				throw new Exception("Library was not compiled with OBFUSCATION symbol");
#endif
			}
			else if (_httpClient != null)
			{
				Helpers.Log(2, $"Using HTTP Mode");
				_reactorTask = Task.CompletedTask;
			}
			else
			{
				endpoint = _dcSession?.EndPoint ?? GetDefaultEndpoint(out int defaultDc);

				Helpers.Log(2, $"Connecting to {endpoint}{(localEPs != null ? $" (multipath, {localEPs.Count} endpoints)" : "")}...");
				TcpClient tcpClient = null;
				try
				{
					try
					{
						if (localEPs != null && localEPs.Count > 1 && SendMode == PathSendMode.PreferredOrder)
						{
							// PreferredOrder: connect sequentially so address[0] is always Path 0.
							// This ensures the preferred path matches the first address in the config.
							for (int i = 0; i < localEPs.Count; i++)
							{
								try
								{
									tcpClient = await TcpHandler(endpoint.Address.ToString(), endpoint.Port, localEPs[i])
										.WaitAsync(TimeSpan.FromSeconds(PathConnectTimeout));
									primaryEPIndex = i;
									_lastConnectedEPIndex = i;
									Helpers.Log(2, $"Primary connected via {localEPs[i].Address}{(i == 0 ? " (preferred)" : " (fallback)")}.");
									break;
								}
								catch (Exception ex)
								{
									Helpers.Log(3, $"Connect via {localEPs[i].Address} failed: {ex.Message}");
								}
							}
							if (tcpClient == null)
							throw new SocketException(10060); // all endpoints failed
						}
						else if (localEPs != null && localEPs.Count > 1)
						{
							// RoundRobin/StickyFailover: race ALL endpoints in parallel — first one wins.
							// This avoids wasting 10s per dead interface in sequential attempts.
							var connectTasks = new List<(Task<TcpClient> task, int epIdx)>();
							for (int i = 0; i < localEPs.Count; i++)
							{
								int idx = i;
								connectTasks.Add((TcpHandler(endpoint.Address.ToString(), endpoint.Port, localEPs[idx])
									.WaitAsync(TimeSpan.FromSeconds(PathConnectTimeout)), idx));
							}

							Exception lastEx = null;
							while (connectTasks.Count > 0 && tcpClient == null)
							{
								var completedTask = await Task.WhenAny(connectTasks.Select(x => x.task));
								var entry = connectTasks.First(x => x.task == completedTask);
								connectTasks.Remove(entry);
								try
								{
									tcpClient = await entry.task;
									primaryEPIndex = entry.epIdx;
									_lastConnectedEPIndex = entry.epIdx;
									Helpers.Log(2, $"Primary connected via {localEPs[entry.epIdx].Address}.");
								}
								catch (Exception ex)
								{
									Helpers.Log(3, $"Connect via {localEPs[entry.epIdx].Address} failed: {ex.Message}");
									lastEx = ex;
								}
							}

							// Dispose any late-arriving connections we don't need
							foreach (var remaining in connectTasks)
								_ = remaining.task.ContinueWith(t => { if (t.IsCompletedSuccessfully) t.Result?.Dispose(); }, TaskScheduler.Default);

							if (tcpClient == null && lastEx != null)
							throw lastEx;
						}
						else
						{
							tcpClient = await TcpHandler(endpoint.Address.ToString(), endpoint.Port, localEPs?[0]);
						}
					}
					catch (SocketException ex) // cannot connect to target endpoint, try to find an alternate
					{
						Helpers.Log(4, $"SocketException {ex.SocketErrorCode} ({ex.ErrorCode}): {ex.Message}");
						if (_dcSession?.DataCenter == null)
						throw;
						var triedEndpoints = new HashSet<IPEndPoint> { endpoint };
						if (_session.DcOptions != null)
						{
							var altOptions = GetDcOptions(_dcSession.DataCenter.id, _dcSession.DataCenter.flags);
							// try alternate addresses for this DC
							foreach (var dcOption in altOptions)
							{
								endpoint = new(IPAddress.Parse(dcOption.ip_address), dcOption.port);
								if (!triedEndpoints.Add(endpoint))
							continue;
								Helpers.Log(2, $"Connecting to {endpoint}...");
								try
								{
									tcpClient = await TcpHandler(endpoint.Address.ToString(), endpoint.Port, localEPs?[primaryEPIndex]);
									_dcSession.DataCenter = dcOption;
									break;
								}
								catch (SocketException) { }
							}
						}
						if (tcpClient == null)
						{
							endpoint = GetDefaultEndpoint(out defaultDc); // re-ask callback for an address
							if (!triedEndpoints.Add(endpoint))
						throw;
							needMigrate = _dcSession.DataCenter.id == _session.MainDC && defaultDc != _session.MainDC;
							_dcSession.Client = null;
							// is it address for a known DCSession?
							_dcSession = _session.DCSessions.Values.FirstOrDefault(dcs => dcs.EndPoint.Equals(endpoint));
							if (defaultDc != 0)
						_dcSession ??= _session.DCSessions.GetValueOrDefault(defaultDc);
							_dcSession ??= new();
							_dcSession.Client = this;
							_dcSession.DataCenter = null;
							Helpers.Log(2, $"Connecting to {endpoint}...");
							tcpClient = await TcpHandler(endpoint.Address.ToString(), endpoint.Port, localEPs?[primaryEPIndex]);
						}
					}
				}
				catch
				{
					tcpClient?.Dispose();
					throw;
				}

				// Create primary path from the first connection
				usingPaths = true;
				ConfigureKeepalive(tcpClient);
				var primaryPath = new TransportPath
				{
					TcpClient = tcpClient,
					NetworkStream = tcpClient.GetStream(),
					PaddedMode = _paddedMode,
					LocalEndPoint = localEPs?[primaryEPIndex],
					PathIndex = 0,
					Cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token),
					Generation = Interlocked.Increment(ref _pathGeneration)
				};
				byte protocolId = (byte)(primaryPath.PaddedMode ? 0xDD : 0xEE);
#if OBFUSCATION
				(primaryPath.SendCtr, primaryPath.RecvCtr, preamble) = InitObfuscation(secret, protocolId, dcId);
#else
				preamble = new byte[] { protocolId, protocolId, protocolId, protocolId };
#endif
				await primaryPath.NetworkStream.WriteAsync(preamble, 0, preamble.Length, _cts.Token);
				primaryPath.ReactorTask = Reactor(primaryPath, primaryPath.NetworkStream, primaryPath.Cts.Token);
				primaryPath.IsAlive = true;
				primaryPath.ConnectedSinceTicks = Environment.TickCount64;
				primaryPath.LastRecvTicks = primaryPath.ConnectedSinceTicks;
				lock (_pathsLock)
					_paths.Add(primaryPath);
				Helpers.Log(2, $"{dcId}>Path 0 connected{(localEPs != null ? $" from {localEPs[primaryEPIndex].Address}" : "")}.");
				RaisePathChanged(primaryPath);
			}

			_dcSession.Salts?.Remove(DateTime.MaxValue);
			if (!usingPaths && _networkStream != null)
			{
				// Legacy single-connection mode (MTProxy)
				byte protocolId = (byte)(_paddedMode ? 0xDD : 0xEE);
#if OBFUSCATION
				(_sendCtr, _recvCtr, preamble) = InitObfuscation(secret, protocolId, dcId);
#else
				preamble = new byte[] { protocolId, protocolId, protocolId, protocolId };
#endif
				await _networkStream.WriteAsync(preamble, 0, preamble.Length, _cts.Token);

				_reactorTask = Reactor(_networkStream, _cts.Token);
			}
			_sendSemaphore.Release();

			// Start connecting secondary paths in the background BEFORE auth/InitConnection.
			// Secondary connections only need TCP + obfuscation + registration ping — they share
			// the session and don't need their own auth. This runs concurrently with auth so the
			// bot starts operating on the first available path immediately.
			if (usingPaths && localEPs?.Count > 1)
			{
				_secondaryPathsTask = ConnectSecondaryPathsAsync(endpoint, localEPs, primaryEPIndex, dcId);
			}

			try
			{
				if (_dcSession.authKeyID == 0)
					await CreateAuthorizationKey(this, _dcSession);

				bool hasConnection = _networkStream != null || _paths.Count > 0;
				if (hasConnection)
					_ = KeepAlive(_cts.Token);
				if (quickResume && _dcSession.Layer == Layer.Version && _dcSession.DataCenter != null && _session.MainDC != 0)
				{
					TLConfig = new Config { this_dc = _session.MainDC, dc_options = _session.DcOptions };
				}
				else
				{
					if (_dcSession.Layer != 0 && _dcSession.Layer != Layer.Version)
						_dcSession.Renew();
					await InitConnection();
					if (_dcSession.DataCenter == null)
					{
						_dcSession.DataCenter = _session.DcOptions.Where(dc => dc.id == TLConfig.this_dc)
							.OrderByDescending(dc => dc.ip_address == endpoint?.Address.ToString())
							.ThenByDescending(dc => dc.port == endpoint?.Port)
							.ThenByDescending(dc => dc.flags == (endpoint?.AddressFamily == AddressFamily.InterNetworkV6 ? DcOption.Flags.ipv6 : 0))
							.First();
						_session.DCSessions[TLConfig.this_dc] = _dcSession;
					}
					if (_session.MainDC == 0)
						_session.MainDC = TLConfig.this_dc;
					else if (needMigrate)
						await MigrateToDC(_session.MainDC);
				}
			}
			catch
			{
				// Auth/InitConnection failed — clean up the secondary paths task reference.
				// The secondary Reactors will self-terminate when _cts is cancelled by the
				// next ResetAsync call; we just prevent stale task reference reuse.
				_secondaryPathsTask = null;
				throw;
			}
			finally
			{
				if (_reactorTask != null || _paths.Count > 0) // client not disposed
					lock (_session)
						_session.Save();
			}
			Helpers.Log(2, $"Connected to {(TLConfig.test_mode ? "Test DC" : "DC")} {TLConfig.this_dc}... {TLConfig.flags & (Config.Flags)~0x18E00U}");

			// Secondary path connection was already started in the background (see below).
			// Wait for it to complete now that auth is done, so the caller knows all paths
			// are established (or at least attempted).
			if (_secondaryPathsTask != null)
			{
				await _secondaryPathsTask;
				_secondaryPathsTask = null;
			}

			// Start health/stall monitors now that all paths have been attempted
			if (usingPaths && localEPs?.Count > 1)
			{
				if (_parentClient == null)
				{
					// Main client: full health monitor with liveness probing, stall detection, penalty decay.
					_ = PathHealthMonitor(_cts.Token);
				}
				else
				{
					// Child client (media DC): lightweight stall watcher only.
					// No liveness probing (Telegram delays Pong while processing file parts),
					// but we DO need stall detection because file transfers are the primary
					// use case and stalls are the most impactful failure mode.
					_ = ChildStallWatcher(_cts.Token);
				}
			}
		}

		/// <summary>Connect secondary paths (all local endpoints except the primary).
		/// Runs in the background during DoConnectAsync so auth/InitConnection proceeds in parallel.</summary>
		private async Task ConnectSecondaryPathsAsync(IPEndPoint endpoint, List<IPEndPoint> localEPs, int primaryEPIndex, int dcId)
		{
			byte[] preamble;
			int pathIdx = 1;
			for (int i = 0; i < localEPs.Count; i++)
			{
				if (i == primaryEPIndex)
					continue;
				var localEP = localEPs[i];
				try
				{
					var tcpClient2 = await TcpHandler(endpoint.Address.ToString(), endpoint.Port, localEP).WaitAsync(TimeSpan.FromSeconds(PathConnectTimeout));
					ConfigureKeepalive(tcpClient2);
					var path = new TransportPath
					{
						TcpClient = tcpClient2,
						NetworkStream = tcpClient2.GetStream(),
						PaddedMode = _paddedMode,
						LocalEndPoint = localEP,
						PathIndex = pathIdx,
						Cts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token),
						Generation = Interlocked.Increment(ref _pathGeneration)
					};
					byte protocolId2 = (byte)(path.PaddedMode ? 0xDD : 0xEE);
#if OBFUSCATION
					(path.SendCtr, path.RecvCtr, preamble) = InitObfuscation(null, protocolId2, dcId);
#else
					preamble = new byte[] { protocolId2, protocolId2, protocolId2, protocolId2 };
#endif
					await path.NetworkStream.WriteAsync(preamble, 0, preamble.Length, _cts.Token);
					path.ReactorTask = Reactor(path, path.NetworkStream, path.Cts.Token);
					path.LastRecvTicks = Environment.TickCount64;
					lock (_pathsLock)
						_paths.Add(path); // not alive yet: nothing is sent on it before Telegram answers its registration ping

					_ = RegisterSecondaryPathAsync(path, dcId); // in the background: connecting never waits for it
				}
				catch (Exception ex)
				{
					Helpers.Log(4, $"{dcId}>Failed to connect path {pathIdx} from {localEP.Address}: {ex.Message}. Will retry in background.");
					var deadPath = new TransportPath
					{
						PaddedMode = _paddedMode,
						LocalEndPoint = localEP,
						PathIndex = pathIdx,
						IsAlive = false,
						LastRecvTicks = 0,
						LastProbeTicks = 0
					};
					lock (_pathsLock) _paths.Add(deadPath);
					_ = ReconnectPathAsync(deadPath);
				}
				pathIdx++;
			}
			Helpers.Log(2, $"{dcId}>Multipath transport: {_paths.Count(p => p.IsAlive)}/{_paths.Count} paths alive (the rest are registering or retrying in the background).");
		}

		/// <summary>Marks a freshly connected secondary path alive once Telegram answers its registration ping;
		/// otherwise hands it to the reconnect loop.</summary>
		private async Task RegisterSecondaryPathAsync(TransportPath path, int dcId)
		{
			bool registered = false;
			try
			{
				// first-ever login: this runs while the auth key is still being created; a ping needs it
				for (int i = 0; i < 300 && _dcSession.authKeyID == 0 && _cts?.IsCancellationRequested == false; i++)
					await Task.Delay(100);
				registered = await RegisterPathAsync(path);
			}
			catch (Exception ex)
			{
				Helpers.Log(3, $"{dcId}>Path {path.PathIndex} registration ping failed: {ex.Message}");
			}
			if (registered)
			{
				lock (_pathsLock)
				{
					if (!_paths.Contains(path))
						return;
					path.IsAlive = true;
				}
				path.ConnectedSinceTicks = Environment.TickCount64;
				Helpers.Log(2, $"{dcId}>Path {path.PathIndex} connected from {path.LocalEndPoint?.Address}.");
				RaisePathChanged(path);
			}
			else
			{
				Helpers.Log(3, $"{dcId}>Path {path.PathIndex} from {path.LocalEndPoint?.Address}: registration ping unanswered. Will retry in background.");
				_ = ReconnectPathAsync(path);
			}
		}

		private async Task InitConnection()
		{
			var initParams = JSONValue.FromJsonElement(System.Text.Json.JsonDocument.Parse(Config("init_params")).RootElement);
			TLConfig = await this.InvokeWithLayer(Layer.Version,
				new TL.Methods.InitConnection<Config>
				{
					flags = TL.Methods.InitConnection<Config>.Flags.has_params,
					api_id = _session.ApiId,
					device_model = Config("device_model"),
					system_version = Config("system_version"),
					app_version = Config("app_version"),
					system_lang_code = Config("system_lang_code"),
					lang_pack = Config("lang_pack"),
					lang_code = Config("lang_code"),
					params_ = initParams,
					query = new TL.Methods.Help_GetConfig()
				});
			_dcSession.Layer = Layer.Version;
			_session.DcOptions = TLConfig.dc_options;
		}

		private async Task KeepAlive(CancellationToken ct)
		{
			int ping_id = _random.Next();
			while (!ct.IsCancellationRequested)
			{
				await Task.Delay(Math.Abs(PingInterval) * 1000, ct);

				// On the main client, multi-path keepalive is handled by PathHealthMonitor
				// (PingDelayDisconnect every 3s per path). Child clients (media DCs) have
				// no health monitor, so they still need KeepAlive pings for liveness.
				if (_paths.Count > 1 && _parentClient == null)
					continue;

				if (PingInterval <= 0)
					await this.Ping(ping_id++);
				else // see https://core.telegram.org/api/optimisation#grouping-updates
#if DEBUG
					await this.PingDelayDisconnect(ping_id++, PingInterval * 5);
#else
					await this.PingDelayDisconnect(ping_id++, PingInterval * 5 / 4);
#endif
			}
		}

		/// <summary>Login as a user with given phone number (or resume previous session)<br/>Call this method again to provide additional requested login information</summary>
		/// <param name="loginInfo">First call should be with phone number<br/>Further calls should be with the requested configuration value</param>
		/// <returns>Configuration item requested to continue login, or <see langword="null"/> when login is successful<br/>
		/// Possible values: <b>verification_code</b>, <b>name</b> (signup), <b>password</b> (2FA), <b>email</b> &amp; <b>email_verification_code</b> (email registration)</returns>
		/// <exception cref="WTException"/><exception cref="RpcException"/>
		public async Task<string> Login(string loginInfo)
		{
			if (_loginCfg.request == default)
			{
				RunLoginAsync(loginInfo);
			}
			else
			{
				if (await _loginCfg.request.Task == null)
					return null;
				loginInfo ??= AskConfig(await _loginCfg.request.Task);
				_loginCfg.request = new();
				_loginCfg.response.SetResult(loginInfo);
			}
			return await _loginCfg.request.Task;
		}
		private (TaskCompletionSource<string> request, TaskCompletionSource<string> response) _loginCfg;
		private async void RunLoginAsync(string phone)
		{
			_loginCfg.request = new();
			var prevConfig = _config;
			_config = what =>
			{
				if (prevConfig(what) is string value)
					return value;
				switch (what)
				{
					case "phone_number": return phone;
					case "last_name": break;
					case "first_name": what = "name"; goto case "email";
					case "email": case "email_verification_code":
					case "password": case "verification_code": _loginCfg.response = new(); _loginCfg.request.SetResult(what); break;
					default: return null;
				};
				value = _loginCfg.response.Task.Result;
				if (what == "name" && value != null)
				{
					var lf = value.IndexOf('\n');
					if (lf < 0)
					lf = value.IndexOf(' ');
					_loginCfg.response = new();
					_loginCfg.response.SetResult(lf < 0 ? "" : value[(lf + 1)..]);
					value = lf < 0 ? value : value[0..lf];
					return value;
				}
				return value;
			};
			try
			{
				await ConnectAsync(); // start reactor on the current (UI?) context
				// Login logic is executed on TaskScheduler while request TCS are still received on current SynchronizationContext
				await Task.Run(() => LoginUserIfNeeded());
				_loginCfg.request.SetResult(null);
			}
			catch (Exception ex)
			{
				_loginCfg.request.SetException(ex);
			}
			finally
			{
				_config = prevConfig;
			}
		}

		/// <summary>Login as a bot (if not already logged-in).</summary>
		/// <param name="bot_token">bot token, or <see langword="null"/> if token is provided by Config callback</param>
		/// <remarks>Config callback may be queried for: <b>bot_token</b>
		/// <br/>Bots can only call API methods marked with [bots: ✓] in their documentation. </remarks>
		/// <returns>Detail about the logged-in bot</returns>
		public async Task<User> LoginBotIfNeeded(string bot_token = null)
		{
			await ConnectAsync();
			string botToken = bot_token ?? Config("bot_token");
			if (_session.UserId != 0) // a user is already logged-in
			{
				try
				{
					var users = await this.Users_GetUsers(InputUser.Self); // this call also reenable incoming Updates
					var self = users[0] as User;
					if (self.id == long.Parse(botToken.Split(':')[0]))
					{
						_session.UserId = _dcSession.UserId = self.id;
						lock (_session) _session.Save();
						RaiseUpdates(self);
						return User = self;
					}
					Helpers.Log(3, $"Current logged user {self.id} mismatched bot_token. Logging out and in...");
				}
				catch (Exception ex)
				{
					Helpers.Log(4, $"Error while verifying current bot! ({ex.Message}) Proceeding to login...");
				}
				await this.Auth_LogOut();
				_session.UserId = _dcSession.UserId = 0;
				User = null;
			}
			var authorization = await this.Auth_ImportBotAuthorization(0, _session.ApiId, _apiHash ??= Config("api_hash"), botToken);
			return LoginAlreadyDone(authorization);
		}

		/// <summary>Login as a user (if not already logged-in).
		/// <br/><i>(this method calls <see cref="ConnectAsync">ConnectAsync</see> if necessary)</i></summary>
		/// <remarks>Config callback is queried for: <b>phone_number</b>, <b>verification_code</b> <br/>and eventually <b>first_name</b>, <b>last_name</b> (signup required), <b>password</b> (2FA auth), <b>email</b> &amp; <b>email_verification_code</b> (email registration), <b>user_id</b> (alt validation)</remarks>
		/// <param name="settings">(optional) Preference for verification_code sending</param>
		/// <param name="reloginOnFailedResume">Proceed to logout and login if active user session cannot be resumed successfully</param>
		/// <returns>Detail about the logged-in user
		/// <br/>Most methods of this class are async (Task), so please use <see langword="await"/> to get the result</returns>
		public async Task<User> LoginUserIfNeeded(CodeSettings settings = null, bool reloginOnFailedResume = true)
		{
			await ConnectAsync();
			string phone_number = null;
			if (_session.UserId != 0) // a user is already logged-in
			{
				try
				{
					var users = await this.Users_GetUsers(InputUser.Self); // this call also reenable incoming Updates
					var self = users[0] as User;
					// check user_id or phone_number match currently logged-in user
					if ((long.TryParse(_config("user_id"), out long id) && (id == -1 || self.id == id)) ||
						self.phone == string.Concat((phone_number = Config("phone_number")).Where(char.IsDigit)))
					{
						_session.UserId = _dcSession.UserId = self.id;
						lock (_session) _session.Save();
						RaiseUpdates(self);
						return User = self;
					}
					var mismatch = $"Current logged user {self.id} mismatched user_id or phone_number";
					Helpers.Log(3, mismatch);
					if (!reloginOnFailedResume)
					throw new WTException(mismatch);
				}
				catch (RpcException ex) when (reloginOnFailedResume)
				{
					Helpers.Log(4, $"Error while fetching current user! ({ex.Message})");
				}
				Helpers.Log(3, $"Proceeding to logout and login...");
				await this.Auth_LogOut();
				_session.UserId = _dcSession.UserId = 0;
				User = null;
			}
			phone_number ??= Config("phone_number");
			Auth_SentCodeBase sentCodeBase;
#pragma warning disable CS0618 // Auth_* methods are marked as obsolete
			try
			{
				sentCodeBase = await this.Auth_SendCode(phone_number, _session.ApiId, _apiHash ??= Config("api_hash"), settings ??= new());
			}
			catch (RpcException ex) when (ex.Code == 500 && ex.Message == "AUTH_RESTART")
			{
				sentCodeBase = await this.Auth_SendCode(phone_number, _session.ApiId, _apiHash, settings);
			}
			Auth_AuthorizationBase authorization = null;
			string phone_code_hash = null, email = null;
			try
			{
				if (sentCodeBase is Auth_SentCode { type: Auth_SentCodeTypeSetUpEmailRequired setupEmail } setupSentCode)
				{
					phone_code_hash = setupSentCode.phone_code_hash;
					Helpers.Log(3, "A login email is required");
					RaiseUpdates(sentCodeBase);
					email = _config("email");
					if (string.IsNullOrEmpty(email))
					{
						sentCodeBase = await this.Auth_ResendCode(phone_number, phone_code_hash);
					}
					else
					{
						var purpose = new EmailVerifyPurposeLoginSetup { phone_number = phone_number, phone_code_hash = phone_code_hash };
						if (email is not "Google" and not "Apple")
						{
							var sentEmail = await this.Account_SendVerifyEmailCode(purpose, email);
							Helpers.Log(3, "An email verification code has been sent to " + sentEmail.email_pattern);
							RaiseUpdates(sentEmail);
						}
						Account_EmailVerified verified = null;
						for (int retry = 1; verified == null; retry++)
							try
							{
								var code = await ConfigAsync("email_verification_code");
								verified = await this.Account_VerifyEmail(purpose, EmailVerification(email, code));
							}
							catch (RpcException e) when (e.Code == 400 && e.Message is "CODE_INVALID" or "EMAIL_TOKEN_INVALID")
							{
								Helpers.Log(4, "Wrong email verification code!");
								if (retry >= MaxCodePwdAttempts)
									throw;
							}
						if (verified is Account_EmailVerifiedLogin verifiedLogin) // (it should always be)
							sentCodeBase = verifiedLogin.sent_code;
					}
					RaiseUpdates(sentCodeBase);
				}
			resent:
				if (sentCodeBase is Auth_SentCodeSuccess success)
				{
					authorization = success.authorization;
				}
				else if (sentCodeBase is Auth_SentCodePaymentRequired paymentRequired)
				{
					throw new WTException("Auth_SentCodePaymentRequired unsupported");
				}
				else if (sentCodeBase is Auth_SentCode sentCode)
				{
					phone_code_hash = sentCode.phone_code_hash;
					var timeout = DateTime.UtcNow + TimeSpan.FromSeconds(sentCode.timeout);
					Helpers.Log(3, $"A verification code has been sent via {sentCode.type.GetType().Name[17..]}");
					RaiseUpdates(sentCode);
					if (sentCode.type is Auth_SentCodeTypeFirebaseSms firebaseSms)
					{
						var token = await ConfigAsync("firebase");
						int index = token?.IndexOf(':') ?? -1;
						if (!(index > 0 && token[..index] switch
						{
							"safety_net_token" => await this.Auth_RequestFirebaseSms(phone_number, phone_code_hash, safety_net_token: token[(index + 1)..]),
							"ios_push_secret" => await this.Auth_RequestFirebaseSms(phone_number, phone_code_hash, ios_push_secret: token[(index + 1)..]),
							_ => false
						}))
						{
							sentCodeBase = await this.Auth_ResendCode(phone_number, phone_code_hash);
							goto resent;
						}
					}
					for (int retry = 1; authorization == null; retry++)
						try
						{
							var verification_code = await ConfigAsync("verification_code");
							if (verification_code == "" && sentCode.next_type != 0)
							{
								var mustWait = timeout - DateTime.UtcNow;
								if (mustWait.Ticks > 0)
								{
									Helpers.Log(3, $"You must wait {(int)(mustWait.TotalSeconds + 0.5)} more seconds before requesting the code to be sent via {sentCode.next_type}");
									continue;
								}
								sentCodeBase = await this.Auth_ResendCode(phone_number, phone_code_hash);
								goto resent;
							}
							if (sentCode.type is Auth_SentCodeTypeEmailCode)
								authorization = await this.Auth_SignIn(phone_number, phone_code_hash, null, EmailVerification(email ??= _config("email"), verification_code));
							else
								authorization = await this.Auth_SignIn(phone_number, phone_code_hash, verification_code);
						}
						catch (RpcException e) when (e.Code == 400 && e.Message == "PHONE_CODE_INVALID")
						{
							Helpers.Log(4, "Wrong verification code!");
							if (retry >= MaxCodePwdAttempts)
								throw;
						}
						catch (RpcException e) when (e.Code == 401 && e.Message == "SESSION_PASSWORD_NEEDED")
						{
							authorization = await LoginPasswordNeeded();
						}
				}

				static EmailVerification EmailVerification(string email, string code) => email switch
				{
					"Google" => new EmailVerificationGoogle { token = code },
					"Apple" => new EmailVerificationApple { token = code },
					_ => new EmailVerificationCode { code = code }
				};
			}
			catch (Exception ex) when (ex is not RpcException { Message: "FLOOD_WAIT_X" })
			{
				try
			{
				await this.Auth_CancelCode(phone_number, phone_code_hash);
			}
			catch { }
				throw;
			}
			if (authorization is Auth_AuthorizationSignUpRequired signUpRequired)
			{
				var waitUntil = DateTime.UtcNow.AddSeconds(3);
				RaiseUpdates(signUpRequired); // give caller the possibility to read and accept TOS
				var first_name = Config("first_name");
				var last_name = Config("last_name");
				var wait = waitUntil - DateTime.UtcNow;
				if (wait > TimeSpan.Zero)
				await Task.Delay(wait); // we get a FLOOD_WAIT_3 if we SignUp too fast
				authorization = await this.Auth_SignUp(phone_number, phone_code_hash, first_name, last_name);
			}
#pragma warning restore CS0618
			LoginAlreadyDone(authorization);
			if (User.phone != string.Concat(phone_number.Where(char.IsDigit)))
				Helpers.Log(3, $"Mismatched phone_number (+{User.phone} != {phone_number}). Fix this to avoid verification code each time");
			return User;
		}

		/// <summary>Login via QR code</summary>
		/// <param name="qrDisplay">Callback to display the login url as QR code to the user (<a href="https://www.nuget.org/packages/QRCoder/">QRCoder library</a> can help you)</param>
		/// <param name="except_ids">(optional) To prevent logging as these user ids</param>
		/// <param name="logoutFirst">If session is already connected to a user, this method will first log out.<br/>You can also check property <see cref="UserId"/> before calling this method.</param>
		/// <param name="ct">If you need to abort the method before login is completed</param>
		/// <returns>Detail about the logged-in user</returns>
		public async Task<User> LoginWithQRCode(Action<string> qrDisplay, long[] except_ids = null, bool logoutFirst = true, CancellationToken ct = default)
		{
			await ConnectAsync();
			if (logoutFirst && _session.UserId != 0) // a user is already logged-in
			{
				await this.Auth_LogOut();
				_session.UserId = _dcSession.UserId = 0;
				User = null;
			}
			var tcs = new TaskCompletionSource<bool>();
			OnUpdates += CatchQRUpdate;
			try
			{
				while (!ct.IsCancellationRequested)
				{
					var ltb = await this.Auth_ExportLoginToken(_session.ApiId, _apiHash ??= Config("api_hash"), except_ids);
				retry:
					switch (ltb)
					{
						case Auth_LoginToken lt:
							var url = "tg://login?token=" + System.Convert.ToBase64String(lt.token).Replace('/', '_').Replace('+', '-');
							Helpers.Log(3, $"Waiting for this QR code login to be accepted: " + url);
							qrDisplay(url);
							if (lt.expires - DateTime.UtcNow is { Ticks: >= 0 } delay)
								await Task.WhenAny(Task.Delay(delay, ct), tcs.Task);
							break;
						case Auth_LoginTokenMigrateTo ltmt:
							await MigrateToDC(ltmt.dc_id);
							ltb = await this.Auth_ImportLoginToken(ltmt.token);
							goto retry;
						case Auth_LoginTokenSuccess lts:
							return LoginAlreadyDone(lts.authorization);
					}
				}
				ct.ThrowIfCancellationRequested();
				return null;
			}
			catch (RpcException e) when (e.Code == 401 && e.Message == "SESSION_PASSWORD_NEEDED")
			{
				return LoginAlreadyDone(await LoginPasswordNeeded());
			}
			finally
			{
				OnUpdates -= CatchQRUpdate;
			}

			Task CatchQRUpdate(UpdatesBase updates)
			{
				if (updates.UpdateList.OfType<UpdateLoginToken>().Any())
					tcs.TrySetResult(true);
				return Task.CompletedTask;
			}
		}

		private async Task<Auth_AuthorizationBase> LoginPasswordNeeded()
		{
			for (int pwdRetry = 1; ; pwdRetry++)
				try
				{
					var accountPassword = await this.Account_GetPassword();
					RaiseUpdates(accountPassword);
					var checkPasswordSRP = await Check2FA(accountPassword, () => ConfigAsync("password"));
					return await this.Auth_CheckPassword(checkPasswordSRP);
				}
				catch (RpcException pe) when (pe.Code == 400 && pe.Message == "PASSWORD_HASH_INVALID")
				{
					Helpers.Log(4, "Wrong password!");
					if (pwdRetry >= MaxCodePwdAttempts)
					throw;
				}
		}

		/// <summary><b>[Not recommended]</b> You can use this if you have already obtained a login authorization manually</summary>
		/// <param name="authorization">if this was not a successful Auth_Authorization, an exception is thrown</param>
		/// <returns>the User that was authorized</returns>
		/// <remarks>This approach is not recommended because you likely didn't properly handle all aspects of the login process
		/// <br/>(transient failures, unnecessary login, 2FA, sign-up required, slowness to respond, verification code resending, encryption key safety, etc..)
		/// <br/>Methods <c>LoginUserIfNeeded</c> and <c>LoginBotIfNeeded</c> handle these automatically for you</remarks>
		[EditorBrowsable(EditorBrowsableState.Never)]
		public User LoginAlreadyDone(Auth_AuthorizationBase authorization)
		{
			if (authorization is not Auth_Authorization { user: User self })
				throw new WTException("Failed to get Authorization: " + authorization?.GetType().Name);
			_session.UserId = _dcSession.UserId = self.id;
			lock (_session) _session.Save();
			RaiseUpdates(self);
			return User = self;
		}

		private MsgsAck CheckMsgsToAck()
		{
			lock (_msgsToAck)
			{
				if (_msgsToAck.Count == 0)
					return null;
				var msgsAck = new MsgsAck { msg_ids = [.. _msgsToAck] };
				_msgsToAck.Clear();
				return msgsAck;
			}
		}

		internal (long msgId, int seqno) NewMsgId(bool isContent)
		{
			int seqno;
			long msgId = DateTime.UtcNow.Ticks + _dcSession.serverTicksOffset - 621355968000000000L;
			msgId = msgId * 428 + (msgId >> 24) * 25110956; // approximately unixtime*2^32 and divisible by 4
			lock (_session)
			{
				if (msgId <= _dcSession.lastSentMsgId)
					msgId = _dcSession.lastSentMsgId += 4;
				else
					_dcSession.lastSentMsgId = msgId;
				seqno = isContent ? _dcSession.seqno++ * 2 + 1 : _dcSession.seqno * 2;
			}
			return (msgId, seqno);
		}

		/// <param name="containedRpc">For a container: the request inside it, so a failed write can be rescued</param>
		private async Task SendAsync(IObject msg, bool isContent, Rpc rpc = null, Rpc containedRpc = null)
		{
			if (_reactorTask == null && _paths.Count == 0)
				throw new WTException("You must connect to Telegram first");
			isContent &= _dcSession.authKeyID != 0;
			var (msgId, seqno) = NewMsgId(isContent);
			if (rpc != null)
			{
				rpc.sentTicks = Environment.TickCount64;
				if (isContent) // encrypted request: may be copied to another path under this msg_id
				{
					rpc.query = msg;
					rpc.seqno = seqno;
				}
				lock (_pendingRpcs)
					_pendingRpcs[rpc.msgId = msgId] = rpc;
			}
			if (isContent)
			{
				List<_Message> messages = null;
				if (_httpWait != null && NewMsgId(false) is var (hwId, hwSeqno))
					(messages ??= []).Add(new(hwId, hwSeqno, _httpWait));
				if (CheckMsgsToAck() is MsgsAck msgsAck && NewMsgId(false) is var (ackId, ackSeqno))
					(messages ??= []).Add(new(ackId, ackSeqno, msgsAck));
				if (messages != null)
				{
					messages.Add(new(msgId, seqno, msg));
					try
					{
						await SendAsync(new MsgContainer { messages = messages }, false, containedRpc: rpc);
					}
					catch when (rpc != null)
					{
						ForgetPending(rpc); // not answered: drop it so it cannot linger beside Invoke's retry
						throw;
					}
					HedgeIfWanted(rpc);
					return;
				}
			}
			Task receiveTask = null;
			Task pathWrite = null;
			TransportPath writePath = null;
			int writeLength = 0;
			long writeGeneration = 0;
			var sem = _sendSemaphore;
			try
			{
				await sem.WaitAsync(_cts.Token);
			}
			catch when (rpc != null)
			{
				ForgetPending(rpc); // the caller gets the exception: the request must not go out later as a copy
				throw;
			}
			try
			{
				// Select transport path for sending: a file part goes where the transfer scheduler put it
				// (if that path is still up), everything else by SendMode
				var path = (rpc ?? containedRpc)?.lease is { PathIndex: >= 0 } lease && lease.Client == this
					? AlivePathByIndex(lease.PathIndex) ?? GetPrimaryAlivePath()
					: GetPrimaryAlivePath();
				// The connection's parts, read together (a reconnect swaps them together under _pathsLock): the frame is
				// built, recorded and written for one and the same connection.
				Stream pathStream = null;
				long pathGeneration = 0;
				SHA256 pathSha256 = null;
				bool pathPadded = false;
				if (path != null)
					lock (_pathsLock)
						(pathStream, pathGeneration, pathSha256, pathPadded) = (path.NetworkStream, path.Generation, path.Sha256Send, path.PaddedMode);
				// For containers: update sentPathIndex on all contained RPCs.
				// These RPCs were registered in _pendingRpcs before container batching
				// (with sentTicks already set) but sentPathIndex was left at -1 since
				// the actual sending path wasn't known yet.
				if (msg is MsgContainer msgContainer && path != null)
				{
					lock (_pendingRpcs)
					{
						foreach (var m in msgContainer.messages)
							if (_pendingRpcs.TryGetValue(m.msg_id, out var crpc))
							{
								crpc.sentGen = pathGeneration;
								crpc.sentPathIndex = path.PathIndex;
								crpc.containerMsgId = msgId;
							}
					}
				}
				if (rpc != null && path != null)
				{
					rpc.sentGen = pathGeneration;
					rpc.sentPathIndex = path.PathIndex;
					// sentTicks stays the registration time (the msg_id's age); writtenTicks is set
					// after the write, for stall detection (a part queued behind others is not stalled)
				}
				var sha256Send = path != null ? pathSha256 : _sha256;
				var paddedMode = path != null ? pathPadded : _paddedMode;

				using var memStream = new MemoryStream(1024);
				using var writer = new BinaryWriter(memStream);
				writer.Write(0);                // int32 payload_len (to be patched with payload length)

				if (_dcSession.authKeyID == 0) // send unencrypted message
				{
					if (_bareRpc == null)
					throw new WTException($"Shouldn't send a {msg.GetType().Name} unencrypted");
					writer.Write(0L);                       // int64 auth_key_id = 0 (Unencrypted)
					writer.Write(msgId);                    // int64 message_id
					writer.Write(0);                        // int32 message_data_length (to be patched)
					Helpers.Log(1, $"{_dcSession.DcID}>Sending   {msg.GetType().Name.TrimEnd('_')}");
					writer.WriteTLObject(msg);              // bytes message_data
					BinaryPrimitives.WriteInt32LittleEndian(memStream.GetBuffer().AsSpan(20), (int)memStream.Length - 24);    // patch message_data_length
				}
				else
				{
					CheckSalt();
					using var clearStream = new MemoryStream(1024);
					using var clearWriter = new BinaryWriter(clearStream);
					clearWriter.Write(_dcSession.AuthKey, 88, 32);
					clearWriter.Write(_dcSession.Salt);     // int64 salt
					clearWriter.Write(_dcSession.id);       // int64 session_id
					clearWriter.Write(msgId);               // int64 message_id
					clearWriter.Write(seqno);               // int32 msg_seqno
					clearWriter.Write(0);                   // int32 message_data_length (to be patched)
					if ((seqno & 1) != 0)
						Helpers.Log(1, $"{_dcSession.DcID}>Sending   {msg.GetType().Name.TrimEnd('_'),-40} #{(short)msgId.GetHashCode():X4}{(path != null ? $" [P{path.PathIndex}]" : "")}");
					else
						Helpers.Log(1, $"{_dcSession.DcID}>Sending   {msg.GetType().Name.TrimEnd('_'),-40} {MsgIdToStamp(msgId):u} (svc){(path != null ? $" [P{path.PathIndex}]" : "")}");
					clearWriter.WriteTLObject(msg);         // bytes message_data
					int clearLength = (int)clearStream.Length - 32;  // length before padding (= 32 + message_data_length)
					int padding = (0x7FFFFFF0 - clearLength) % 16;
					padding += _random.Next(2, 16) * 16;        // MTProto 2.0 padding must be between 12..1024 with total length divisible by 16
					clearStream.SetLength(32 + clearLength + padding);
					byte[] clearBuffer = clearStream.GetBuffer();
					BinaryPrimitives.WriteInt32LittleEndian(clearBuffer.AsSpan(60), clearLength - 32);    // patch message_data_length
					RNG.GetBytes(clearBuffer, 32 + clearLength, padding);
					var msgKeyLarge = sha256Send.ComputeHash(clearBuffer, 0, 32 + clearLength + padding);
					const int msgKeyOffset = 8; // msg_key = middle 128-bits of SHA256(authkey_part+plaintext+padding)
					byte[] encrypted_data = EncryptDecryptMessage(clearBuffer.AsSpan(32, clearLength + padding), true, 0, _dcSession.AuthKey, msgKeyLarge, msgKeyOffset, sha256Send);

					writer.Write(_dcSession.authKeyID);             // int64 auth_key_id
					writer.Write(msgKeyLarge, msgKeyOffset, 16);    // int128 msg_key
					writer.Write(encrypted_data);                   // bytes encrypted_data
				}
				if (paddedMode) // Padded intermediate mode => append random padding
				{
					var padding = new byte[_random.Next(_dcSession.authKeyID == 0 ? 257 : 16)];
					RNG.GetBytes(padding);
					writer.Write(padding);
				}
				var buffer = memStream.GetBuffer();
				int frameLength = (int)memStream.Length;
				BinaryPrimitives.WriteInt32LittleEndian(buffer, frameLength - 4); // patch payload_len with correct value

				if (path != null)
				{
					// Multi-path TCP mode: queued on the selected path (in build order), written once the
					// semaphore is released, so a path that is slow to drain holds up only its own frames
					writePath = path;
					writeLength = frameLength;
					writeGeneration = pathGeneration;
					if ((rpc ?? containedRpc) is Rpc queued)
					{
						long queuedAt = Environment.TickCount64;
						Volatile.Write(ref queued.queuedTicks, queuedAt);
						MarkTransferQueued(queued, queuedAt);
					}
					pathWrite = pathStream != null
						? QueueFrameWrite(path, pathStream, buffer, frameLength)
						: Task.FromException(new IOException($"Path {path.PathIndex} has no connection"));
				}
				else if (_paths.Count > 0)
				{
					// Multipath mode but all paths are dead — don't fall through to HTTP.
					// Clean up the RPC since we can't send it.
					if (rpc != null) // just created by Invoke, never copied: no transfer to settle
						lock (_pendingRpcs) _pendingRpcs.Remove(rpc.msgId);
					throw new IOException("All transport paths are currently dead");
				}
				else if (_networkStream != null)
				{
					// Legacy single-connection TCP mode (MTProxy)
#if OBFUSCATION
					_sendCtr?.EncryptDecrypt(buffer.AsSpan(0, frameLength));
#endif
					await _networkStream.WriteAsync(buffer, 0, frameLength);
				}
				else
				{
					// HTTP mode
					receiveTask = SendReceiveHttp(buffer, frameLength);
				}
				_lastSentMsg = msg;
			}
			catch when (rpc != null)
			{
				ForgetPending(rpc); // the caller gets the exception: the request must not go out later as a copy
				throw;
			}
			finally
			{
				sem.Release();
			}
			if (pathWrite != null)
			{
				var subject = rpc ?? containedRpc;
				HedgeIfWanted(subject); // at once: the copy never waits for this path to drain
				// A request answered (by a copy) while its frame still waits to be written on a slow path: the
				// caller has its answer now; the write completes in the background, handled the same way.
				if (subject != null && !pathWrite.IsCompleted && await Task.WhenAny(pathWrite, subject.Task) != pathWrite)
					_ = AfterPathWriteAsync(pathWrite, writePath, writeGeneration, writeLength, subject);
				else
					try
					{
						await AfterPathWriteAsync(pathWrite, writePath, writeGeneration, writeLength, subject);
					}
					catch when (rpc != null)
					{
						ForgetPending(rpc); // the caller gets the exception: the request must not be re-sent after a reconnect
						throw;
					}
				return;
			}
			HedgeIfWanted(rpc);
			if (receiveTask != null)
				await receiveTask;
		}

		/// <summary>Once a queued frame is written: path counters and the request's write time. A failed write marks the
		/// path dead and reconnects it; the frame may have reached the server, so its request is copied (same msg_id)
		/// to another path rather than letting Invoke retry with a new one. With no other path alive it stays pending:
		/// the path's reconnect re-sends it the same way or hands it back to Invoke (RescueStrandedRpcsAsync /
		/// ResendPendingAfterReconnectAsync). A frame without a request rethrows the write error.</summary>
		/// <param name="generation">The connection the frame was queued on: a failure only ever concerns that one, and is
		/// acted on once (every frame queued behind it fails too)</param>
		private async Task AfterPathWriteAsync(Task write, TransportPath path, long generation, int length, Rpc subject)
		{
			try
			{
				await write;
				Interlocked.Add(ref path.BytesSent, length);
				var writtenAt = Environment.TickCount64;
				Volatile.Write(ref path.LastSendTicks, writtenAt);
				if (subject != null)
					Volatile.Write(ref subject.writtenTicks, writtenAt);
				return;
			}
			catch (IOException ex) when (_paths.Count > 0)
			{
				bool first, wasAlive = false;
				lock (_pathsLock)
				{
					first = path.Generation == generation && path.FailedGeneration != generation;
					if (first)
					{
						path.FailedGeneration = generation;
						wasAlive = path.IsAlive;
						path.IsAlive = false;
					}
				}
				if (first)
				{
					// Task.Run so ReconnectPathAsync (and its RaisePathChanged) starts on a ThreadPool thread,
					// never inline in a caller that may hold locks.
					AddPenalty(path, 500);
					if (wasAlive)
						Helpers.Log(3, $"{_dcSession.DcID}>Path {path.PathIndex} write failed ({ex.Message}). Reconnecting path in background.");
					if (_paths.Count > 1)
						_ = Task.Run(() => ReconnectPathAsync(path, ex));
					else
						path.NetworkStream?.Close(); // single path: its reactor fails and runs the full reconnect (with Updates_GetState)
				}
				if (subject == null)
					throw;
				await SendCopyAsync(subject, path.PathIndex, $"P{path.PathIndex} write failed", sole: true);
			}
		}

		/// <summary>Removes a request whose caller is getting an exception, unless it was already answered.</summary>
		private void ForgetPending(Rpc rpc)
		{
			bool forgotten = false;
			lock (_pendingRpcs)
				if (_pendingRpcs.TryGetValue(rpc.msgId, out var current) && current == rpc)
				{
					_pendingRpcs.Remove(rpc.msgId);
					// settled, like an answered one: a copy already out may still be answered (dropped as a second
					// answer), and a BadMsgNotification 32/33 about it is no reason for a session reset (nobody waits on it)
					_settledHedged[rpc.msgId] = Environment.TickCount64;
					forgotten = true;
				}
			if (forgotten)
				SettledTransfer(rpc);
		}

		/// <summary>Main client with several paths: every request also goes out on the next best path at once
		/// (same msg_id, executed once), so one slow or dying path never makes the caller wait.
		/// File parts are left alone here; they are copied only if they stall.</summary>
		private void HedgeIfWanted(Rpc rpc)
		{
			if (rpc?.query == null || rpc.bulk || rpc.hedged || rpc.sentPathIndex < 0)
				return;
			if (!HedgeAllRequests || _parentClient != null || _paths.Count < 2)
				return;
			if (rpc.query is TL.Methods.Ping or TL.Methods.PingDelayDisconnect)
				return;
			if (Interlocked.Exchange(ref rpc.hedgeStarted, 1) != 0)
				return; // once (a request in a container passes here twice)
			_ = SendCopyAsync(rpc, rpc.sentPathIndex, "Hedge", sole: false, onlyIfUnhedged: true);
		}

		private async Task SendReceiveHttp(byte[] buffer, int frameLength)
		{
			var endpoint = _dcSession?.EndPoint ?? GetDefaultEndpoint(out _);
			var content = new ByteArrayContent(buffer, 4, frameLength - 4);
			var response = await _httpClient.PostAsync($"http://{endpoint}/api", content, _cts.Token);
			if (response.StatusCode != HttpStatusCode.OK)
				throw new RpcException((int)response.StatusCode, TransportError((int)response.StatusCode));
			var data = await response.Content.ReadAsByteArrayAsync();
			var obj = ReadFrame(data, data.Length);
			if (obj != null)
				await HandleMessageAsync(obj);
		}

		/// <summary>Long poll on HTTP connections</summary>
		/// <param name="httpWait">Parameters for the long poll. Leave <see langword="null"/> for the default 25 seconds.</param>
		/// <remarks>⚠️ Telegram servers don't seem to support other parameter than <see langword="null"/> correctly</remarks>
		public async Task HttpWait(HttpWait httpWait = null)
		{
			if (_networkStream != null)
				throw new InvalidOperationException("Can't use HttpWait over TCP connection");
			var container = new MsgContainer { messages = [] };
			if (httpWait != null && NewMsgId(false) is var (hwId, hwSeqno))
				container.messages.Add(new(hwId, hwSeqno, httpWait));
			if (CheckMsgsToAck() is MsgsAck msgsAck && NewMsgId(false) is var (ackId, ackSeqno))
				container.messages.Add(new(ackId, ackSeqno, msgsAck));
			await SendAsync(container, false);
		}

		internal async Task<T> InvokeBare<T>(IMethod<T> request)
		{
			if (_bareRpc != null)
				throw new WTException("A bare request is already undergoing");
		retry:
			var bareRpc = _bareRpc = new Rpc { type = typeof(T) };
			await SendAsync(request, false, _bareRpc);
			var result = await bareRpc.Task;
			if (result is ReactorError)
				goto retry;
			return (T)result;
		}

		/// <summary>Call the given TL method <i>(You shouldn't need to use this method directly)</i></summary>
		/// <typeparam name="T">Expected type of the returned object</typeparam>
		/// <param name="query">TL method structure</param>
		/// <returns>Wait for the reply and return the resulting object, or throws an RpcException if an error was replied</returns>
		public virtual async Task<T> Invoke<T>(IMethod<T> query)
		{
			if (_dcSession.withoutUpdates && query is not IMethod<Pong> and not IMethod<FutureSalts>)
				query = new TL.Methods.InvokeWithoutUpdates<T> { query = query };
			bool got503 = false;
			int ioRetries = 0;
			static bool IsFilePart(IObject q) => q is TL.Methods.Upload_SaveFilePart or TL.Methods.Upload_SaveBigFilePart
				or TL.Methods.Upload_GetFile or TL.Methods.Upload_GetCdnFile or TL.Methods.Upload_GetWebFile;
			bool bulk = IsFilePart(query) || (query is TL.Methods.InvokeWithoutUpdates<T> { query: var inner } && IsFilePart(inner));
			var part = query is TL.Methods.InvokeWithoutUpdates<T> { query: var wrapped } ? wrapped : query;
			var (transferDir, transferBytes) = part switch
			{
				TL.Methods.Upload_SaveFilePart p => (TransferUp, p.bytes?.Length ?? 0),
				TL.Methods.Upload_SaveBigFilePart p => (TransferUp, p.bytes?.Length ?? 0),
				TL.Methods.Upload_GetFile p => (TransferDown, p.limit),
				_ => (-1, 0),
			};
			// the transfer scheduler's lease for this part (flows here from UploadFileAsync / DownloadFileAsync)
			var lease = transferDir >= 0 && CurrentTransfer.Value is TransferLease l && l.Dir == transferDir ? l : null;
			if (lease == null)
				transferDir = -1; // not scheduled (TransferMode FollowSendMode): none of the transfer handling, as before it
		retry:
			var rpc = new Rpc { type = typeof(T), bulk = bulk, transferDir = transferDir, transferBytes = transferBytes,
				lease = lease, stallExtraMs = lease?.StallExtraMs ?? 0,
				transferAttempt = lease != null ? Interlocked.Increment(ref lease.Attempts) : 0 };
			try
			{
				await SendAsync(query, true, rpc);
			}
			catch (IOException) when (_paths.Count > 0 && ++ioRetries <= 5)
			{
				if (lease != null)
					Volatile.Write(ref lease.QueuedTicks, 0); // this attempt is over: the wait below is no time on a WAN
				// Multipath: transport write failed on a dead path but other paths
				// may be alive (or reconnecting). Wait for path recovery, then retry.
				// Don't check Disconnected — even if all paths are currently dead,
				// ReconnectPathAsync is running in background and may restore them.
				Helpers.Log(3, $"{_dcSession.DcID}>Invoke: transport IOException, retrying ({ioRetries}/5)...");
				await Task.Delay(1000 * ioRetries);
				goto retry;
			}
			while (_httpClient != null && !rpc.Task.IsCompleted)
				await HttpWait(_httpWait); // need to wait a bit more in some case

			var result = await rpc.Task;
			if (lease != null) // this attempt is over (answered, or about to be retried): no more time on a WAN
				Volatile.Write(ref lease.QueuedTicks, 0);
			switch (result)
			{
				case null: return default;
				case T resultT: return resultT;
				case RpcError { error_code: var code, error_message: var message }:
					int x = -1;
					for (int index = message.Length - 1; index > 0 && (index = message.LastIndexOf('_', index - 1)) >= 0;)
						if (message[index + 1] is >= '0' and <= '9')
						{
							int end = ++index;
							do end++; while (end < message.Length && message[end] is >= '0' and <= '9');
							x = int.Parse(message[index..end]);
							message = $"{message[0..index]}X{message[end..]}";
							break;
						}

					if (code == 303 && message.EndsWith("_MIGRATE_X"))
					{
						if (message != "FILE_MIGRATE_X")
						{
							await MigrateToDC(x);
							goto retry;
						}
					}
					else if (code == 420 && (message.EndsWith("_WAIT_X") || message.EndsWith("_DELAY_X")))
					{
						if (x <= FloodRetryThreshold)
						{
							if (x == 0)
						x = 1;
							await Task.Delay(x * 1000);
							goto retry;
						}
					}
					else if (code == -503 && !got503)
					{
						got503 = true;
						goto retry;
					}
					else if (code == 401 && !IsMainDC && message is "SESSION_REVOKED" or "AUTH_KEY_UNREGISTERED") // need to renegociate alt-DC auth
					{
						lock (_session)
						{
							_session.DCSessions.Remove(_dcSession.DcID);
							if (_session.MainDC != -_dcSession.DcID)
						_session.DCSessions.Remove(-_dcSession.DcID);
							_session.Save();
						}
						await DisposeAsync();
					}
					else if (code == 400 && message == "CONNECTION_NOT_INITED")
					{
						await InitConnection();
						lock (_session) _session.Save();
						goto retry;
					}
					else if (code == 500 && message == "AUTH_RESTART")
						lock (_session)
						{
							_session.UserId = 0; // force a full login authorization flow, next time
							User = null;
							_session.Save();
						}
					throw new RpcException(code, message, x);
				case ReactorError:
					goto retry;
				default:
					throw new WTException($"{query.GetType().Name} call got a result of type {result.GetType().Name} instead of {typeof(T).Name}");
			}
		}

		private async Task MigrateToDC(int dcId)
		{
			// this is a hack to migrate _dcSession in-place (staying in same Client):
			Session.DCSession dcSession;
			lock (_session)
				dcSession = GetOrCreateDCSession(dcId, _dcSession.DataCenter.flags);
			await ResetAsync(false, false);
			_session.MainDC = dcId;
			_dcSession.Client = null;
			_dcSession = dcSession;
			_dcSession.Client = this;
			await ConnectAsync();
		}

		[EditorBrowsable(EditorBrowsableState.Never)]
		public async Task<T> InvokeAffected<T>(IMethod<T> query, long peerId) where T : Messages_AffectedMessages
		{
			var result = await Invoke(query);
			if (OnOwnUpdates != null)
				RaiseOwnUpdates(new UpdateShort
				{
					update = new UpdateAffectedMessages { mbox_id = peerId, pts = result.pts, pts_count = result.pts_count },
					date = MsgIdToStamp(_lastRecvMsgId)
				});
			return result;
		}
	}
}
