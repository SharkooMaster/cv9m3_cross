using System.Runtime.CompilerServices;
using System.Threading.Channels;
using Crossv9.Jobevents;
using Google.Protobuf.WellKnownTypes;

namespace Cross.Services.JobEvents;

/// <summary>
/// In-process bus that the compression hot path emits to. The promise:
///   - Emit is a single non-blocking <c>Channel.Writer.TryWrite</c>.
///   - When the channel is full, new events are dropped (DropWrite). The
///     compression pipeline is NEVER backpressured by telemetry.
///   - When <c>JOB_EVENTS_ENABLED=false</c> the emit methods become an inlined no-op
///     (the kill switch is checked once at module load, so there is no per-call
///     branch cost in the hot path beyond the inlined <c>if</c>).
///   - Subscribers (the gRPC forwarder) read from the channel on a background
///     thread; if they fall behind, the queue absorbs up to <see cref="ChannelCapacity"/>
///     events before drops kick in.
///
/// Job correlation is via an <see cref="AsyncLocal{T}"/> so existing
/// <c>Observability.RecordStage</c> callsites can attribute themselves to the
/// surrounding job without changing their signatures.
/// </summary>
public static class JobEventBus
{
    /// <summary>Default 10 000 events × ~256 B = ~2.5 MB managed.</summary>
    public const int ChannelCapacity = 10_000;

    public static readonly bool Enabled =
        (Environment.GetEnvironmentVariable("JOB_EVENTS_ENABLED") ?? "true")
            .Equals("true", StringComparison.OrdinalIgnoreCase);

    private static readonly Channel<JobEvent> _channel = Channel.CreateBounded<JobEvent>(
        new BoundedChannelOptions(ChannelCapacity)
        {
            // DropWrite (not DropOldest): the producer takes a single TryWrite that
            // returns false when the channel is full, increments _dropped, and
            // returns. There is no list traversal or replacement on the hot path.
            FullMode = BoundedChannelFullMode.DropWrite,
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

    private static long _emitted;
    private static long _dropped;

    /// <summary>Reader for the forwarder background service.</summary>
    public static ChannelReader<JobEvent> Reader => _channel.Reader;

    /// <summary>Total events that succeeded the TryWrite (handed off to the channel).</summary>
    public static long Emitted => Interlocked.Read(ref _emitted);

    /// <summary>Total events dropped because the channel was full when TryWrite was called.</summary>
    public static long Dropped => Interlocked.Read(ref _dropped);

    // ── Ambient job context. AsyncLocal flows through Tasks/awaits so existing
    //    code paths can attribute themselves to a job without API changes. ──
    private static readonly AsyncLocal<string?> _currentJobId = new();
    private static readonly AsyncLocal<JobMode> _currentMode = new();

    public static string? CurrentJobId => _currentJobId.Value;
    public static JobMode CurrentMode => _currentMode.Value;

    /// <summary>RAII helper: sets the ambient job id for the current async flow.</summary>
    public sealed class JobScope : IDisposable
    {
        private readonly string? _previousId;
        private readonly JobMode _previousMode;
        public JobScope(string jobId, JobMode mode)
        {
            _previousId = _currentJobId.Value;
            _previousMode = _currentMode.Value;
            _currentJobId.Value = jobId;
            _currentMode.Value = mode;
        }
        public void Dispose()
        {
            _currentJobId.Value = _previousId;
            _currentMode.Value = _previousMode;
        }
    }

    public static JobScope BeginScope(string jobId, JobMode mode) => new(jobId, mode);

    // ── Pod identity (read once at startup, no per-emit env lookups) ──
    private static readonly string _podName =
        Environment.GetEnvironmentVariable("MY_POD_NAME") ?? Environment.MachineName ?? "unknown";
    private static readonly string _nodeName =
        Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown";

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static long NowUnixNs()
    {
        // DateTime.UtcNow.Ticks * 100 → nanoseconds since 0001. Convert to unix epoch.
        long ticks = DateTime.UtcNow.Ticks - DateTime.UnixEpoch.Ticks;
        return ticks * 100; // 1 tick = 100 ns
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private static void TryEmit(JobEvent ev)
    {
        if (_channel.Writer.TryWrite(ev)) Interlocked.Increment(ref _emitted);
        else Interlocked.Increment(ref _dropped);
    }

    // ══════════════════════════════════════════════════════════════════════
    //  EMIT METHODS
    //  Each is short, allocation-bounded (one JobEvent + small strings),
    //  and never throws (the gRPC-generated JobEvent has no failable setters).
    // ══════════════════════════════════════════════════════════════════════

    public static void EmitStarted(string jobId, JobMode mode, string fileName, ulong originalSize)
    {
        if (!Enabled) return;
        TryEmit(new JobEvent
        {
            JobId = jobId,
            CrossPod = _podName,
            CrossNode = _nodeName,
            TsUnixNs = (ulong)NowUnixNs(),
            Phase = JobPhase.Started,
            Mode = mode,
            FileName = fileName ?? string.Empty,
            OriginalSize = originalSize,
        });
    }

    public static void EmitStageDone(string stage, double stageMs, int? chunkCount = null,
        int? bucketCount = null, ulong? bytes = null)
    {
        if (!Enabled) return;
        var jobId = _currentJobId.Value;
        if (string.IsNullOrEmpty(jobId)) return; // stage outside a job — ignore (e.g., warmup)

        TryEmit(new JobEvent
        {
            JobId = jobId,
            CrossPod = _podName,
            CrossNode = _nodeName,
            TsUnixNs = (ulong)NowUnixNs(),
            Phase = JobPhase.StageDone,
            Mode = _currentMode.Value,
            Stage = stage ?? string.Empty,
            StageMs = stageMs,
            StageAttrChunkCount = (uint)Math.Max(0, chunkCount ?? 0),
            StageAttrBucketCount = (uint)Math.Max(0, bucketCount ?? 0),
            StageAttrBytes = bytes ?? 0,
        });
    }

    public static void EmitBlockDone(string parentJobId, int blockIndex, int blockCount,
        long bytesIn, long bytesOut, int refsFound, int chunks, long dcBytes, double blockMs)
    {
        if (!Enabled) return;
        TryEmit(new JobEvent
        {
            JobId = $"{parentJobId}:b{blockIndex}",
            ParentJobId = parentJobId,
            CrossPod = _podName,
            CrossNode = _nodeName,
            TsUnixNs = (ulong)NowUnixNs(),
            Phase = JobPhase.BlockDone,
            Mode = JobMode.Windowed,
            BlockIndex = (uint)Math.Max(0, blockIndex),
            BlockCount = (uint)Math.Max(0, blockCount),
            BlockBytesIn = (ulong)Math.Max(0, bytesIn),
            BlockBytesOut = (ulong)Math.Max(0, bytesOut),
            BlockRefsFound = (uint)Math.Max(0, refsFound),
            BlockChunks = (uint)Math.Max(0, chunks),
            BlockDcBytes = (ulong)Math.Max(0, dcBytes),
            BlockMs = blockMs,
        });
    }

    public static void EmitCompleted(string jobId, ulong compressedSize, int refsFound, int chunks,
        long dcBytes, double serverMs, double wallMs, float avgErrorRate, long errorPayloadBytes,
        string? fileId)
    {
        if (!Enabled) return;
        TryEmit(new JobEvent
        {
            JobId = jobId,
            CrossPod = _podName,
            CrossNode = _nodeName,
            TsUnixNs = (ulong)NowUnixNs(),
            Phase = JobPhase.Completed,
            FinalCompressedSize = compressedSize,
            FinalRefsFound = (uint)Math.Max(0, refsFound),
            FinalChunks = (uint)Math.Max(0, chunks),
            FinalDcBytes = (ulong)Math.Max(0, dcBytes),
            FinalServerMs = serverMs,
            FinalWallMs = wallMs,
            FinalAvgErrorRate = avgErrorRate,
            FinalErrorPayloadBytes = (ulong)Math.Max(0, errorPayloadBytes),
            FinalFileId = fileId ?? string.Empty,
        });
    }

    public static void EmitFailed(string jobId, string errorClass, string? errorMessage, string? errorStage)
    {
        if (!Enabled) return;
        TryEmit(new JobEvent
        {
            JobId = jobId,
            CrossPod = _podName,
            CrossNode = _nodeName,
            TsUnixNs = (ulong)NowUnixNs(),
            Phase = JobPhase.Failed,
            ErrorClass = errorClass ?? string.Empty,
            ErrorMessage = Truncate(errorMessage, 200) ?? string.Empty,
            ErrorStage = errorStage ?? string.Empty,
        });
    }

    private static string? Truncate(string? s, int max)
    {
        if (s == null) return null;
        return s.Length <= max ? s : s.Substring(0, max);
    }
}
