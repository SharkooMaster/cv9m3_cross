
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using System;

namespace Cross.Utilities;

public static class Globals
{
    public static int chunkSize = int.TryParse(Environment.GetEnvironmentVariable("CHUNK_SIZE"), out var cs) ? cs : 5120;
    public static int k = 16;

    /// <summary>
    /// Minimum cosine similarity in LSH vector space for a search match to be considered.
    /// Higher values → fewer matches but tighter byte-level correlation.
    /// </summary>
    public static float SearchSimilarityThreshold =
        float.TryParse(Environment.GetEnvironmentVariable("SEARCH_SIMILARITY_THRESHOLD"), out var sst) ? sst : 0.60f;

    /// <summary>
    /// Maximum fraction of differing bytes allowed before a matched chunk is re-stored.
    /// This is a BYTE-LEVEL metric (Hamming ratio), independent of the cosine similarity
    /// threshold which operates in LSH vector space. The two can diverge significantly.
    /// </summary>
    public static float BloatGuardThreshold =
        float.TryParse(Environment.GetEnvironmentVariable("BLOAT_GUARD_THRESHOLD"), out var bgt) ? bgt : 0.75f;

    /// <summary>
    /// Bloat guard threshold for clustered non-representative chunks. These chunks
    /// share a bitstring with their rep, so they are genuinely similar. Ejecting a
    /// non-rep ADDS to datacenter storage (new store). Keeping it only adds to client
    /// .ccf diff size. Since datacenter growth is the priority metric, we tolerate a
    /// larger diff for non-reps before ejecting. Default 0.85 = eject only if >85%
    /// of bytes differ (nearly random relative to base).
    /// </summary>
    public static float ClusterBloatGuardThreshold =
        float.TryParse(Environment.GetEnvironmentVariable("CLUSTER_BLOAT_GUARD_THRESHOLD"), out var cbgt) ? cbgt : 0.85f;

    /// <summary>
    /// Local chunk clustering: group chunks with identical LSH bitstrings before
    /// searching agents. Reduces agent I/O by 5-50x and captures intra-file dedup
    /// that is otherwise impossible (chunks not yet stored when later chunks search).
    /// Set DISABLE_CHUNK_CLUSTERING=true to revert to per-chunk agent search.
    /// </summary>
    public static bool EnableChunkClustering =
        !string.Equals(Environment.GetEnvironmentVariable("DISABLE_CHUNK_CLUSTERING"), "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Level 2 Mosaic Dedup: for high-entropy chunks that fail Level 1 whole-vector search,
    /// decompose into 64 sub-regions and find the best donor per sub-region from the top-K
    /// candidates. Produces a mosaic base that reduces error encoding size.
    /// </summary>
    public static bool EnableMosaicDedup =
        string.Equals(Environment.GetEnvironmentVariable("ENABLE_MOSAIC_DEDUP"), "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Shannon entropy threshold (bits/byte) above which a chunk is considered high-entropy
    /// and eligible for Level 2 mosaic dedup. Range 0.0-8.0. Default 7.0.
    /// </summary>
    public static float MosaicEntropyThreshold =
        float.TryParse(Environment.GetEnvironmentVariable("MOSAIC_ENTROPY_THRESHOLD"), out var met) ? met : 7.0f;

    /// <summary>
    /// Number of top-K candidates to request from agents for high-entropy chunks.
    /// More candidates = better mosaic quality but more data returned from agents.
    /// </summary>
    public static int MosaicTopK =
        int.TryParse(Environment.GetEnvironmentVariable("MOSAIC_TOP_K"), out var mtk) ? mtk : 10;

    /// <summary>
    /// Number of top-K candidates to return from Level 1 search. Independent of mosaic.
    /// Agents return the K most similar chunks per query, enabling multi-reference
    /// diagnostics and future multi-reference error encoding.
    /// </summary>
    public static int SearchTopK =
        int.TryParse(Environment.GetEnvironmentVariable("SEARCH_TOP_K"), out var stk) ? stk : 10;

    public const int MosaicNComponents = 64;
    public static int MosaicSubChunkSize => chunkSize / MosaicNComponents;

    // ── Lane bucket index (Level 2 global sub-chunk search) ──
    public static int LaneHashBits =
        int.TryParse(Environment.GetEnvironmentVariable("LANE_HASH_BITS"), out var lhb) ? lhb : 16;

    public static int LaneSearchMaxPerQuery =
        int.TryParse(Environment.GetEnvironmentVariable("LANE_SEARCH_MAX"), out var lsm) ? lsm : 50;

    public static bool EnableLaneSearch =
        string.Equals(Environment.GetEnvironmentVariable("ENABLE_LANE_SEARCH"), "true",
            StringComparison.OrdinalIgnoreCase);

    // ── Cluster-stored CCF mode ──
    public static bool EnableCcfStore =
        string.Equals(Environment.GetEnvironmentVariable("ENABLE_CCF_STORE"), "true",
            StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// Diagnostic: after each block is compressed, decompress it in-process and
    /// compare byte-for-byte against the original. Catches the exact compress→
    /// decompress mismatch in the same process the bug occurs in, before the CCF
    /// even leaves cross. Off by default — adds a full decompress per compress
    /// (~20% latency) so only flip on while actively hunting integrity bugs.
    /// </summary>
    public static bool IntegrityDecompressSmoketest =
        string.Equals(Environment.GetEnvironmentVariable("INTEGRITY_DECOMPRESS_SMOKETEST"), "true",
            StringComparison.OrdinalIgnoreCase);

    public static bool EnableCcfBackgroundServices =
        !string.Equals(Environment.GetEnvironmentVariable("CCF_BACKGROUND_SERVICES_DISABLED"), "true",
            StringComparison.OrdinalIgnoreCase);

    public static string CcfStorePath =
        Environment.GetEnvironmentVariable("CCF_STORE_PATH") ?? "/data/ccf-store";

    public static int CcfPackThreshold =
        int.TryParse(Environment.GetEnvironmentVariable("CCF_PACK_THRESHOLD"), out var cpt) ? cpt : 100;

    public static int CcfPackIntervalSec =
        int.TryParse(Environment.GetEnvironmentVariable("CCF_PACK_INTERVAL_SEC"), out var cpi) ? cpi : 300;

    // ── Optional global CCF compaction (budgeted, non-blocking) ──
    public static bool CcfGlobalCompactionEnabled =
        string.Equals(Environment.GetEnvironmentVariable("CCF_GLOBAL_COMPACTION_ENABLED"), "true",
            StringComparison.OrdinalIgnoreCase);

    public static int CcfCompactionMaxSourcePacksPerCycle =
        int.TryParse(Environment.GetEnvironmentVariable("CCF_COMPACTION_MAX_SOURCE_PACKS_PER_CYCLE"), out var csp) ? csp : 2;

    public static long CcfCompactionMaxReadBytesPerCycle =
        long.TryParse(Environment.GetEnvironmentVariable("CCF_COMPACTION_MAX_READ_BYTES_PER_CYCLE"), out var crb) ? crb : 512L * 1024 * 1024;

    public static int CcfCompactionMaxDurationSec =
        int.TryParse(Environment.GetEnvironmentVariable("CCF_COMPACTION_MAX_DURATION_SEC"), out var cds) ? cds : 45;

    public static int CcfCompactionMaxWorkingSetMb =
        int.TryParse(Environment.GetEnvironmentVariable("CCF_COMPACTION_MAX_WORKING_SET_MB"), out var cws) ? cws : 0;

    public static int CcfOptimizerParallelism =
        int.TryParse(Environment.GetEnvironmentVariable("CCF_OPTIMIZER_PARALLELISM"), out var cop) ? cop : 0;

    public static bool CcfEncodingV6 =
        string.Equals(Environment.GetEnvironmentVariable("CCF_ENCODING_V6"), "true",
            StringComparison.OrdinalIgnoreCase);

    public static bool CcfNeuralErrorCompression =
        !string.Equals(Environment.GetEnvironmentVariable("CCF_NEURAL_ERROR_DISABLED"), "true",
            StringComparison.OrdinalIgnoreCase);

    internal static readonly SemaphoreSlim CcfOptimizationLock = new(1, 1);

    /// <summary>
    /// Returns an absolute working-set ceiling based on available container memory.
    /// Container-aware via GC.GetGCMemoryInfo (respects cgroup limits).
    /// </summary>
    public static long GetDynamicWorkingSetCeiling(double fraction = 0.85)
    {
        try
        {
            var gcInfo = GC.GetGCMemoryInfo();
            long totalAvailable = gcInfo.TotalAvailableMemoryBytes;
            long ceiling = (long)(totalAvailable * fraction);
            return Math.Clamp(ceiling, 1024L * 1024 * 1024, 48L * 1024 * 1024 * 1024);
        }
        catch
        {
            return 8L * 1024 * 1024 * 1024;
        }
    }

    //public static string GatewayLoadbalancer = "192.168.50.241";
    // Allow running outside Kubernetes/Docker by overriding via env var.
    // Examples:
    // - GATEWAY_LOADBALANCER=localhost
    // - GATEWAY_LOADBALANCER=127.0.0.1
    // - GATEWAY_LOADBALANCER=gateway (docker-compose / k8s service)
    public static string GatewayLoadbalancer = Environment.GetEnvironmentVariable("GATEWAY_LOADBALANCER") ?? "gateway";
    // Used by decompression pipeline to resolve chunks by (bucketId, bucketIndex).
    public static string AgentsLoadbalancer = Environment.GetEnvironmentVariable("AGENTS_LOADBALANCER") ?? "agent-1";
    public static SearchAllServiceClient searchAllServiceClient = new SearchAllServiceClient();

    // LOCAL MODE: Reduce retries in local mode (local network is reliable)
    // Note: gRPC requires MaxAttempts > 1, so ALWAYS return at least 2
    private static int GetMaxRetryAttempts()
    {
        // CRITICAL: gRPC requires MaxAttempts > 1, so ALWAYS return at least 2
        // In local mode, use 2; in distributed mode, use 4
        try
        {
            bool isLocal = LocalModeDetector.IsLocalMode();
            int attempts = isLocal ? 2 : 4;
            // Safety check: ensure we never return 1
            if (attempts <= 1)
            {
                Console.WriteLine($"[Cross Globals] WARNING: GetMaxRetryAttempts returned {attempts}, forcing to 2");
                attempts = 2;
            }
            Console.WriteLine($"[Cross Globals] GetMaxRetryAttempts: isLocal={isLocal}, returning {attempts}");
            return attempts;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[Cross Globals] ERROR in GetMaxRetryAttempts: {ex.Message}, defaulting to 2");
            return 2; // Safe default
        }
    }

    private static GrpcChannelOptions? _grpcOptions = null;
    private static readonly object _grpcOptionsLock = new object();

    public static GrpcChannelOptions GRPC_OPTIONS
    {
        get
        {
            if (_grpcOptions == null)
            {
                lock (_grpcOptionsLock)
                {
                    if (_grpcOptions == null)
                    {
                        int retryAttempts = GetMaxRetryAttempts();
                        byte maxAttempts = (byte)Math.Max(2, retryAttempts); // CRITICAL: Always >= 2
                        Console.WriteLine($"[Cross Globals] Creating GRPC_OPTIONS with MaxAttempts={maxAttempts} (from GetMaxRetryAttempts={retryAttempts})");
                        
                        _grpcOptions = new GrpcChannelOptions
                        {
                            HttpHandler = new SocketsHttpHandler()
                            {
                                EnableMultipleHttp2Connections = true,
                                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                                KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
                            },
                            ServiceConfig = new Grpc.Net.Client.Configuration.ServiceConfig()
                            {
                                MethodConfigs =
                                {
                                    new Grpc.Net.Client.Configuration.MethodConfig
                                    {
                                        Names = { MethodName.Default },
                                        RetryPolicy = new RetryPolicy
                                        {
                                            MaxAttempts = maxAttempts, // Use validated byte value
                                            InitialBackoff = TimeSpan.FromMilliseconds(200),
                                            MaxBackoff = TimeSpan.FromSeconds(1),
                                            BackoffMultiplier = 2,
                                            RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable, Grpc.Core.StatusCode.ResourceExhausted}
                                        }
                                    }
                                }
                            },
                            MaxReceiveMessageSize = 1000 * 1024 * 1024,
                            MaxSendMessageSize = 1000 * 1024 * 1024
                        };
                        
                        // Final validation
                        var createdMaxAttempts = _grpcOptions.ServiceConfig.MethodConfigs[0].RetryPolicy?.MaxAttempts;
                        if (createdMaxAttempts == null || createdMaxAttempts.Value <= 1)
                        {
                            Console.WriteLine($"[Cross Globals] ERROR: Created retry policy has MaxAttempts={createdMaxAttempts}, this will fail!");
                            throw new InvalidOperationException($"Retry policy MaxAttempts must be > 1, but got {createdMaxAttempts}");
                        }
                        Console.WriteLine($"[Cross Globals] ✅ GRPC_OPTIONS created successfully with MaxAttempts={createdMaxAttempts.Value}");
                    }
                }
            }
            return _grpcOptions;
        }
    }
}
