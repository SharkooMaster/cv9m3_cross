
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
        float.TryParse(Environment.GetEnvironmentVariable("BLOAT_GUARD_THRESHOLD"), out var bgt) ? bgt : 0.60f;

    /// <summary>
    /// Local chunk clustering: group chunks with identical LSH bitstrings before
    /// searching agents. Reduces agent I/O by 5-50x and captures intra-file dedup
    /// that is otherwise impossible (chunks not yet stored when later chunks search).
    /// Set DISABLE_CHUNK_CLUSTERING=true to revert to per-chunk agent search.
    /// </summary>
    public static bool EnableChunkClustering =
        !string.Equals(Environment.GetEnvironmentVariable("DISABLE_CHUNK_CLUSTERING"), "true",
            StringComparison.OrdinalIgnoreCase);
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
