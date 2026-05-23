
using Cross.Utilities;
using System;
using System.Collections.Concurrent;
using System.Linq;
using System.Net.Http;
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;
using Grpc.Core;

public static class GrpcChannelFactory
{

    private static readonly ConcurrentDictionary<string, GrpcChannel> _channels
      = new ConcurrentDictionary<string, GrpcChannel>();

    /// <summary>
    /// Number of cached gRPC channels. A monotonically rising value here while
    /// the cluster is otherwise stable is the canonical signature of a channel
    /// leak (e.g. agent IP churn that bypasses EvictOnFailure). Surfaced into
    /// /stats/runtime so the control center can graph it per-pod.
    /// </summary>
    public static int ChannelCacheCount => _channels.Count;

    /// <summary>
    /// Gets (or creates) a normal single‑endpoint channel.
    /// </summary>
    public static GrpcChannel GetChannel(string ipOrHost, int port = 5000)
    {
        var uri = $"http://{ipOrHost}:{port}";
        return _channels.GetOrAdd(uri, _ => MakeChannel(uri, useRoundRobin: false));
    }

    /// <summary>
    /// Gets (or creates) a headless‑service channel that will round‑robin across all pods.
    /// </summary>
    public static GrpcChannel GetRoundRobinChannel(
        string headlessServiceName,    // e.g. "agent-headless.default.svc.cluster.local"
        int port = 5000)
    {
        // The "dns:///" prefix tells Grpc.Net.Client to use the DNS resolver
        var uri = $"dns:///{headlessServiceName}:{port}";
        return _channels.GetOrAdd(uri, _ => MakeChannel(uri, useRoundRobin: true));
    }

    private static GrpcChannel MakeChannel(string uri, bool useRoundRobin)
    {
        var handler = new SocketsHttpHandler
        {
            EnableMultipleHttp2Connections = true,
            // Finite recycling — Timeout.InfiniteTimeSpan keeps stale HTTP/2 connections
            // to dead/restarted pods forever, producing PROTOCOL_ERROR on every reuse.
            PooledConnectionIdleTimeout = TimeSpan.FromMinutes(2),
            PooledConnectionLifetime = TimeSpan.FromMinutes(5),
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
        };

        var options = new GrpcChannelOptions
        {
            HttpHandler = handler,
            // 256 MiB cap: the V7 batch path sends `SEARCH_BATCH=1024` query
            // objects with embedded chunk bytes per request. At Globals.chunkSize
            // = 64 KiB that's already 64 MiB of payload; the previous 32 MiB
            // limit silently failed those calls with ResourceExhausted as soon
            // as chunk size grew. Match the gateway's 1 GiB *read* limit on
            // the receive side too — the gateway can ship up to 1 GiB back
            // when it forwards multi-agent search results, and we'd rather
            // accept a fat message than retry-storm.
            MaxReceiveMessageSize = 256 * 1024 * 1024,
            MaxSendMessageSize = 256 * 1024 * 1024,
            Credentials = ChannelCredentials.Insecure
        };

        // Get the retry policy from Globals, ensuring MaxAttempts >= 2
        var sourceConfig = Globals.GRPC_OPTIONS.ServiceConfig;
        byte maxAttempts = 2; // Safe default - CRITICAL: gRPC requires > 1
        
        // Extract max attempts from source config
        if (sourceConfig?.MethodConfigs != null)
        {
            foreach (var mc in sourceConfig.MethodConfigs)
            {
                if (mc?.RetryPolicy != null && mc.RetryPolicy.MaxAttempts.HasValue)
                {
                    byte sourceMaxAttempts = (byte)mc.RetryPolicy.MaxAttempts.Value;
                    if (sourceMaxAttempts <= 1)
                    {
                        Console.WriteLine($"[GrpcChannelFactory] ERROR: Source MaxAttempts={sourceMaxAttempts} is <= 1, forcing to 2");
                        maxAttempts = 2;
                    }
                    else
                    {
                        maxAttempts = sourceMaxAttempts;
                    }
                    Console.WriteLine($"[GrpcChannelFactory] Using MaxAttempts={maxAttempts} for channel to {uri}");
                    break;
                }
            }
        }
        
        // Final safety check - this should never happen, but just in case
        if (maxAttempts <= 1)
        {
            Console.WriteLine($"[GrpcChannelFactory] CRITICAL ERROR: maxAttempts={maxAttempts} is <= 1, forcing to 2");
            maxAttempts = 2;
        }

        if (useRoundRobin)
        {
            // Start with a clean ServiceConfig
            var sc = new ServiceConfig();

            // Create a new MethodConfig with validated retry policy
            var sourceRetryPolicy = (sourceConfig?.MethodConfigs?.Count > 0) ? sourceConfig.MethodConfigs[0]?.RetryPolicy : null;
            var newMethodConfig = new Grpc.Net.Client.Configuration.MethodConfig
            {
                Names = { MethodName.Default },
                RetryPolicy = new RetryPolicy
                {
                    MaxAttempts = maxAttempts, // Use validated value
                    InitialBackoff = sourceRetryPolicy?.InitialBackoff ?? TimeSpan.FromMilliseconds(200),
                    MaxBackoff = sourceRetryPolicy?.MaxBackoff ?? TimeSpan.FromSeconds(1),
                    BackoffMultiplier = sourceRetryPolicy?.BackoffMultiplier ?? 2,
                    // Internal covers HTTP/2 PROTOCOL_ERROR on stale connections after
                    // pod restart. All our RPCs are either reads (Search) or content-
                    // addressable stores (chunk hash dedups on agent side) — safe to retry.
                    RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable, Grpc.Core.StatusCode.Internal }
                }
            };
            sc.MethodConfigs.Add(newMethodConfig);

            // Now add the round‑robin balancer
            sc.LoadBalancingConfigs.Add(new RoundRobinConfig());

            options.ServiceConfig = sc;
        }
        else
        {
            // Create a new ServiceConfig with validated retry policy
            var sc = new ServiceConfig();
            var sourceRetryPolicy = (sourceConfig?.MethodConfigs?.Count > 0) ? sourceConfig.MethodConfigs[0]?.RetryPolicy : null;
            var newMethodConfig = new Grpc.Net.Client.Configuration.MethodConfig
            {
                Names = { MethodName.Default },
                RetryPolicy = new RetryPolicy
                {
                    MaxAttempts = maxAttempts, // Use validated value
                    InitialBackoff = sourceRetryPolicy?.InitialBackoff ?? TimeSpan.FromMilliseconds(200),
                    MaxBackoff = sourceRetryPolicy?.MaxBackoff ?? TimeSpan.FromSeconds(1),
                    BackoffMultiplier = sourceRetryPolicy?.BackoffMultiplier ?? 2,
                    // Internal covers HTTP/2 PROTOCOL_ERROR on stale connections after
                    // pod restart. Same rationale as round-robin case above.
                    RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable, Grpc.Core.StatusCode.Internal }
                }
            };
            sc.MethodConfigs.Add(newMethodConfig);
            options.ServiceConfig = sc;
        }

        // Final validation before creating channel
        var finalRetryPolicy = options.ServiceConfig?.MethodConfigs?.FirstOrDefault()?.RetryPolicy;
        if (finalRetryPolicy?.MaxAttempts != null && finalRetryPolicy.MaxAttempts.Value <= 1)
        {
            Console.WriteLine($"[GrpcChannelFactory] CRITICAL: Final retry policy has MaxAttempts={finalRetryPolicy.MaxAttempts.Value}, this will fail!");
            throw new InvalidOperationException($"Cannot create channel: Retry policy MaxAttempts must be > 1, but got {finalRetryPolicy.MaxAttempts.Value}");
        }
        
        Console.WriteLine($"### Opening channel to {uri} (RR={useRoundRobin}, MaxAttempts={finalRetryPolicy?.MaxAttempts ?? (byte?)null}) ###");
        return GrpcChannel.ForAddress(uri, options);
    }


    /// <summary>
    /// Generic client‑stub getter:
    /// </summary>
    /// <summary>
    /// Number of cached typed-client stubs. There is one per
    /// (TClient, channelUri) pair, so this count grows at most by the number
    /// of distinct stub types per target.
    /// </summary>
    public static int ClientCacheCount => _clients.Count;

    private static readonly ConcurrentDictionary<string, object> _clients
      = new ConcurrentDictionary<string, object>();

    public static TClient GetClient<TClient>(
        string target,
        Func<GrpcChannel, TClient> ctor,
        bool roundRobin = false,
        int port = 5000)
        where TClient : class
    {
        // pick the right channel URI
        var channelUri = roundRobin
            ? $"dns:///{target}:{port}"
            : $"http://{target}:{port}";

        var key = $"{typeof(TClient).FullName}@{channelUri}";

        return (TClient)_clients.GetOrAdd(key, _ =>
        {
            var channel = roundRobin
                ? GetRoundRobinChannel(target, port)
                : GetChannel(target, port);
            return ctor(channel);
        });
    }

    /// <summary>
    /// Remove cached channel and all client stubs for a given target.
    /// Evicts BOTH the direct-IP form (`http://target:port`) and the
    /// round-robin DNS form (`dns:///target:port`), because the previous
    /// implementation only removed the direct-IP form — round-robin clients
    /// stayed cached on a dead-channel forever after a pod restart, manifesting
    /// as repeated PROTOCOL_ERROR / Unavailable on the same target.
    /// </summary>
    public static void EvictChannel(string target, int port = 5000)
    {
        var httpUri = $"http://{target}:{port}";
        var dnsUri = $"dns:///{target}:{port}";

        foreach (var uri in new[] { httpUri, dnsUri })
        {
            if (_channels.TryRemove(uri, out var channel))
            {
                Console.WriteLine($"[GrpcChannelFactory] Evicting channel to {uri}");
                try { channel.Dispose(); } catch { }
            }

            var suffix = $"@{uri}";
            var keysToRemove = _clients.Keys.Where(k => k.EndsWith(suffix, StringComparison.Ordinal)).ToList();
            foreach (var key in keysToRemove)
                _clients.TryRemove(key, out _);
        }
    }

    /// <summary>
    /// Inspect <paramref name="ex"/>; if it indicates a dead/stale connection
    /// to <paramref name="target"/> (RpcException Unavailable / Internal /
    /// DeadlineExceeded, or a raw transport failure), evict the cached channel
    /// + client stubs for that target. Safe to call on every retry path —
    /// non-transport errors are no-ops, and a missing cache entry is also a
    /// no-op. The next <see cref="GetClient{TClient}"/> for the same target
    /// rebuilds a fresh channel + does a fresh DNS resolve, which is the only
    /// reliable recovery for a Kubernetes pod restart that reused the same
    /// service hostname.
    /// </summary>
    public static void EvictOnFailure(string target, Exception ex, int port = 5000)
    {
        bool shouldEvict = false;
        if (ex is RpcException rex)
        {
            shouldEvict = rex.StatusCode == StatusCode.Unavailable
                       || rex.StatusCode == StatusCode.Internal
                       || rex.StatusCode == StatusCode.DeadlineExceeded;
        }
        else if (ex is System.Net.Http.HttpRequestException
              || ex is System.Net.Sockets.SocketException
              || (ex.InnerException is System.Net.Http.HttpRequestException)
              || (ex.InnerException is System.Net.Sockets.SocketException))
        {
            shouldEvict = true;
        }

        if (shouldEvict && !string.IsNullOrWhiteSpace(target))
        {
            try { EvictChannel(target, port); } catch { /* best effort */ }
        }
    }

}