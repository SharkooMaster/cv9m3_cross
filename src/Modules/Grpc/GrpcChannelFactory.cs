
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
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10),
        };

        var options = new GrpcChannelOptions
        {
            HttpHandler = handler,
            // LoggerFactory = Globals.GRPC_OPTIONS.LoggerFactory,

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
                    RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable, Grpc.Core.StatusCode.ResourceExhausted }
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
                    RetryableStatusCodes = { Grpc.Core.StatusCode.Unavailable, Grpc.Core.StatusCode.ResourceExhausted }
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

}