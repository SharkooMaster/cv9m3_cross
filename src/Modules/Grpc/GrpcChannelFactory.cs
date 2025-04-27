
using Cross.Utilities;
using System;
using System.Collections.Concurrent;
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

        if (useRoundRobin)
        {
            // Start with a clean ServiceConfig
            var sc = new ServiceConfig();

            // Copy over your retry + backoff method configs
            foreach (var mc in Globals.GRPC_OPTIONS.ServiceConfig.MethodConfigs)
                sc.MethodConfigs.Add(mc);

            // Now add the round‑robin balancer
            sc.LoadBalancingConfigs.Add(new RoundRobinConfig());

            options.ServiceConfig = sc;
        }
        else
        {
            // Just use your existing policy
            options.ServiceConfig = Globals.GRPC_OPTIONS.ServiceConfig;
        }

        Console.WriteLine($"### Opening channel to {uri} (RR={useRoundRobin}) ###");
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