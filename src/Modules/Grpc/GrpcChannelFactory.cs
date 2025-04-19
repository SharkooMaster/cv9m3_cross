
using System.Collections.Concurrent;
using Cross.Utilities;
using Grpc.Net.Client;

public static class GrpcChannelFactory
{
    private static readonly ConcurrentDictionary<string, GrpcChannel> _channels = new ConcurrentDictionary<string, GrpcChannel>();
    private static readonly ConcurrentDictionary<string, object> _clients = new ConcurrentDictionary<string, object>();

    public static GrpcChannel GetChannel(string ip, string port = "5000")
    {
        return _channels.GetOrAdd(ip, ipAddr => 
        {
            var handler = new SocketsHttpHandler
            {
                EnableMultipleHttp2Connections = true,
                PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
                KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
            };

            Console.WriteLine($"### Connecting to endpoint: http://{ipAddr}:{port} ###");
            return GrpcChannel.ForAddress($"http://{ipAddr}:{port}", new GrpcChannelOptions
            {
                HttpHandler = handler,
                ServiceConfig = Globals.GRPC_OPTIONS.ServiceConfig,
                LoggerFactory = Globals.GRPC_OPTIONS.LoggerFactory
            });
        });
    }

    public static TClient GetClient<TClient>(string ip, Func<GrpcChannel, TClient> ctor, string port = "5000") where TClient : class
    {
        // key by client-type + endpoint
        var key = $"{typeof(TClient).FullName}@{ip}:{port}";
        return (TClient)_clients.GetOrAdd(key, _ =>
        {
            var channel = GetChannel(ip, port);
            return ctor(channel);
        });
    }

}