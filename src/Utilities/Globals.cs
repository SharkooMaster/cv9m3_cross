
using Grpc.Net.Client;
using Grpc.Net.Client.Configuration;

namespace Cross.Utilities;

public static class Globals
{
    public static int chunkSize = 5120;
    public static int k = 16;
    //public static string GatewayLoadbalancer = "192.168.50.241";
    public static string GatewayLoadbalancer = "gateway-headless.cross-test.svc.cluster.local";
    public static SearchAllServiceClient searchAllServiceClient = new SearchAllServiceClient();

    public static GrpcChannelOptions GRPC_OPTIONS = new GrpcChannelOptions
    {
        HttpHandler = new SocketsHttpHandler()
        {
            EnableMultipleHttp2Connections = true,
            PooledConnectionIdleTimeout = Timeout.InfiniteTimeSpan,
            KeepAlivePingDelay = TimeSpan.FromSeconds(30),
            KeepAlivePingTimeout = TimeSpan.FromSeconds(10)
        },
/*         LoggerFactory = LoggerFactory.Create(lb =>
        {
            lb.AddConsole();
            lb.SetMinimumLevel(LogLevel.Debug);
        }), */
        ServiceConfig = new Grpc.Net.Client.Configuration.ServiceConfig()
        {
            MethodConfigs =
            {
                new Grpc.Net.Client.Configuration.MethodConfig
                {
                    Names = { MethodName.Default },
                    RetryPolicy = new RetryPolicy
                    {
                        MaxAttempts = 4,
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
}
