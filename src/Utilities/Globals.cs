
namespace Cross.Utilities;

public static class Globals
{
    public static int chunkSize = 10240;
    public static int k = 16;
    public static string GatewayLoadbalancer = "http://192.168.50.241:80";
    public static SearchAllServiceClient searchAllServiceClient = new SearchAllServiceClient();
}
