using System.Collections.Concurrent;
using System.Text;
using Cross.Interfaces;

namespace Cross.Services.Clms;

public class ClmsClientService : ICLMSClientService
{
    public ConcurrentDictionary<string, M_RoutePoint> routePoints = new ConcurrentDictionary<string, M_RoutePoint>();
    private readonly HttpClient client = new HttpClient();
    public string prefixURL = "http://192.168.50.244/";

    public async Task<string> RegisterHeadRoute()
    {
        try
        {
            string reqUri = $"{prefixURL}routetrace/head/new";
            string headRouteID = await client.GetAsync(reqUri).Result.Content.ReadAsStringAsync();
            Console.WriteLine($"HEADROUTEID_REGISTERED: {headRouteID}");
            return headRouteID;
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred:" + ex.Message);
            return "";
        }
    }

    public async Task RegisterRoutePoint(string _headRouteID, string _name, string _id)
    {
        if(!routePoints.TryAdd(_headRouteID, new M_RoutePoint() { HeadRouteID = _headRouteID, NodeName = _name, NodeID = _id, Status = "Running"}))
        {
            Console.WriteLine("Failed to register RoutePoint");
        }
    }

    public async Task SendRoutePoint(string _headRouteID)
    {
        try
        {
            string reqUri = $"{prefixURL}routetrace/point/new";
            var content = new StringContent(routePoints[_headRouteID].ToJson(), Encoding.UTF8, "application/json");
            await client.PostAsync(reqUri, content);

            if(routePoints.TryRemove(_headRouteID, out M_RoutePoint? x)){}
            else{ Console.WriteLine("Failed to remove route"); }
        }
        catch (Exception ex)
        {
            Console.WriteLine("An error occurred:" + ex.Message);
        }
    }

    public async Task AddEventToRoutePoint(string _headRouteID, M_CLMSEvent clmsEvent)
    {
        routePoints[_headRouteID].Events.Add(clmsEvent);
    }
}
