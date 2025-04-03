
using Cross.Services.Clms;

namespace Cross.Modules;

public static class ClmsHandler
{
    private static ClmsClientService _instance;
    public static void SetClmsInstance(ClmsClientService _clmsClientService)
    {
        _instance = _clmsClientService;
    }

    public static async Task<string> RegisterHeadRoute()
    {
        return await _instance.RegisterHeadRoute();
    }

    public static async Task RegisterRoutePoint(string _headRouteID)
    {
        await _instance.RegisterRoutePoint(_headRouteID);
    }

    public static async Task SendRoutePoint(string _headRouteID)
    {
        await _instance.SendRoutePoint(_headRouteID);
    }

    public static async Task AddEventToRoutePoint(string _headRouteID, M_CLMSEvent clmsEvent)
    {
        await _instance.AddEventToRoutePoint(_headRouteID, clmsEvent);
    }
}
