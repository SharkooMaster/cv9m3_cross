
namespace Cross.Interfaces;

public interface ICLMSClientService
{
    Task<string> RegisterHeadRoute(); // Only used in CROSS
    Task RegisterRoutePoint(string _headRouteID, string _name, string _id);
    Task SendRoutePoint(string _headRouteID);

    Task AddEventToRoutePoint(string _headRouteID, M_CLMSEvent clmsEvent);
}
