
using System.Globalization;
using System.Text.Json;
using System.Text.Json.Serialization;
using Cross.Utilities;

public class M_RoutePoint
{
    public string? headRouteID;
    public string? nodeName;
    public string? nodeID;

    public List<string>? events = new List<string>();
    public List<string>? events_time_stamps = new List<string>();

    [JsonIgnore]
    public List<M_CLMSEvent> eventsObj = new List<M_CLMSEvent>();

    public string? status;

    public string ToJson()
    {
        foreach (M_CLMSEvent ev in eventsObj)
        {
            events.Add(ev.ToJson());
            events_time_stamps.Add(ev.startTime);
        }

        return JsonSerializer.Serialize(this);
    }
}

public class M_CLMSEvent
{
    public string? level; // 1*, 2, 3 | log, warn, error
    public string? startTime;
    public string? stepName;
    public string? type; // step*, forward, response
    public string? message;

    // Monitoring
    public float? cpuUsage;
    public float? ramUsage;
    public float? ramMax;

    public M_CLMSEvent()
    {
        startTime = DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
        cpuUsage = Convert.ToSingle(Misc.GetLoadAverage());
        ramUsage = Convert.ToSingle(Misc.GetMemoryUsagePercentage());
        ramMax = Misc.GetAvailableMemory();
    }

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }
}
