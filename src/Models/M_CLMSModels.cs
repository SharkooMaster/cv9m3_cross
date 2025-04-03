using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Text.Json;
using System.Text.Json.Serialization;
using Cross.Utilities;

public class M_RoutePoint
{
    public string? HeadRouteID { get; set; }
    public string? NodeName { get; set; }
    public string? NodeID { get; set; }
    public string? Status { get; set; }

    // Keep the original event objects, but don't expose them directly in JSON.
    [JsonIgnore]
    public List<M_CLMSEvent> Events { get; set; } = new List<M_CLMSEvent>();

    // Use calculated properties for serialization, so they’re generated on the fly.
    [JsonPropertyName("events")]
    public List<string> SerializedEvents => Events.Select(e => e.ToJson()).ToList();

    [JsonPropertyName("events_time_stamps")]
    public List<string> SerializedEventTimeStamps => Events.Select(e => e.startTime).ToList();

    public string ToJson()
    {
        string toRet = JsonSerializer.Serialize(this);
        Console.WriteLine(toRet);
        return toRet;
    }
}

public class M_CLMSEvent
{
    public string? Level { get; set; }   // e.g. "1*", "2", "3" or "log", "warn", "error"
    public string? startTime { get; set; }
    public string? StepName { get; set; }
    public string? Type { get; set; }    // e.g. "step*", "forward", "response"
    public string? Message { get; set; }

    // Monitoring
    public float? cpuUsage { get; set; }
    public float? ramUsage { get; set; }
    public float? ramMax { get; set; }

    public M_CLMSEvent()
    {
        // Record the event start time.
        startTime = DateTime.UtcNow.ToString("HH:mm:ss.fff", CultureInfo.InvariantCulture);
        // Use your custom methods from Cross.Utilities to get monitoring stats.
        cpuUsage = Convert.ToSingle(Misc.GetLoadAverage());
        ramUsage = Convert.ToSingle(Misc.GetMemoryUsagePercentage());
        ramMax = Misc.GetAvailableMemory();
    }

    public string ToJson()
    {
        return JsonSerializer.Serialize(this);
    }
}