using Cross.Services.Agneta;
using Cross.Models;
using Cross.Services;
using Cross.Utilities;
using Newtonsoft.Json;

namespace Cross.Modules.Agneta;

public class LogMessage
{
    [JsonProperty("client_key")]
    public string ClientKey { get; set; }

    [JsonProperty("client_type")]
    public string ClientType { get; set; }

    [JsonProperty("log_level")]
    public int LogLevel { get; set; }

    [JsonProperty("log_message")]
    public string LogMessageText { get; set; }
}


public static class AgnetaHandler
{
    private static AgnetaClientService _instance;

    public static AgnetaClientService Instance => _instance ?? throw new InvalidOperationException("AgnetaClientService not initialized.");

    public static void SetInstance(AgnetaClientService instance)
    {
        _instance = instance ?? throw new ArgumentNullException(nameof(instance));
    }

    public static async Task Log(int _level, string _message)
    {
        return;
        if(_instance != null)
        {
            LogMessage _log = new LogMessage();
            _log.ClientKey = "NOT IMPLEMENTED (IP)";
            _log.ClientType = "Agent";
            _log.LogLevel = _level;
            _log.LogMessageText = _message;

            await _instance.SendMessageAsync(JsonConvert.SerializeObject(_log));
            // Console.WriteLine("Sent a message");
        }
        else
        {
            //PushoverHandler.PushNotification($"Gateway:(Cross):Failed to send log to agneta. No service running");
            Console.WriteLine("ERROR::AgnetaHandler.Log: No service running");
        }
    }

    //public static async Task SendUsageStats()
    //{
    //    await _instance.SendUsageStatistics();
    //}

    //public static async Task<NeighbourData> GetNeighbour()
    //{
    //    return await _instance.GetAssignedNeighbour();
    //}

    public static async Task Close()
    {
        return;
        await _instance.SendCloseAsync();
    }
}