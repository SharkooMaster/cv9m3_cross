namespace Cross.Utilities;

/// <summary>
/// Detects if the system is running in local mode (single machine, Docker) vs distributed mode.
/// </summary>
public static class LocalModeDetector
{
    private static readonly bool _isLocalMode;

    static LocalModeDetector()
    {
        _isLocalMode = DetectLocalMode();
    }

    public static bool IsLocalMode()
    {
        return _isLocalMode;
    }

    private static bool DetectLocalMode()
    {
        // Check environment variable first (explicit override)
        var mode = Environment.GetEnvironmentVariable("CROSS_MODE");
        if (mode?.Equals("local", StringComparison.OrdinalIgnoreCase) == true)
            return true;
        
        if (mode?.Equals("distributed", StringComparison.OrdinalIgnoreCase) == true)
            return false;

        // Check if running in Docker with single agent
        if (File.Exists("/.dockerenv"))
        {
            var agentCount = Environment.GetEnvironmentVariable("AGENT_COUNT");
            if (agentCount == "1" || string.IsNullOrEmpty(agentCount))
                return true;
        }

        // Check if Gateway URL is "local" (in-process)
        var gatewayUrl = Environment.GetEnvironmentVariable("CrossService__Url");
        if (gatewayUrl?.Equals("local", StringComparison.OrdinalIgnoreCase) == true)
            return true;

        // Check if we're likely running locally (not in a distributed environment)
        // If no explicit mode set, default to local for Docker environments
        if (File.Exists("/.dockerenv"))
            return true;

        return false;
    }
}

