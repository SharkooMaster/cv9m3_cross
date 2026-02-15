using System.Diagnostics;

namespace Cross.Utilities;

/// <summary>
/// Dynamically adjusts concurrency limits based on current CPU and memory usage.
/// Prevents resource overuse by scaling down when system is under load.
/// </summary>
public static class DynamicResourceManager
{
    private static readonly object _lock = new object();
    private static DateTime _lastCheck = DateTime.MinValue;
    private static double _lastCpuLoad = 0.0;
    private static double _lastMemoryUsage = 0.0;
    private static readonly TimeSpan _checkInterval = TimeSpan.FromSeconds(2); // Check every 2 seconds

    // Safety thresholds
    private const double CPU_HIGH_THRESHOLD = 0.80; // 80% CPU usage - scale down aggressively
    private const double CPU_MEDIUM_THRESHOLD = 0.60; // 60% CPU usage - scale down moderately
    private const double MEMORY_HIGH_THRESHOLD = 0.85; // 85% memory usage - scale down aggressively
    private const double MEMORY_MEDIUM_THRESHOLD = 0.70; // 70% memory usage - scale down moderately

    /// <summary>
    /// Gets the optimal concurrency limit based on current system resources.
    /// Returns a value between minConcurrency and maxConcurrency.
    /// </summary>
    public static int GetOptimalConcurrency(int minConcurrency, int maxConcurrency, int baseConcurrency)
    {
        lock (_lock)
        {
            // Check if we need to update metrics (throttle checks to avoid overhead)
            if (DateTime.UtcNow - _lastCheck > _checkInterval)
            {
                UpdateMetrics();
            }

            // Calculate adjustment factor based on CPU and memory
            double cpuFactor = GetCpuAdjustmentFactor();
            double memoryFactor = GetMemoryAdjustmentFactor();
            
            // Use the more restrictive factor (conservative approach)
            double adjustmentFactor = Math.Min(cpuFactor, memoryFactor);

            // Calculate optimal concurrency
            int optimalConcurrency = (int)(baseConcurrency * adjustmentFactor);
            
            // Clamp to min/max bounds
            optimalConcurrency = Math.Max(minConcurrency, Math.Min(maxConcurrency, optimalConcurrency));

            return optimalConcurrency;
        }
    }

    /// <summary>
    /// Gets the optimal MaxDegreeOfParallelism for ParallelOptions.
    /// </summary>
    public static int GetOptimalParallelism(int baseParallelism)
    {
        int minParallelism = Math.Max(1, baseParallelism / 4); // At least 25% of base
        int maxParallelism = baseParallelism;
        return GetOptimalConcurrency(minParallelism, maxParallelism, baseParallelism);
    }

    private static void UpdateMetrics()
    {
        try
        {
            _lastCpuLoad = GetCurrentCpuLoad();
            _lastMemoryUsage = GetCurrentMemoryUsage();
            _lastCheck = DateTime.UtcNow;
        }
        catch
        {
            // Use last known values on error
        }
    }

    private static double GetCpuAdjustmentFactor()
    {
        int processorCount = Environment.ProcessorCount;
        double normalizedLoad = _lastCpuLoad / processorCount; // Normalize load average to per-core

        if (normalizedLoad >= CPU_HIGH_THRESHOLD)
        {
            // CPU is high - scale down to 30% of base
            return 0.30;
        }
        else if (normalizedLoad >= CPU_MEDIUM_THRESHOLD)
        {
            // CPU is medium - scale down to 50% of base
            return 0.50;
        }
        else if (normalizedLoad >= 0.40)
        {
            // CPU is moderate - scale down to 75% of base
            return 0.75;
        }
        else
        {
            // CPU is low - use full base concurrency
            return 1.0;
        }
    }

    private static double GetMemoryAdjustmentFactor()
    {
        if (_lastMemoryUsage >= MEMORY_HIGH_THRESHOLD)
        {
            // Memory is high - scale down to 30% of base
            return 0.30;
        }
        else if (_lastMemoryUsage >= MEMORY_MEDIUM_THRESHOLD)
        {
            // Memory is medium - scale down to 50% of base
            return 0.50;
        }
        else if (_lastMemoryUsage >= 0.50)
        {
            // Memory is moderate - scale down to 75% of base
            return 0.75;
        }
        else
        {
            // Memory is low - use full base concurrency
            return 1.0;
        }
    }

    private static double GetCurrentCpuLoad()
    {
        try
        {
            if (File.Exists("/proc/loadavg"))
            {
                string[] parts = File.ReadAllText("/proc/loadavg").Split(' ', StringSplitOptions.RemoveEmptyEntries);
                return double.Parse(parts[0]);
            }
        }
        catch { }

        // Fallback: Use Process CPU time (less accurate but works everywhere)
        try
        {
            var process = Process.GetCurrentProcess();
            var cpuTime = process.TotalProcessorTime.TotalMilliseconds;
            var elapsed = (DateTime.UtcNow - process.StartTime).TotalMilliseconds;
            return (cpuTime / elapsed) * Environment.ProcessorCount;
        }
        catch { }

        return 0.0; // Unknown - assume low load
    }

    private static double GetCurrentMemoryUsage()
    {
        try
        {
            if (File.Exists("/proc/meminfo"))
            {
                double totalMemory = 0;
                double freeMemory = 0;

                var lines = File.ReadAllLines("/proc/meminfo");
                foreach (var line in lines)
                {
                    if (line.StartsWith("MemTotal:"))
                    {
                        totalMemory = ParseMemValue(line);
                    }
                    else if (line.StartsWith("MemAvailable:"))
                    {
                        freeMemory = ParseMemValue(line);
                        break;
                    }
                }

                if (totalMemory > 0)
                {
                    double usedMemory = totalMemory - freeMemory;
                    return usedMemory / totalMemory;
                }
            }
        }
        catch { }

        // Fallback: Use GC memory info
        try
        {
            var gcInfo = GC.GetGCMemoryInfo();
            var totalMemory = gcInfo.TotalAvailableMemoryBytes;
            var heapMemory = gcInfo.HeapSizeBytes;
            return (double)heapMemory / totalMemory;
        }
        catch { }

        return 0.0; // Unknown - assume low usage
    }

    private static double ParseMemValue(string line)
    {
        var parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
        return double.Parse(parts[1]) / 1024; // Convert KB to MB
    }
}

