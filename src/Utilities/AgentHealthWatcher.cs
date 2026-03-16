using System.Collections.Concurrent;
using Google.Protobuf.WellKnownTypes;

namespace Cross.Utilities;

public enum AgentStatus { Healthy, Suspect, Down }

public sealed class AgentHealthWatcher : BackgroundService
{
    private static AgentHealthWatcher? _instance;
    public static AgentHealthWatcher Instance =>
        _instance ?? throw new InvalidOperationException("AgentHealthWatcher not started");

    private readonly ConcurrentDictionary<string, AgentStatus> _agentStatus = new();
    private readonly ConcurrentDictionary<string, int> _failCount = new();
    private readonly ConcurrentDictionary<string, TaskCompletionSource<bool>> _recoveryWaiters = new();
    private readonly object _waitLock = new();

    private volatile bool _anyAgentReady;
    private readonly TaskCompletionSource<bool> _firstAgentReady = new(TaskCreationOptions.RunContinuationsAsynchronously);

    private const int SuspectAfter = 1;
    private const int DownAfter = 3;

    public AgentHealthWatcher()
    {
        _instance = this;
    }

    public bool IsHealthy(string agentIp)
    {
        return !_agentStatus.TryGetValue(agentIp, out var status) || status == AgentStatus.Healthy;
    }

    /// <summary>
    /// Block until the given agent IP is reachable again.
    /// If the agent comes back with a new IP (pod restart), the caller must
    /// re-resolve via RendezvousRouter after this returns.
    /// </summary>
    public async Task WaitForAgentAsync(string agentIp, CancellationToken ct)
    {
        if (IsHealthy(agentIp)) return;

        TaskCompletionSource<bool> tcs;
        lock (_waitLock)
        {
            if (!_recoveryWaiters.TryGetValue(agentIp, out tcs!))
            {
                tcs = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
                _recoveryWaiters[agentIp] = tcs;
            }
        }

        using var reg = ct.Register(() => tcs.TrySetCanceled());
        await tcs.Task;
    }

    /// <summary>
    /// Wait until at least one agent is discovered. Used at startup to gate
    /// incoming compression requests.
    /// </summary>
    public Task WaitForFirstAgentAsync(CancellationToken ct)
    {
        if (_anyAgentReady) return Task.CompletedTask;
        return WaitForFirstAgentCoreAsync(ct);
    }

    private async Task WaitForFirstAgentCoreAsync(CancellationToken ct)
    {
        using var reg = ct.Register(() => _firstAgentReady.TrySetCanceled());
        await _firstAgentReady.Task;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        Console.WriteLine("[AgentHealthWatcher] Starting background health monitor");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await CheckAgentsAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[AgentHealthWatcher] Health check error: {ex.Message}");
            }

            try { await Task.Delay(5000, stoppingToken); }
            catch (OperationCanceledException) { break; }
        }
    }

    private async Task CheckAgentsAsync()
    {
        RendezvousRouter.ForceRefresh();
        var currentIps = RendezvousRouter.GetAgents();

        if (currentIps.Length > 0 && !_anyAgentReady)
        {
            _anyAgentReady = true;
            _firstAgentReady.TrySetResult(true);
        }

        var previousIps = new HashSet<string>(_agentStatus.Keys);

        foreach (var ip in currentIps)
        {
            bool alive = await ProbeAgentAsync(ip);
            previousIps.Remove(ip);

            if (alive)
            {
                var oldStatus = _agentStatus.TryGetValue(ip, out var s) ? s : AgentStatus.Healthy;
                _agentStatus[ip] = AgentStatus.Healthy;
                _failCount[ip] = 0;

                if (oldStatus != AgentStatus.Healthy)
                {
                    Console.WriteLine($"[AgentHealthWatcher] Agent {ip} recovered (was {oldStatus})");
                    NotifyRecovery(ip);
                }
            }
            else
            {
                int fails = _failCount.AddOrUpdate(ip, 1, (_, c) => c + 1);
                var newStatus = fails >= DownAfter ? AgentStatus.Down
                              : fails >= SuspectAfter ? AgentStatus.Suspect
                              : AgentStatus.Healthy;

                var oldStatus = _agentStatus.TryGetValue(ip, out var s2) ? s2 : AgentStatus.Healthy;
                _agentStatus[ip] = newStatus;

                if (newStatus != oldStatus)
                    Console.WriteLine($"[AgentHealthWatcher] Agent {ip}: {oldStatus} -> {newStatus} (fails={fails})");
            }
        }

        foreach (var goneIp in previousIps)
        {
            _agentStatus.TryRemove(goneIp, out _);
            _failCount.TryRemove(goneIp, out _);
            GrpcChannelFactory.EvictChannel(goneIp);
            Console.WriteLine($"[AgentHealthWatcher] Agent {goneIp} removed from topology");
            NotifyRecovery(goneIp);
        }
    }

    private void NotifyRecovery(string agentIp)
    {
        lock (_waitLock)
        {
            if (_recoveryWaiters.TryRemove(agentIp, out var tcs))
                tcs.TrySetResult(true);
        }
    }

    private static async Task<bool> ProbeAgentAsync(string ip)
    {
        try
        {
            var client = GrpcChannelFactory.GetClient(
                target: ip,
                ctor: chan => new GetNodeInfo.GetNodeInfoClient(chan),
                roundRobin: false, port: 5000);

            await client.GetAsync(new Empty(), deadline: DateTime.UtcNow.AddSeconds(5));
            return true;
        }
        catch
        {
            return false;
        }
    }
}
