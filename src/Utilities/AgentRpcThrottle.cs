using System.Collections.Concurrent;

namespace Cross.Utilities;

/// <summary>
/// Per-agent in-flight RPC throttle and shared retry policy for cross→agent
/// calls (BatchGet / BatchSearch / BatchStore / Store).
///
/// Why this exists
/// ───────────────
/// A single Compress request fans 4 096 chunks across 6 agents. Without a
/// throttle, cross can have 30–50 concurrent BatchGetAsync calls in flight to
/// the same agent (one per route bucket × multiple parallel chunks), and each
/// new caller piles on more. The agent's threadpool starves, RPCs queue up,
/// and the old 30 s deadline used to fire on calls that were genuinely making
/// progress — which then triggered an infinite retry loop because
/// AgentHealthWatcher's lightweight ping made the agent look "healthy".
///
/// Behaviour
/// ─────────
/// • Caps in-flight cross→agent RPCs to <see cref="MaxInflight"/> per agent
///   (default 4, override with env <c>AGENT_MAX_INFLIGHT_RPCS</c>).
/// • Acquisition is non-blocking on the threadpool (SemaphoreSlim.WaitAsync).
/// • A separate gate exists per agent IP, so a slow agent can never starve
///   calls to the rest of the cluster.
/// • On agent topology change (pod IP rotation / node removal), call
///   <see cref="EvictGate"/> to drop the gate alongside the channel.
///
/// The deadline contract
/// ─────────────────────
/// All cross→agent RPCs that go through this throttle are issued **without an
/// application deadline**. Connection-level keep-alives (30 s ping + 10 s
/// timeout, configured in <c>GrpcChannelFactory</c>) detect actually-dead TCP
/// in ~40 s. A heavily-loaded but alive agent gets to take as long as it
/// needs without cross tearing down its in-flight work.
/// </summary>
public static class AgentRpcThrottle
{
    public static readonly int MaxInflight = ParseEnv("AGENT_MAX_INFLIGHT_RPCS", 4, 1);
    public static readonly int MaxAttempts = ParseEnv("AGENT_RPC_MAX_ATTEMPTS", 5, 1);

    private static readonly ConcurrentDictionary<string, SemaphoreSlim> _gates = new();

    private static int ParseEnv(string name, int defaultValue, int min)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (int.TryParse(raw, out var v) && v >= min) return v;
        return defaultValue;
    }

    private static SemaphoreSlim GetGate(string agentIp) =>
        _gates.GetOrAdd(agentIp, _ => new SemaphoreSlim(MaxInflight, MaxInflight));

    /// <summary>
    /// Run an RPC against the given agent under the per-agent gate. Use this
    /// for fire-and-forget calls where the caller already owns its own retry
    /// policy (e.g. lane fan-outs that tolerate single-pod failure).
    /// </summary>
    public static async Task<T> RunAsync<T>(string agentIp, Func<Task<T>> work, CancellationToken ct = default)
    {
        var gate = GetGate(agentIp);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            return await work().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    public static async Task RunAsync(string agentIp, Func<Task> work, CancellationToken ct = default)
    {
        var gate = GetGate(agentIp);
        await gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await work().ConfigureAwait(false);
        }
        finally
        {
            gate.Release();
        }
    }

    /// <summary>
    /// Backoff schedule for the bounded retry loops in Cross.cs. Lengths in
    /// milliseconds: 1 s, 2 s, 5 s, 10 s, 20 s. Index = attempt number that
    /// just failed (0-based). Returns <see cref="TimeSpan.Zero"/> for any
    /// attempt index past the table.
    /// </summary>
    public static TimeSpan BackoffFor(int failedAttempt)
    {
        // Tuned to give a slow agent breathing room without busy-spinning. The
        // agent ingest path keeps making progress while we wait — this is
        // strictly the "give it a sec" pause between attempts.
        return failedAttempt switch
        {
            0 => TimeSpan.FromSeconds(1),
            1 => TimeSpan.FromSeconds(2),
            2 => TimeSpan.FromSeconds(5),
            3 => TimeSpan.FromSeconds(10),
            _ => TimeSpan.FromSeconds(20),
        };
    }

    /// <summary>
    /// Drop the per-agent semaphore. Call alongside
    /// <c>GrpcChannelFactory.EvictChannel</c> when an agent leaves the
    /// topology so we don't leak gates over long-running pods.
    /// </summary>
    public static void EvictGate(string agentIp)
    {
        if (_gates.TryRemove(agentIp, out var gate))
        {
            try { gate.Dispose(); } catch { /* gate may have pending waiters; best-effort */ }
        }
    }
}
