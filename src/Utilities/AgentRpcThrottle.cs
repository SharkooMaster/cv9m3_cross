using System.Collections.Concurrent;

namespace Cross.Utilities;

/// <summary>
/// Per-agent in-flight RPC throttle, circuit breaker and shared retry policy
/// for cross→agent calls (BatchGet / BatchSearch / BatchStore / Store).
///
/// Why this exists
/// ───────────────
/// A single Compress request fans 4 096 chunks across 6 agents. Without a
/// throttle, cross can have 30–50 concurrent BatchGetAsync calls in flight to
/// the same agent, and each new caller piles on more. Combined with the
/// rendezvous-router's "hot bucket keys land on stable agents" property, a
/// single overloaded agent becomes a sink that drowns the whole cluster.
///
/// The two failure modes this prevents:
///   1. Queue explosion: 50+ in-flight RPCs per agent, agent threadpool
///      starves, latency spikes, deadlines used to trip on calls that were
///      genuinely making progress.
///   2. Stuck cluster: an overloaded agent slowly responds, but the gate
///      keeps every cross call queued behind older work, so new files
///      starve indefinitely. Removing the deadline (yesterday's change)
///      made symptom 1 go away but turned it into symptom 2: stuck clients.
///
/// Behaviour
/// ─────────
/// Three independent mechanisms per agent:
///
///   • Gate (SemaphoreSlim): caps in-flight RPCs to <see cref="MaxInflight"/>
///     (default 4, env AGENT_MAX_INFLIGHT_RPCS). Provides ordering only;
///     does not fail-fast.
///
///   • Circuit breaker (Closed → Open → HalfOpen → Closed):
///       - Closed: normal operation. Successes reset the failure counter;
///         failures increment it.
///       - Closed → Open: after N consecutive failures (default 5, env
///         AGENT_BREAKER_FAILURE_THRESHOLD) OR after a single timeout
///         exceeding the saturation budget. While Open, every call to
///         this agent fails fast with <see cref="CircuitOpenException"/>
///         so cross threads don't pile up against a sick agent.
///       - Open → HalfOpen: after a cooldown period (default 30 s, env
///         AGENT_BREAKER_COOLDOWN_SEC). The first call after cooldown
///         becomes the probe.
///       - HalfOpen → Closed: probe call succeeds. Failure counter resets,
///         normal traffic resumes.
///       - HalfOpen → Open: probe call fails. New cooldown starts. While a
///         probe is in flight, other callers immediately get
///         <see cref="CircuitOpenException"/> — only one trial at a time.
///
///   • Retry policy: callers wrap the RunAsync result in a bounded retry
///     loop (<see cref="MaxAttempts"/> attempts, <see cref="BackoffFor"/>
///     schedule). On <see cref="CircuitOpenException"/> the retry loop's
///     backoff naturally waits long enough for the breaker to half-open,
///     so the next attempt either succeeds (cluster recovered) or fails
///     again (still sick — surfaced as a clean error to the client after
///     N attempts).
///
/// The deadline contract
/// ─────────────────────
/// All cross→agent RPCs go through this throttle WITHOUT an application
/// deadline. Channel keep-alives (30 s ping + 10 s timeout in
/// GrpcChannelFactory) detect actually-dead TCP in ~40 s. A heavily loaded
/// but alive agent gets to take as long as it needs.
///
/// Failures (timeouts, gRPC errors, network) feed the breaker so a slow
/// agent eventually trips it and stops absorbing more work until it
/// recovers — which is the missing piece that left yesterday's "stuck
/// client" failure mode.
/// </summary>
public static class AgentRpcThrottle
{
    public static readonly int MaxInflight = ParseEnv("AGENT_MAX_INFLIGHT_RPCS", 4, 1);
    public static readonly int MaxAttempts = ParseEnv("AGENT_RPC_MAX_ATTEMPTS", 5, 1);
    public static readonly int FailureThreshold = ParseEnv("AGENT_BREAKER_FAILURE_THRESHOLD", 5, 1);
    public static readonly TimeSpan CooldownPeriod = TimeSpan.FromSeconds(ParseEnv("AGENT_BREAKER_COOLDOWN_SEC", 30, 1));

    private static readonly ConcurrentDictionary<string, AgentEntry> _entries = new();

    private static int ParseEnv(string name, int defaultValue, int min)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (int.TryParse(raw, out var v) && v >= min) return v;
        return defaultValue;
    }

    private static AgentEntry GetEntry(string agentIp) =>
        _entries.GetOrAdd(agentIp, _ => new AgentEntry(agentIp));

    /// <summary>
    /// Run an RPC against the given agent under the per-agent gate + breaker.
    /// Throws <see cref="CircuitOpenException"/> immediately if the breaker
    /// is Open. On exception the breaker counts the failure and may trip.
    /// On success the failure counter resets.
    /// </summary>
    public static async Task<T> RunAsync<T>(string agentIp, Func<Task<T>> work, CancellationToken ct = default)
    {
        var entry = GetEntry(agentIp);
        entry.AdmitOrThrow();
        await entry.Gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            var result = await work().ConfigureAwait(false);
            entry.RecordSuccess();
            return result;
        }
        catch (CircuitOpenException) { throw; }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch
        {
            entry.RecordFailure();
            throw;
        }
        finally
        {
            entry.Gate.Release();
        }
    }

    public static async Task RunAsync(string agentIp, Func<Task> work, CancellationToken ct = default)
    {
        var entry = GetEntry(agentIp);
        entry.AdmitOrThrow();
        await entry.Gate.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await work().ConfigureAwait(false);
            entry.RecordSuccess();
        }
        catch (CircuitOpenException) { throw; }
        catch (OperationCanceledException) when (ct.IsCancellationRequested) { throw; }
        catch
        {
            entry.RecordFailure();
            throw;
        }
        finally
        {
            entry.Gate.Release();
        }
    }

    /// <summary>
    /// True if the breaker for this agent is currently rejecting calls.
    /// Useful for routing decisions that have a fallback agent.
    /// </summary>
    public static bool IsCircuitOpen(string agentIp) =>
        _entries.TryGetValue(agentIp, out var e) && e.IsOpenNow();

    /// <summary>
    /// Backoff schedule for the bounded retry loops in Cross.cs.
    /// 1 s → 2 s → 5 s → 10 s → 20 s. The longer entries are intentionally
    /// long enough to exceed the breaker cooldown so a retry after a
    /// breaker trip has a fresh chance with a half-open probe.
    /// </summary>
    public static TimeSpan BackoffFor(int failedAttempt) => failedAttempt switch
    {
        0 => TimeSpan.FromSeconds(1),
        1 => TimeSpan.FromSeconds(2),
        2 => TimeSpan.FromSeconds(5),
        3 => TimeSpan.FromSeconds(10),
        _ => TimeSpan.FromSeconds(20),
    };

    /// <summary>
    /// Drop the per-agent state. Call alongside
    /// <c>GrpcChannelFactory.EvictChannel</c> when an agent leaves the
    /// topology so we don't leak gates over long-running pods.
    /// </summary>
    public static void EvictGate(string agentIp)
    {
        if (_entries.TryRemove(agentIp, out var entry))
        {
            try { entry.Gate.Dispose(); } catch { }
        }
    }

    /// <summary>
    /// Snapshot of current breaker state for the controlcenter dashboard.
    /// </summary>
    public static IReadOnlyDictionary<string, AgentBreakerSnapshot> Snapshot()
    {
        var dict = new Dictionary<string, AgentBreakerSnapshot>(_entries.Count);
        foreach (var kv in _entries)
        {
            dict[kv.Key] = kv.Value.SnapshotForUi();
        }
        return dict;
    }

    public enum CircuitState { Closed = 0, Open = 1, HalfOpen = 2 }

    public readonly record struct AgentBreakerSnapshot(
        string AgentIp,
        CircuitState State,
        int ConsecutiveFailures,
        long LastTransitionUnixMs,
        long LastSuccessUnixMs,
        long LastFailureUnixMs,
        int GateAvailable,
        int GateCapacity);

    /// <summary>
    /// Thrown by <see cref="RunAsync{T}"/> when the agent's breaker is Open
    /// or when another caller already holds the half-open probe slot. The
    /// retry loop catches this, sleeps a backoff interval, then retries —
    /// by which time the breaker will typically have transitioned through
    /// HalfOpen back to Closed (recovered) or stayed Open (still sick).
    /// </summary>
    public sealed class CircuitOpenException : Exception
    {
        public CircuitOpenException(string agentIp, string reason)
            : base($"Circuit open for agent {agentIp}: {reason}") { }
    }

    // ── Per-agent state ───────────────────────────────────────────────────

    private sealed class AgentEntry
    {
        public readonly string AgentIp;
        public readonly SemaphoreSlim Gate;

        private long _state; // CircuitState as int, accessed via Interlocked
        private int _consecutiveFailures;
        private long _openedAtTicks; // Stopwatch ticks
        private int _probeInFlight;  // 0 or 1
        private long _lastTransitionUnixMs;
        private long _lastSuccessUnixMs;
        private long _lastFailureUnixMs;

        public AgentEntry(string agentIp)
        {
            AgentIp = agentIp;
            Gate = new SemaphoreSlim(MaxInflight, MaxInflight);
            _lastTransitionUnixMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        }

        public bool IsOpenNow()
        {
            var s = (CircuitState)Interlocked.Read(ref _state);
            if (s != CircuitState.Open) return false;
            // Open might be transitioning to half-open by now; report ground truth.
            return System.Diagnostics.Stopwatch.GetElapsedTime(Interlocked.Read(ref _openedAtTicks)) < CooldownPeriod;
        }

        /// <summary>
        /// Decide whether the caller is admitted. If the breaker is Open and
        /// the cooldown has elapsed, exactly one caller wins the half-open
        /// probe slot and proceeds; everyone else gets <see cref="CircuitOpenException"/>.
        /// </summary>
        public void AdmitOrThrow()
        {
            var s = (CircuitState)Interlocked.Read(ref _state);
            if (s == CircuitState.Closed) return;

            if (s == CircuitState.HalfOpen)
            {
                // A probe is in flight; reject everyone else.
                throw new CircuitOpenException(AgentIp, "half-open probe in flight");
            }

            // s == Open: check if cooldown has elapsed.
            var elapsed = System.Diagnostics.Stopwatch.GetElapsedTime(Interlocked.Read(ref _openedAtTicks));
            if (elapsed < CooldownPeriod)
            {
                throw new CircuitOpenException(AgentIp,
                    $"cooling down ({(CooldownPeriod - elapsed).TotalSeconds:F1}s left, {Volatile.Read(ref _consecutiveFailures)} consecutive failures)");
            }

            // Cooldown elapsed — try to claim the probe slot.
            if (Interlocked.CompareExchange(ref _probeInFlight, 1, 0) != 0)
            {
                throw new CircuitOpenException(AgentIp, "probe claim lost");
            }

            // We won the probe. Transition Open → HalfOpen and let the caller through.
            if (Interlocked.CompareExchange(ref _state, (long)CircuitState.HalfOpen, (long)CircuitState.Open)
                == (long)CircuitState.Open)
            {
                Interlocked.Exchange(ref _lastTransitionUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                Console.WriteLine($"[Breaker] {AgentIp}: Open → HalfOpen (cooldown elapsed; probing)");
            }
            else
            {
                // Another thread already transitioned. Release the slot and reject.
                Interlocked.Exchange(ref _probeInFlight, 0);
                throw new CircuitOpenException(AgentIp, "lost transition race");
            }
        }

        public void RecordSuccess()
        {
            Interlocked.Exchange(ref _lastSuccessUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            Interlocked.Exchange(ref _consecutiveFailures, 0);

            var s = (CircuitState)Interlocked.Read(ref _state);
            if (s == CircuitState.HalfOpen)
            {
                // Probe succeeded — close the circuit.
                if (Interlocked.CompareExchange(ref _state, (long)CircuitState.Closed, (long)CircuitState.HalfOpen)
                    == (long)CircuitState.HalfOpen)
                {
                    Interlocked.Exchange(ref _lastTransitionUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                    Console.WriteLine($"[Breaker] {AgentIp}: HalfOpen → Closed (probe ok, recovered)");
                }
                Interlocked.Exchange(ref _probeInFlight, 0);
            }
        }

        public void RecordFailure()
        {
            Interlocked.Exchange(ref _lastFailureUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
            var s = (CircuitState)Interlocked.Read(ref _state);

            if (s == CircuitState.HalfOpen)
            {
                // Probe failed — reopen the circuit, restart cooldown.
                Interlocked.Exchange(ref _openedAtTicks, System.Diagnostics.Stopwatch.GetTimestamp());
                if (Interlocked.CompareExchange(ref _state, (long)CircuitState.Open, (long)CircuitState.HalfOpen)
                    == (long)CircuitState.HalfOpen)
                {
                    Interlocked.Exchange(ref _lastTransitionUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                    Console.WriteLine($"[Breaker] {AgentIp}: HalfOpen → Open (probe failed; cooling for {CooldownPeriod.TotalSeconds:F0}s)");
                }
                Interlocked.Exchange(ref _probeInFlight, 0);
                return;
            }

            var fails = Interlocked.Increment(ref _consecutiveFailures);
            if (s == CircuitState.Closed && fails >= FailureThreshold)
            {
                Interlocked.Exchange(ref _openedAtTicks, System.Diagnostics.Stopwatch.GetTimestamp());
                if (Interlocked.CompareExchange(ref _state, (long)CircuitState.Open, (long)CircuitState.Closed)
                    == (long)CircuitState.Closed)
                {
                    Interlocked.Exchange(ref _lastTransitionUnixMs, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
                    Console.WriteLine($"[Breaker] {AgentIp}: Closed → Open ({fails} consecutive failures; cooling for {CooldownPeriod.TotalSeconds:F0}s)");
                }
            }
        }

        public AgentBreakerSnapshot SnapshotForUi() => new(
            AgentIp: AgentIp,
            State: (CircuitState)Interlocked.Read(ref _state),
            ConsecutiveFailures: Volatile.Read(ref _consecutiveFailures),
            LastTransitionUnixMs: Interlocked.Read(ref _lastTransitionUnixMs),
            LastSuccessUnixMs: Interlocked.Read(ref _lastSuccessUnixMs),
            LastFailureUnixMs: Interlocked.Read(ref _lastFailureUnixMs),
            GateAvailable: Gate.CurrentCount,
            GateCapacity: MaxInflight);
    }
}
