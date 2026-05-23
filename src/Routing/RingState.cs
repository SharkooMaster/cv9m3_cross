namespace Cross.Routing;

/// <summary>
/// Process-wide holder for the active <see cref="ConsistentHashRing"/>.
///
/// The membership watcher (etcd) calls <see cref="Set"/> on every event;
/// every PickReplicas call reads the current ring through <see cref="Current"/>.
/// Writers and readers race only on the volatile reference: the ring
/// itself is immutable, so a reader never sees a torn snapshot.
///
/// We also stamp the topology version so consumers (e.g. the rebalance
/// coordinator) can detect transitions without holding a reference to
/// the previous ring.
/// </summary>
public static class RingState
{
    private static volatile ConsistentHashRing _current = ConsistentHashRing.Empty;
    private static long _topologyVersion;
    private static long _lastUpdatedTicks;

    public static ConsistentHashRing Current => _current;

    /// <summary>
    /// Monotonically increasing topology version. Bumped on every Set
    /// where the ring actually changed (membership delta or vnode-count
    /// change). Useful for the rebalance coordinator and for diagnostics.
    /// </summary>
    public static long TopologyVersion => Interlocked.Read(ref _topologyVersion);

    public static DateTime LastUpdated
    {
        get
        {
            long t = Interlocked.Read(ref _lastUpdatedTicks);
            return t == 0 ? DateTime.MinValue : new DateTime(t, DateTimeKind.Utc);
        }
    }

    /// <summary>
    /// Atomically swap in a new ring. Returns true if the ring changed
    /// (different agents, different IPs, or different vnode count).
    /// </summary>
    public static bool Set(ConsistentHashRing next)
    {
        if (next == null) throw new ArgumentNullException(nameof(next));

        var prev = _current;
        bool changed = !RingsAreEqual(prev, next);
        _current = next;
        Interlocked.Exchange(ref _lastUpdatedTicks, DateTime.UtcNow.Ticks);
        if (changed)
        {
            Interlocked.Increment(ref _topologyVersion);
        }
        return changed;
    }

    private static bool RingsAreEqual(ConsistentHashRing a, ConsistentHashRing b)
    {
        if (ReferenceEquals(a, b)) return true;
        if (a.Agents.Count != b.Agents.Count) return false;
        if (a.VnodesPerAgent != b.VnodesPerAgent) return false;
        for (int i = 0; i < a.Agents.Count; i++)
        {
            if (!string.Equals(a.Agents[i], b.Agents[i], StringComparison.Ordinal)) return false;
            var aIp = a.GetIp(a.Agents[i]);
            var bIp = b.GetIp(b.Agents[i]);
            if (!string.Equals(aIp, bIp, StringComparison.Ordinal)) return false;
        }
        return true;
    }
}
