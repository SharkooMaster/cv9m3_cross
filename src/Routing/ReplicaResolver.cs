using Cross.Routing;
using Cross.Utilities;

namespace Cross.Routing;

/// <summary>
/// Single entry-point for "what agents own this key?" — bridges the new
/// <see cref="RingState"/> (R replicas via consistent hashing) with the
/// legacy <see cref="RendezvousRouter"/> (1 owner via HRW).
///
/// Picks the ring when it has agents (etcd watcher published a snapshot)
/// and falls back to rendezvous when it doesn't, so a transient etcd
/// outage doesn't kill all writes — we just degrade to single-replica
/// writes during the outage and recover when etcd is back.
///
/// Returns IPs (not pod names) because every caller in the write/read
/// hot path needs to dial directly. PickReplicas (pod names) is exposed
/// for diagnostics/observability where the stable identity matters.
/// </summary>
public static class ReplicaResolver
{
    /// <summary>
    /// Default replication factor when the env var isn't set. Plan v1
    /// is R=3 — one primary + two secondaries → W=2 quorum tolerates
    /// a single-replica failure without blocking writes.
    /// </summary>
    public const int DefaultReplicationFactor = 3;

    /// <summary>
    /// Default write quorum. With R=3 and W=2 we accept a write as
    /// durable when any 2 of the 3 replicas have acked, which means
    /// any single replica can fail (or be slow) without holding up
    /// the writer.
    /// </summary>
    public const int DefaultWriteQuorum = 2;

    public static int ReplicationFactor
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("REPLICATION_FACTOR");
            if (int.TryParse(raw, out var v) && v > 0) return v;
            return DefaultReplicationFactor;
        }
    }

    public static int WriteQuorum
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("WRITE_QUORUM");
            if (int.TryParse(raw, out var v) && v > 0) return Math.Min(v, ReplicationFactor);
            return Math.Min(DefaultWriteQuorum, ReplicationFactor);
        }
    }

    /// <summary>
    /// Pick the R replica IPs for a key. The first IP is the "primary"
    /// and the rest are secondaries — callers that want to use a single
    /// agent's response (e.g. the encoder) should prefer the primary.
    ///
    /// Falls back to a single-element list (the rendezvous pick) when
    /// the ring is empty.
    /// </summary>
    public static IReadOnlyList<string> PickReplicaIps(string key, int? replicaCountOverride = null)
    {
        int r = replicaCountOverride ?? ReplicationFactor;

        var ring = RingState.Current;
        if (ring.Agents.Count > 0)
        {
            var ips = ring.PickReplicaIps(key, r);
            if (ips.Count > 0) return ips;
        }

        // Legacy fallback: single-replica via rendezvous. Returning a
        // single-element list keeps callers' shape uniform — they always
        // iterate a list, just one element when ring is unavailable.
        try
        {
            string fallback = RendezvousRouter.PickAgent(key);
            return new[] { fallback };
        }
        catch
        {
            return Array.Empty<string>();
        }
    }

    /// <summary>
    /// Pick the R replica pod names for a key, in priority order.
    /// </summary>
    public static IReadOnlyList<string> PickReplicaNames(string key, int? replicaCountOverride = null)
    {
        int r = replicaCountOverride ?? ReplicationFactor;
        var ring = RingState.Current;
        if (ring.Agents.Count == 0) return Array.Empty<string>();
        return ring.PickReplicas(key, r);
    }

    /// <summary>
    /// Convenience: just the primary IP. Identical to the legacy
    /// rendezvous PickAgent contract, used when migrating call sites
    /// one at a time.
    /// </summary>
    public static string PickPrimaryIp(string key)
    {
        var replicas = PickReplicaIps(key, 1);
        return replicas.Count > 0 ? replicas[0] : Globals.AgentsLoadbalancer;
    }
}
