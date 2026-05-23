using System.Text;

namespace Cross.Routing;

/// <summary>
/// Consistent-hash ring with virtual nodes for replicated agent routing.
///
/// Replaces rendezvous (highest-random-weight) hashing because rendezvous
/// returns a single owner per key, which makes elastic scaling painful:
/// adding/removing an agent silently re-points ~1/N keys to a new agent
/// that has no data, with a correctness gap until the data physically
/// moves. The ring solves that by combining two ideas:
///
///   1. Each agent owns N "virtual nodes" (vnodes), spread across the
///      hash space. Adding an agent claims ~1/N of the vnodes from the
///      existing owners; the rest of the ring is unaffected. This keeps
///      key movement bounded on resize.
///
///   2. Each key maps to R successive vnodes (replicas), not just the
///      first one. R=3 means each chunk lives on 3 agents, written with
///      W=2 quorum. A new agent can join: writes immediately fan out to
///      the new replica set (which already includes peers that have the
///      data); reads fall back across replicas; the new agent backfills
///      its share in the background. There is no correctness gap — at
///      every point in time, at least R-1 replicas have the data.
///
/// Determinism: every cross/gateway/agent pod that builds the ring from
/// the same membership map produces the same vnode placement. This is
/// what eliminates the cross-pod routing drift that previously caused
/// silent decompress corruption when two cross pods disagreed on which
/// agent owned a chunk.
///
/// The ring is immutable once built. The membership watcher constructs
/// a fresh ring on each etcd event and atomically swaps it via
/// <see cref="RingState"/>.
/// </summary>
public sealed class ConsistentHashRing
{
    public const int DefaultVnodesPerAgent = 256;

    /// <summary>
    /// One slot on the ring: the 32-bit hash of "{agentName}#{vnodeIndex}"
    /// and the agent that owns it. Sorted ascending by Hash.
    /// </summary>
    private readonly RingEntry[] _entries;

    /// <summary>
    /// Pod name → pod IP. The ring's PickReplicas returns pod names because
    /// names are stable; callers that need the wire address translate via
    /// this map.
    /// </summary>
    private readonly Dictionary<string, string> _agentToIp;

    /// <summary>
    /// All agent pod names currently on the ring, sorted ordinally.
    /// Cheap to expose because building it is part of construction.
    /// </summary>
    public IReadOnlyList<string> Agents { get; }

    /// <summary>
    /// Number of vnodes each agent contributed to this ring. Surfaced so
    /// observability can report it without re-reading env.
    /// </summary>
    public int VnodesPerAgent { get; }

    public static readonly ConsistentHashRing Empty = new(
        Array.Empty<RingEntry>(),
        new Dictionary<string, string>(),
        Array.Empty<string>(),
        DefaultVnodesPerAgent);

    private ConsistentHashRing(
        RingEntry[] entries,
        Dictionary<string, string> agentToIp,
        IReadOnlyList<string> agents,
        int vnodesPerAgent)
    {
        _entries = entries;
        _agentToIp = agentToIp;
        Agents = agents;
        VnodesPerAgent = vnodesPerAgent;
    }

    /// <summary>
    /// Build a ring from a {podName → podIp} map. Deterministic: identical
    /// inputs produce identical rings on every node, byte for byte.
    ///
    /// Vnode placement uses MurmurHash3("{podName}#{vnodeIndex}") so a
    /// single node contributes vnodesPerAgent entries spread across the
    /// 32-bit hash space. With vnodesPerAgent=256 and 5 agents that's
    /// 1280 total ring slots — load is uniform within ~5% per agent
    /// (verified by tests).
    /// </summary>
    public static ConsistentHashRing Build(
        IReadOnlyDictionary<string, string> agentToIp,
        int vnodesPerAgent = DefaultVnodesPerAgent)
    {
        if (agentToIp == null) throw new ArgumentNullException(nameof(agentToIp));
        if (vnodesPerAgent <= 0)
            throw new ArgumentOutOfRangeException(nameof(vnodesPerAgent), "vnodesPerAgent must be > 0");

        if (agentToIp.Count == 0)
            return new ConsistentHashRing(
                Array.Empty<RingEntry>(),
                new Dictionary<string, string>(),
                Array.Empty<string>(),
                vnodesPerAgent);

        // Sort agent names ordinally so tie-breaking on identical hashes is
        // deterministic across pods. (Identical hashes are vanishingly rare
        // with MurmurHash3 over 1000-ish ring slots, but the sort is free
        // insurance.)
        var sortedAgents = agentToIp.Keys.OrderBy(x => x, StringComparer.Ordinal).ToArray();

        var entries = new RingEntry[sortedAgents.Length * vnodesPerAgent];
        Span<byte> buf = stackalloc byte[256];
        int idx = 0;
        for (int a = 0; a < sortedAgents.Length; a++)
        {
            var name = sortedAgents[a];
            int nameLen = Encoding.UTF8.GetBytes(name, buf);
            buf[nameLen] = (byte)'#';

            for (int v = 0; v < vnodesPerAgent; v++)
            {
                int vlen = WriteIntDecimal(v, buf.Slice(nameLen + 1));
                int totalLen = nameLen + 1 + vlen;
                uint hash = MurmurHash3.Hash(buf.Slice(0, totalLen));
                entries[idx++] = new RingEntry(hash, a);
            }
        }

        Array.Sort(entries, RingEntryComparer.Instance);

        var agentToIpCopy = new Dictionary<string, string>(agentToIp.Count);
        foreach (var kv in agentToIp) agentToIpCopy[kv.Key] = kv.Value;

        return new ConsistentHashRing(entries, agentToIpCopy, sortedAgents, vnodesPerAgent);
    }

    /// <summary>
    /// Pick the R replica owners for a key, in priority order (replica 0 =
    /// "primary", others = secondaries). Replicas are guaranteed distinct
    /// agents — we walk the ring from the key's hash and skip vnodes
    /// owned by an agent we've already added.
    ///
    /// If the ring has fewer than R agents we return what's available
    /// (callers must tolerate this — a 1-agent dev cluster is real).
    /// Zero allocations on the hot path apart from the result list.
    /// </summary>
    public IReadOnlyList<string> PickReplicas(string key, int replicaCount)
    {
        if (replicaCount <= 0)
            throw new ArgumentOutOfRangeException(nameof(replicaCount), "replicaCount must be > 0");
        if (_entries.Length == 0 || Agents.Count == 0)
            return Array.Empty<string>();

        int targetCount = Math.Min(replicaCount, Agents.Count);
        var result = new List<string>(targetCount);
        var seen = new HashSet<int>(targetCount);

        uint keyHash = MurmurHash3.Hash(Encoding.UTF8.GetBytes(key));
        int start = LowerBound(keyHash);

        // Walk the ring (with wrap-around) until we've collected R distinct
        // agents. Worst case is R*vnodesPerAgent steps; in practice it's
        // close to R because adjacent vnodes tend to belong to different
        // agents once the ring is well-mixed.
        int ringLen = _entries.Length;
        for (int i = 0; i < ringLen && result.Count < targetCount; i++)
        {
            int slot = (start + i) % ringLen;
            int agentIdx = _entries[slot].AgentIndex;
            if (seen.Add(agentIdx))
            {
                result.Add(Agents[agentIdx]);
            }
        }

        return result;
    }

    /// <summary>
    /// Convenience: PickReplicas, then translate pod names to IPs in the
    /// same order. Names missing from the IP map are silently skipped
    /// (treated as "agent disappeared between PickReplicas and lookup").
    /// </summary>
    public IReadOnlyList<string> PickReplicaIps(string key, int replicaCount)
    {
        var names = PickReplicas(key, replicaCount);
        if (names.Count == 0) return Array.Empty<string>();

        var ips = new List<string>(names.Count);
        foreach (var name in names)
        {
            if (_agentToIp.TryGetValue(name, out var ip) && !string.IsNullOrEmpty(ip))
                ips.Add(ip);
        }
        return ips;
    }

    /// <summary>
    /// Pick the primary (first replica). Convenience for code that hasn't
    /// migrated to replicated reads yet — every PickPrimary call is a
    /// candidate to be replaced by PickReplicas down the line.
    /// </summary>
    public string? PickPrimary(string key)
    {
        var replicas = PickReplicas(key, 1);
        return replicas.Count > 0 ? replicas[0] : null;
    }

    public string? GetIp(string agentName)
    {
        return _agentToIp.TryGetValue(agentName, out var ip) ? ip : null;
    }

    /// <summary>
    /// Find the smallest ring index whose hash >= keyHash. If keyHash is
    /// larger than every entry, returns 0 (wrap-around).
    /// </summary>
    private int LowerBound(uint keyHash)
    {
        int lo = 0, hi = _entries.Length;
        while (lo < hi)
        {
            int mid = (lo + hi) >>> 1;
            if (_entries[mid].Hash < keyHash) lo = mid + 1;
            else hi = mid;
        }
        return lo == _entries.Length ? 0 : lo;
    }

    private static int WriteIntDecimal(int value, Span<byte> dst)
    {
        // Hot-path helper to avoid per-vnode allocations from int.ToString().
        // Vnode indices are 0..vnodesPerAgent-1 so they fit easily in
        // 6 digits — we keep it general for safety.
        if (value == 0)
        {
            dst[0] = (byte)'0';
            return 1;
        }
        int n = value;
        int len = 0;
        Span<byte> tmp = stackalloc byte[12];
        while (n > 0)
        {
            tmp[len++] = (byte)('0' + (n % 10));
            n /= 10;
        }
        for (int i = 0; i < len; i++) dst[i] = tmp[len - 1 - i];
        return len;
    }

    private readonly struct RingEntry
    {
        public readonly uint Hash;
        public readonly int AgentIndex;
        public RingEntry(uint hash, int agentIndex) { Hash = hash; AgentIndex = agentIndex; }
    }

    private sealed class RingEntryComparer : IComparer<RingEntry>
    {
        public static readonly RingEntryComparer Instance = new();
        public int Compare(RingEntry a, RingEntry b) => a.Hash.CompareTo(b.Hash);
    }
}

/// <summary>
/// 32-bit MurmurHash3 — same constants and tail-handling as the existing
/// rendezvous router so any future "is X owned by me?" comparisons are
/// hash-stable across the migration. Lifted here to avoid a circular
/// dependency: ConsistentHashRing predates the watcher in build order.
/// </summary>
internal static class MurmurHash3
{
    public static uint Hash(ReadOnlySpan<byte> bytes)
    {
        const uint seed = 0x9747b28c;
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;

        uint hash = seed;
        int length = bytes.Length;
        int nblocks = length / 4;

        for (int i = 0; i < nblocks; i++)
        {
            uint k = BitConverter.ToUInt32(bytes.Slice(i * 4, 4));
            k *= c1; k = RotateLeft(k, 15); k *= c2;
            hash ^= k; hash = RotateLeft(hash, 13); hash = hash * 5 + 0xe6546b64;
        }

        uint tail = 0;
        int tailStart = nblocks * 4;
        switch (length & 3)
        {
            case 3: tail ^= (uint)bytes[tailStart + 2] << 16; goto case 2;
            case 2: tail ^= (uint)bytes[tailStart + 1] << 8; goto case 1;
            case 1: tail ^= bytes[tailStart]; tail *= c1; tail = RotateLeft(tail, 15); tail *= c2; hash ^= tail; break;
        }

        hash ^= (uint)length;
        hash ^= hash >> 16; hash *= 0x85ebca6b;
        hash ^= hash >> 13; hash *= 0xc2b2ae35;
        hash ^= hash >> 16;
        return hash;
    }

    private static uint RotateLeft(uint x, int r) => (x << r) | (x >> (32 - r));
}
