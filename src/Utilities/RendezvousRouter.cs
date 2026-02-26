using System.Collections.Concurrent;
using System.Net;
using System.Text;
using Google.Protobuf.WellKnownTypes;

namespace Cross.Utilities;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing.
/// Hashes by STABLE NODE NAMES (not pod IPs).
/// Pod IPs change on restart → routing breaks.
/// Node names are immutable → routing is stable across pod restarts.
///
/// Flow:
/// 1. DNS resolve → get pod IPs (from headless service, only ready pods)
/// 2. Call GetNodeInfo on each pod IP → get node_name
/// 3. Hash: MurmurHash3(bucket + nodeName) → deterministic routing
/// 4. Connect: nodeName → podIP mapping → gRPC connection
///
/// New node joins: Rendezvous Hashing redistributes ~1/N buckets.
/// Old chunks stay on old nodes → v3.0.0 fallback finds them.
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();

    // ── Stable routing identity (node names don't change across pod restarts) ──
    private static volatile string[] _nodeNames = Array.Empty<string>();
    private static volatile byte[][] _nodeNameBytes = Array.Empty<byte[]>();

    // ── Connection mapping (pod IPs may change, this gets refreshed) ──
    private static volatile Dictionary<string, string> _nodeToIp = new();

    // ── All current pod IPs (for GetAllAgentIps fallback) ──
    private static volatile string[] _agentIps = Array.Empty<string>();

    private static long _resolvedAtTicks = 0;

    /// <summary>
    /// Pick the owning agent for a bucket. Returns a pod IP for gRPC connection.
    /// Deterministic: same (bucket, node set) → same agent. Stable across pod restarts.
    /// Zero allocations in hot path — stackalloc + MurmurHash3.
    /// </summary>
    public static string PickAgent(string bucketString)
    {
        var nodeNames = _nodeNames;

        // ── CRITICAL: Always check cache staleness, not just empty state. ──
        // BUG FIX: Previously only refreshed when _nodeNames.Length == 0.
        // If initial DNS resolved only 1 agent (others not ready yet), PickAgent
        // would PERMANENTLY route everything to that single agent — never re-resolving.
        // DateTime.UtcNow.Ticks + Interlocked.Read ≈ 5ns — negligible even at 4M calls.
        long resolvedTicks = Interlocked.Read(ref _resolvedAtTicks);
        bool stale = (DateTime.UtcNow.Ticks - resolvedTicks) >= TimeSpan.FromSeconds(15).Ticks;
        if (nodeNames.Length == 0 || stale)
        {
            GetAgents();
            nodeNames = _nodeNames;
            if (nodeNames.Length == 0) return Globals.AgentsLoadbalancer;
        }
        if (nodeNames.Length == 1)
        {
            var mapping = _nodeToIp;
            return mapping.TryGetValue(nodeNames[0], out var singleIp) ? singleIp : Globals.AgentsLoadbalancer;
        }

        var nodeNameBytesLocal = _nodeNameBytes;
        int bucketLen = bucketString.Length;
        Span<byte> buf = stackalloc byte[192]; // bucket(64) + "|"(1) + node name(up to ~63)

        for (int c = 0; c < bucketLen; c++)
            buf[c] = (byte)bucketString[c];
        buf[bucketLen] = (byte)'|';

        string bestNode = nodeNames[0];
        uint bestHash = 0;

        for (int i = 0; i < nodeNames.Length; i++)
        {
            var nb = nodeNameBytesLocal[i];
            nb.AsSpan().CopyTo(buf.Slice(bucketLen + 1));
            int totalLen = bucketLen + 1 + nb.Length;

            uint hash = MurmurHash3(buf.Slice(0, totalLen));
            if (hash > bestHash)
            {
                bestHash = hash;
                bestNode = nodeNames[i];
            }
        }

        var nodeMapping = _nodeToIp;
        return nodeMapping.TryGetValue(bestNode, out var agentIp) ? agentIp : Globals.AgentsLoadbalancer;
    }

    /// <summary>
    /// Resolve agents: DNS → pod IPs → GetNodeInfo → node names.
    /// Cached 15s. Node names are stable; only IP mappings change on refresh.
    /// </summary>
    public static string[] GetAgents()
    {
        long resolvedTicks = Interlocked.Read(ref _resolvedAtTicks);
        if (_nodeNames.Length > 0 && (DateTime.UtcNow.Ticks - resolvedTicks) < TimeSpan.FromSeconds(15).Ticks)
            return _agentIps;

        lock (_lock)
        {
            if (_nodeNames.Length > 0 && (DateTime.UtcNow.Ticks - _resolvedAtTicks) < TimeSpan.FromSeconds(15).Ticks)
                return _agentIps;

            try
            {
                // 1. DNS resolve → pod IPs (only ready pods appear in headless service DNS)
                var resolvedIps = Dns.GetHostAddresses(Globals.AgentsLoadbalancer)
                    .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    .Select(ip => ip.ToString())
                    .Distinct()
                    .ToArray();

                if (resolvedIps.Length == 0)
                    return _agentIps.Length > 0 ? _agentIps : new[] { Globals.AgentsLoadbalancer };

                // 2. Call GetNodeInfo on each pod IP → get node_name
                var newNodeToIp = new Dictionary<string, string>();
                var tasks = resolvedIps.Select(async ip =>
                {
                    try
                    {
                        var client = GrpcChannelFactory.GetClient(
                            target: ip,
                            ctor: chan => new GetNodeInfo.GetNodeInfoClient(chan),
                            roundRobin: false,
                            port: 5000);

                        var res = await client.GetAsync(new Empty(),
                            deadline: DateTime.UtcNow.AddSeconds(3));

                        string nodeName = res.NodeName;
                        if (!string.IsNullOrWhiteSpace(nodeName))
                            return (nodeName, ip, ok: true);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"[RendezvousRouter] GetNodeInfo failed for {ip}: {ex.Message}");
                    }
                    return (nodeName: "", ip, ok: false);
                }).ToArray();

                Task.WaitAll(tasks, TimeSpan.FromSeconds(5));

                foreach (var t in tasks)
                {
                    if (t.IsCompletedSuccessfully && t.Result.ok)
                    {
                        // If two pods report the same node name (shouldn't happen with DaemonSet),
                        // last one wins. This is safe — the IP just gets updated.
                        newNodeToIp[t.Result.nodeName] = t.Result.ip;
                    }
                }

                if (newNodeToIp.Count == 0)
                {
                    // GetNodeInfo failed for ALL agents — fall back to IP-based routing
                    // This keeps the system working even if agents haven't been updated yet
                    Console.WriteLine($"[RendezvousRouter] WARNING: GetNodeInfo failed for all {resolvedIps.Length} agents. Falling back to IP-based routing.");
                    var sortedIps = resolvedIps.OrderBy(x => x, StringComparer.Ordinal).ToArray();
                    _nodeNames = sortedIps;
                    _nodeNameBytes = sortedIps.Select(a => Encoding.UTF8.GetBytes(a)).ToArray();
                    _nodeToIp = sortedIps.ToDictionary(ip => ip, ip => ip);
                    _agentIps = sortedIps;
                    Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);
                    Console.WriteLine($"[RendezvousRouter] Fallback: {sortedIps.Length} agents by IP: [{string.Join(", ", sortedIps)}]");
                    return _agentIps;
                }

                // 3. Build sorted node names (deterministic order across all Cross/Gateway pods)
                var sortedNodeNames = newNodeToIp.Keys.OrderBy(x => x, StringComparer.Ordinal).ToArray();
                bool changed = !sortedNodeNames.SequenceEqual(_nodeNames);

                _nodeNameBytes = sortedNodeNames.Select(n => Encoding.UTF8.GetBytes(n)).ToArray();
                _nodeNames = sortedNodeNames;
                _nodeToIp = newNodeToIp;
                _agentIps = newNodeToIp.Values.ToArray();
                Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);

                if (changed)
                {
                    var info = string.Join(", ", sortedNodeNames.Select(n => $"{n}={newNodeToIp[n]}"));
                    Console.WriteLine($"[RendezvousRouter] Resolved {sortedNodeNames.Length} agents by node name: [{info}]");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RendezvousRouter] DNS resolve failed: {ex.Message}");
            }

            return _agentIps.Length > 0 ? _agentIps : new[] { Globals.AgentsLoadbalancer };
        }
    }

    /// <summary>
    /// Return all current agent pod IPs. Used for v3.0.0 safe multi-agent fallback.
    /// </summary>
    public static string[] GetAllAgentIps()
    {
        var agents = _agentIps;
        if (agents.Length == 0)
        {
            GetAgents();
            agents = _agentIps;
        }
        return agents;
    }

    /// <summary>
    /// Compute neighbouring buckets (1 bit-flip radius).
    /// </summary>
    public static List<string> GetNeighbouringBuckets(string bitString, float[]? lshVector = null)
    {
        var results = new List<string>(65);
        char[] buffer = bitString.ToCharArray();
        results.Add(bitString);

        if (lshVector != null && lshVector.Length == bitString.Length)
        {
            var indices = new int[bitString.Length];
            var confidences = new float[bitString.Length];
            for (int i = 0; i < bitString.Length; i++)
            {
                indices[i] = i;
                confidences[i] = Math.Abs(lshVector[i]);
            }
            Array.Sort(confidences, indices);

            foreach (var bitIndex in indices)
            {
                char original = buffer[bitIndex];
                buffer[bitIndex] = (original == '0') ? '1' : '0';
                results.Add(new string(buffer));
                buffer[bitIndex] = original;
            }
        }
        else
        {
            for (int i = 0; i < buffer.Length; i++)
            {
                char original = buffer[i];
                buffer[i] = (original == '0') ? '1' : '0';
                results.Add(new string(buffer));
                buffer[i] = original;
            }
        }

        return results;
    }

    private static uint MurmurHash3(ReadOnlySpan<byte> bytes)
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
