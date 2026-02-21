using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Threading;
using Google.Protobuf.WellKnownTypes;

namespace Cross.Utilities;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing — uses STABLE Kubernetes node names
/// for the hash key, NOT ephemeral pod IPs.
///
/// Why: DaemonSet agent pods get new IPs on every restart / helm upgrade.
/// If we hash against IPs, routing reshuffles and compressed-file references
/// resolve to the WRONG agent (which may have a different chunk at the same
/// (bucketId, bucketIndex)), causing silent data corruption.
///
/// Node names (spec.nodeName) never change. Same node → same data (hostPath).
/// Hash by node name → routing is stable → no corruption.
///
/// Performance: PickAgent is called ~290,000 times per 4000-chunk file.
/// This implementation uses zero heap allocations in the hot path (stackalloc + Span).
/// Discovery (GetNodeInfo calls) happens once every 15 seconds in the background.
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();

    // ── Stable routing data ──
    // Hash keys: node names (stable across pod restarts)
    // Connection targets: pod IPs (current, ephemeral)
    private static volatile string[] _nodeNames = Array.Empty<string>();       // sorted, for deterministic hashing
    private static volatile byte[][] _nodeNameBytes = Array.Empty<byte[]>();   // pre-encoded for zero-alloc hash
    private static volatile string[] _nodeIps = Array.Empty<string>();         // parallel array: nodeNames[i] → nodeIps[i]
    private static long _resolvedAtTicks = 0;

    /// <summary>
    /// Pick the owning agent for a bucket using rendezvous hashing.
    /// Deterministic: same (bucket, node set) always returns the same agent IP.
    /// ZERO heap allocations — uses stackalloc for the hash buffer.
    /// Returns the pod IP of the agent to connect to.
    /// </summary>
    public static string PickAgent(string bucketString)
    {
        var nodeNames = _nodeNames;
        var nodeIps = _nodeIps;

        if (nodeNames.Length == 0)
        {
            // Not yet discovered — trigger discovery and fall back to load balancer
            GetAgents();
            nodeNames = _nodeNames;
            nodeIps = _nodeIps;
            if (nodeNames.Length == 0) return Globals.AgentsLoadbalancer;
        }
        if (nodeNames.Length == 1) return nodeIps[0];

        var nodeNameBytesLocal = _nodeNameBytes;

        // Node names are typically 5-63 chars. Buffer: bucket(64) + "|"(1) + nodeName(max 63) = 128. Use 160 for safety.
        int bucketLen = bucketString.Length;
        Span<byte> buf = stackalloc byte[160];

        // Encode bucket string (ASCII only — '0' and '1')
        for (int c = 0; c < bucketLen; c++)
            buf[c] = (byte)bucketString[c];
        buf[bucketLen] = (byte)'|';

        string bestIp = nodeIps[0];
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
                bestIp = nodeIps[i];
            }
        }
        return bestIp;
    }

    /// <summary>
    /// Resolve agents: DNS → pod IPs → GetNodeInfo gRPC → node names.
    /// Cached 15s. Only ONE thread resolves at a time.
    /// Returns the current pod IPs (for callers that need them).
    /// </summary>
    public static string[] GetAgents()
    {
        // Fast path: lock-free cache check
        long resolvedTicks = Interlocked.Read(ref _resolvedAtTicks);
        if (_nodeNames.Length > 0 && (DateTime.UtcNow.Ticks - resolvedTicks) < TimeSpan.FromSeconds(15).Ticks)
            return _nodeIps;

        // Slow path: resolve DNS + GetNodeInfo (only one thread)
        lock (_lock)
        {
            if (_nodeNames.Length > 0 && (DateTime.UtcNow.Ticks - _resolvedAtTicks) < TimeSpan.FromSeconds(15).Ticks)
                return _nodeIps;

            try
            {
                var podIps = Dns.GetHostAddresses(Globals.AgentsLoadbalancer)
                    .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    .Select(ip => ip.ToString())
                    .Distinct()
                    .OrderBy(x => x)
                    .ToArray();

                if (podIps.Length == 0)
                {
                    Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);
                    return _nodeIps.Length > 0 ? _nodeIps : new[] { Globals.AgentsLoadbalancer };
                }

                // Call GetNodeInfo on each agent to get stable node names
                var entries = new List<(string nodeName, string podIp)>(podIps.Length);
                var tasks = podIps.Select(async ip =>
                {
                    try
                    {
                        var client = GrpcChannelFactory.GetClient(
                            target: ip,
                            ctor: chan => new GetNodeInfo.GetNodeInfoClient(chan),
                            roundRobin: false,
                            port: 5000);

                        var res = await client.GetAsync(
                            new Empty(),
                            deadline: DateTime.UtcNow.AddSeconds(3));

                        string nodeName = res.NodeName;
                        // Fallback: if agent hasn't been updated yet, use IP as hash key
                        if (string.IsNullOrWhiteSpace(nodeName))
                            nodeName = ip;

                        return (nodeName, ip);
                    }
                    catch
                    {
                        // Agent unreachable — use IP as hash key (backward compat)
                        return (ip, ip);
                    }
                }).ToArray();

                Task.WaitAll(tasks);
                foreach (var t in tasks)
                    entries.Add(t.Result);

                // Sort by node name for deterministic ordering
                entries.Sort((a, b) => string.Compare(a.nodeName, b.nodeName, StringComparison.Ordinal));

                var newNodeNames = entries.Select(e => e.nodeName).ToArray();
                var newNodeIps = entries.Select(e => e.podIp).ToArray();
                var newNodeNameBytes = newNodeNames.Select(n => Encoding.UTF8.GetBytes(n)).ToArray();

                bool changed = !newNodeNames.SequenceEqual(_nodeNames) || !newNodeIps.SequenceEqual(_nodeIps);

                _nodeNameBytes = newNodeNameBytes;
                _nodeNames = newNodeNames;
                _nodeIps = newNodeIps;
                Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);

                if (changed)
                {
                    var pairs = entries.Select(e => e.nodeName == e.podIp
                        ? e.podIp
                        : $"{e.nodeName}={e.podIp}");
                    Console.WriteLine($"[RendezvousRouter] Resolved {entries.Count} agents: [{string.Join(", ", pairs)}]");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RendezvousRouter] DNS resolve failed: {ex.Message}");
            }

            return _nodeIps.Length > 0 ? _nodeIps : new[] { Globals.AgentsLoadbalancer };
        }
    }

    /// <summary>
    /// Compute neighbouring buckets (1 bit-flip radius).
    /// Same logic as gateway's GetNeighbouringBuckets.
    /// </summary>
    public static List<string> GetNeighbouringBuckets(string bitString, float[]? lshVector = null)
    {
        var results = new List<string>(65); // 1 original + 64 neighbors
        char[] buffer = bitString.ToCharArray();

        // Always include the original bucket first
        results.Add(bitString);

        if (lshVector != null && lshVector.Length == bitString.Length)
        {
            // Sort bits by confidence (lowest |sum| = most uncertain = flip first)
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

    /// <summary>
    /// MurmurHash3 32-bit — Span overload, zero allocation.
    /// Identical output to the string version (same bytes → same hash).
    /// </summary>
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
            k *= c1;
            k = RotateLeft(k, 15);
            k *= c2;
            hash ^= k;
            hash = RotateLeft(hash, 13);
            hash = hash * 5 + 0xe6546b64;
        }

        uint tail = 0;
        int tailStart = nblocks * 4;
        switch (length & 3)
        {
            case 3: tail ^= (uint)bytes[tailStart + 2] << 16; goto case 2;
            case 2: tail ^= (uint)bytes[tailStart + 1] << 8; goto case 1;
            case 1:
                tail ^= bytes[tailStart];
                tail *= c1;
                tail = RotateLeft(tail, 15);
                tail *= c2;
                hash ^= tail;
                break;
        }

        hash ^= (uint)length;
        hash ^= hash >> 16;
        hash *= 0x85ebca6b;
        hash ^= hash >> 13;
        hash *= 0xc2b2ae35;
        hash ^= hash >> 16;
        return hash;
    }

    private static uint RotateLeft(uint x, int r) => (x << r) | (x >> (32 - r));
}
