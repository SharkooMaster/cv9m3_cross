using System.Net;
using System.Text;

namespace Cross.Utilities;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing — same implementation as Gateway.
/// Cross uses this to route directly to agents, bypassing the gateway in the hot path.
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();
    private static string[] _agents = Array.Empty<string>();
    private static DateTime _resolvedAt = DateTime.MinValue;

    /// <summary>
    /// Pick the owning agent for a bucket using rendezvous hashing.
    /// Deterministic: same (bucket, agent set) always returns the same agent.
    /// </summary>
    public static string PickAgent(string bucketString)
    {
        var agents = GetAgents();
        if (agents.Length == 0) return Globals.AgentsLoadbalancer;
        if (agents.Length == 1) return agents[0];

        string bestAgent = agents[0];
        uint bestHash = 0;
        for (int i = 0; i < agents.Length; i++)
        {
            uint hash = MurmurHash3($"{bucketString}|{agents[i]}");
            if (hash > bestHash)
            {
                bestHash = hash;
                bestAgent = agents[i];
            }
        }
        return bestAgent;
    }

    /// <summary>
    /// Resolve agent IPs from the headless K8s service. Cached 15s.
    /// Sorted for deterministic ordering across all callers (same as gateway).
    /// </summary>
    public static string[] GetAgents()
    {
        lock (_lock)
        {
            if (_agents.Length > 0 && DateTime.UtcNow - _resolvedAt < TimeSpan.FromSeconds(15))
                return _agents;
        }

        try
        {
            var resolved = Dns.GetHostAddresses(Globals.AgentsLoadbalancer)
                .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                .Select(ip => ip.ToString())
                .Distinct()
                .OrderBy(x => x) // Stable order across all callers
                .ToArray();

            if (resolved.Length > 0)
            {
                lock (_lock)
                {
                    _agents = resolved;
                    _resolvedAt = DateTime.UtcNow;
                    Console.WriteLine($"[RendezvousRouter] Resolved {resolved.Length} agents: [{string.Join(", ", resolved)}]");
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[RendezvousRouter] DNS resolve failed: {ex.Message} — keeping previous {_agents.Length} agents");
        }

        lock (_lock)
        {
            return _agents.Length > 0 ? _agents : new[] { Globals.AgentsLoadbalancer };
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
    /// MurmurHash3 32-bit — identical to gateway's implementation.
    /// </summary>
    private static uint MurmurHash3(string key)
    {
        var bytes = Encoding.UTF8.GetBytes(key);
        const uint seed = 0x9747b28c;
        const uint c1 = 0xcc9e2d51;
        const uint c2 = 0x1b873593;

        uint hash = seed;
        int length = bytes.Length;
        int nblocks = length / 4;

        for (int i = 0; i < nblocks; i++)
        {
            uint k = BitConverter.ToUInt32(bytes, i * 4);
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
