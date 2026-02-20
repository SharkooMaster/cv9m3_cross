using System.Net;
using System.Text;
using System.Threading;

namespace Cross.Utilities;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing — same algorithm as Gateway.
/// Cross uses this to route directly to agents, bypassing the gateway in the hot path.
///
/// Performance: PickAgent is called ~290,000 times per 4000-chunk file (65 buckets × N chunks).
/// This implementation uses zero heap allocations in the hot path (stackalloc + Span).
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();
    private static volatile string[] _agents = Array.Empty<string>();
    private static volatile byte[][] _agentBytes = Array.Empty<byte[]>(); // Pre-encoded IPs
    private static long _resolvedAtTicks = 0; // DateTime.UtcNow.Ticks — value type safe

    /// <summary>
    /// Pick the owning agent for a bucket using rendezvous hashing.
    /// Deterministic: same (bucket, agent set) always returns the same agent.
    /// ZERO heap allocations — uses stackalloc for the hash buffer.
    /// </summary>
    public static string PickAgent(string bucketString)
    {
        var agents = GetAgents();
        if (agents.Length == 0) return Globals.AgentsLoadbalancer;
        if (agents.Length == 1) return agents[0];

        var agentBytesLocal = _agentBytes;

        // Bucket strings are always 64 ASCII chars. Agent IPs max ~15 chars.
        // Buffer: bucket(64) + "|"(1) + agent(max 20) = 85 bytes max. Use 96 for safety.
        int bucketLen = bucketString.Length;
        Span<byte> buf = stackalloc byte[96];

        // Encode bucket string (ASCII only — '0' and '1')
        for (int c = 0; c < bucketLen; c++)
            buf[c] = (byte)bucketString[c];
        buf[bucketLen] = (byte)'|';

        string bestAgent = agents[0];
        uint bestHash = 0;

        for (int i = 0; i < agents.Length; i++)
        {
            var ab = agentBytesLocal[i];
            ab.AsSpan().CopyTo(buf.Slice(bucketLen + 1));
            int totalLen = bucketLen + 1 + ab.Length;

            uint hash = MurmurHash3(buf.Slice(0, totalLen));
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
    /// Double-checked locking: only ONE thread resolves DNS on cache miss.
    /// Logs only on actual change, not on every cache refresh.
    /// </summary>
    public static string[] GetAgents()
    {
        // Fast path: lock-free cache check
        var agents = _agents;
        long resolvedTicks = Interlocked.Read(ref _resolvedAtTicks);
        if (agents.Length > 0 && (DateTime.UtcNow.Ticks - resolvedTicks) < TimeSpan.FromSeconds(15).Ticks)
            return agents;

        // Slow path: resolve DNS (only one thread at a time)
        lock (_lock)
        {
            // Double-check inside lock (another thread may have resolved already)
            if (_agents.Length > 0 && (DateTime.UtcNow.Ticks - _resolvedAtTicks) < TimeSpan.FromSeconds(15).Ticks)
                return _agents;

            try
            {
                var resolved = Dns.GetHostAddresses(Globals.AgentsLoadbalancer)
                    .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    .Select(ip => ip.ToString())
                    .Distinct()
                    .OrderBy(x => x)
                    .ToArray();

                if (resolved.Length > 0)
                {
                    bool changed = !resolved.SequenceEqual(_agents);
                    _agentBytes = resolved.Select(a => Encoding.UTF8.GetBytes(a)).ToArray();
                    _agents = resolved;
                    Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);

                    // Only log on actual topology change, not every 15s refresh
                    if (changed)
                        Console.WriteLine($"[RendezvousRouter] Resolved {resolved.Length} agents: [{string.Join(", ", resolved)}]");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[RendezvousRouter] DNS resolve failed: {ex.Message}");
            }

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
