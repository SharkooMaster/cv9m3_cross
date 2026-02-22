using System.Collections.Concurrent;
using System.Net;
using System.Text;

namespace Cross.Utilities;

/// <summary>
/// Rendezvous (Highest Random Weight) hashing.
/// Hash by sorted agent IPs from DNS. DNS is consistent across all Cross pods
/// → identical routing → no cross-pod inconsistency.
/// Zero heap allocations in the hot path (PickAgent).
/// </summary>
public static class RendezvousRouter
{
    private static readonly object _lock = new();
    private static volatile string[] _agents = Array.Empty<string>();
    private static volatile byte[][] _agentBytes = Array.Empty<byte[]>();
    private static long _resolvedAtTicks = 0;

    /// <summary>
    /// Pick the owning agent for a bucket. Deterministic: same (bucket, agent set) → same agent.
    /// Zero allocations — stackalloc + MurmurHash3.
    /// </summary>
    public static string PickAgent(string bucketString)
    {
        var agents = _agents;
        if (agents.Length == 0)
        {
            GetAgents();
            agents = _agents;
            if (agents.Length == 0) return Globals.AgentsLoadbalancer;
        }
        if (agents.Length == 1) return agents[0];

        var agentBytesLocal = _agentBytes;
        int bucketLen = bucketString.Length;
        Span<byte> buf = stackalloc byte[128]; // bucket(64) + "|"(1) + IP(max ~15) = ~80

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
    /// Resolve agents from DNS. Cached 60s. Returns sorted IP list.
    /// </summary>
    public static string[] GetAgents()
    {
        long resolvedTicks = Interlocked.Read(ref _resolvedAtTicks);
        if (_agents.Length > 0 && (DateTime.UtcNow.Ticks - resolvedTicks) < TimeSpan.FromSeconds(60).Ticks)
            return _agents;

        lock (_lock)
        {
            if (_agents.Length > 0 && (DateTime.UtcNow.Ticks - _resolvedAtTicks) < TimeSpan.FromSeconds(60).Ticks)
                return _agents;

            try
            {
                var resolved = Dns.GetHostAddresses(Globals.AgentsLoadbalancer)
                    .Where(ip => ip.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                    .Select(ip => ip.ToString())
                    .Distinct()
                    .OrderBy(x => x, StringComparer.Ordinal)
                    .ToArray();

                if (resolved.Length > 0)
                {
                    bool changed = !resolved.SequenceEqual(_agents);
                    _agentBytes = resolved.Select(a => Encoding.UTF8.GetBytes(a)).ToArray();
                    _agents = resolved;
                    Interlocked.Exchange(ref _resolvedAtTicks, DateTime.UtcNow.Ticks);

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
    /// Return the current list of all agent IPs. Used for safe fallback during v3.0.0 decompression.
    /// </summary>
    public static string[] GetAllAgentIps()
    {
        var agents = _agents;
        if (agents.Length == 0)
        {
            GetAgents();
            agents = _agents;
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
