using Grpc.Core;
using Cross.Routing;
using Cross.Utilities;

namespace Cross.Services.Grpc.Agent;

public class ChunkReferenceServiceClient
{
    /// <summary>
    /// Resolve the R replicas that should hold a chunk, keyed by its
    /// bucketId (same key <see cref="Cross.Services.Cross.ReplicatedBatchStore"/>
    /// uses on the write side). The optional <paramref name="primaryHint"/>
    /// is moved to the front of the candidate list — useful when gateway's
    /// search response told us which agent had a known-fresh copy.
    ///
    /// When <paramref name="bucketId"/> is 0 (caller has no bucket info,
    /// only a storage_guid) we fall back to "try the hint, then any agents
    /// the rendezvous router knows about" — slower but still correct
    /// because chunk lookup is content-addressable.
    /// </summary>
    private static IReadOnlyList<string> ResolveReplicasByBucketId(ulong bucketId, string? primaryHint)
    {
        IReadOnlyList<string> ring;
        if (bucketId != 0)
        {
            // Use the same routing key the writer used (Phase 2). Decoder
            // and encoder converge on the same R replicas.
            string routingKey = BucketRoutingKey.FromBucketId(bucketId);
            ring = ReplicaResolver.PickReplicaIps(routingKey);
        }
        else
        {
            // No bucketId means the caller (e.g. legacy CCF v3.0.0 path)
            // has only a storage_guid. Reads are content-addressable, so
            // it's safe — but slower — to try every agent the ring knows
            // about until one returns the chunk.
            var ringSnapshot = RingState.Current;
            if (ringSnapshot.Agents.Count > 0)
            {
                var ips = new List<string>(ringSnapshot.Agents.Count);
                foreach (var name in ringSnapshot.Agents)
                {
                    var ip = ringSnapshot.GetIp(name);
                    if (!string.IsNullOrEmpty(ip)) ips.Add(ip);
                }
                ring = ips;
            }
            else
            {
                ring = Array.Empty<string>();
            }
        }

        if (ring.Count == 0 && !string.IsNullOrWhiteSpace(primaryHint))
            return new[] { primaryHint! };

        if (string.IsNullOrWhiteSpace(primaryHint)) return ring;

        if (ring.Count > 0 && string.Equals(ring[0], primaryHint, StringComparison.Ordinal))
            return ring;

        var ordered = new List<string>(ring.Count + 1) { primaryHint! };
        foreach (var ip in ring)
            if (!string.Equals(ip, primaryHint, StringComparison.Ordinal))
                ordered.Add(ip);
        return ordered;
    }
    /// <summary>
    /// Fetch a chunk by (bucketId, bucketIndex) — used for v2.0.0/v2.1.0 compressed files.
    /// The agent looks up storageGuid via bucket metadata, then fetches the chunk bytes.
    ///
    /// Replica fallback: bucketId is the consistent-hash routing key, so
    /// the same R replicas the writer used will own the bucket index.
    /// We try each replica in priority order (hint first) until one
    /// returns the chunk.
    /// </summary>
    public async Task<byte[]?> GetChunkByReferenceAsync(
        ulong bucketId,
        ulong bucketIndex,
        string? targetAgent = null,
        CancellationToken ct = default)
    {
        var replicas = ResolveReplicasByBucketId(bucketId, targetAgent);
        if (replicas.Count == 0) return null;

        int perReplicaMs = Math.Max(5_000, 30_000 / Math.Max(1, replicas.Count));
        Exception? lastEx = null;

        for (int attempt = 0; attempt < replicas.Count; attempt++)
        {
            string agentTarget = replicas[attempt];
            if (string.IsNullOrWhiteSpace(agentTarget)) continue;

            try
            {
                var client = GrpcChannelFactory.GetClient(
                    target: agentTarget,
                    ctor: chan => new ChunkReferenceService.ChunkReferenceServiceClient(chan),
                    roundRobin: LocalModeDetector.IsLocalMode(),
                    port: 5000
                );

                var req = new GetChunkByReference_Req
                {
                    BucketId = bucketId,
                    BucketIndex = bucketIndex
                };

                var res = await client.GetChunkByReferenceAsync(
                    req,
                    deadline: DateTime.UtcNow.AddMilliseconds(perReplicaMs),
                    cancellationToken: ct);

                if (res.Found && res.Chunk != null && res.Chunk.Length > 0)
                {
                    return res.Chunk.ToByteArray();
                }
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
            {
                return null;
            }
            catch (RpcException ex)
            {
                GrpcChannelFactory.EvictOnFailure(agentTarget, ex);
                lastEx = ex;
                Console.WriteLine($"[ChunkReferenceServiceClient] replica {agentTarget} failed: {ex.Status.StatusCode} - {ex.Status.Detail}");
            }
            catch (Exception ex)
            {
                lastEx = ex;
                Console.WriteLine($"[ChunkReferenceServiceClient] replica {agentTarget} error: {ex.Message}");
            }
        }

        if (lastEx != null)
            Console.WriteLine($"[ChunkReferenceServiceClient] all {replicas.Count} replicas failed for bucket {bucketId}/{bucketIndex}");
        return null;
    }

    /// <summary>
    /// Fetch a chunk by storageGuid (SHA256 content hash) — used for v3.0.0+ compressed files.
    /// O(1) direct lookup. Content-addressable: any agent with this GUID has the CORRECT data.
    ///
    /// Replica fallback: with R=3 replication every chunk lives on multiple
    /// agents. We resolve the replica set from <see cref="ReplicaResolver"/>
    /// (using storageGuid as the ring key, the same key writes used) and
    /// try each replica in order. The first agent that returns the chunk
    /// wins. Per-replica timeouts are short so a single-agent stall doesn't
    /// hold up the read — by the time we walk through R agents, the total
    /// outer deadline (30s) is still respected.
    /// </summary>
    public async Task<byte[]?> GetChunkByStorageGuidAsync(
        string storageGuid,
        string? targetAgent = null,
        CancellationToken ct = default,
        ulong bucketIdHint = 0)
    {
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;

        var replicas = ResolveReplicasByBucketId(bucketIdHint, targetAgent);
        if (replicas.Count == 0)
        {
            // No ring, no hint, nothing to talk to. The caller treats
            // null as "chunk not available" → falls back to fresh-store.
            return null;
        }

        // Per-replica deadline scales with how many replicas we have to
        // try, so the worst case (every replica down except one) doesn't
        // burst past the outer 30s budget. Floor at 5s so a single-agent
        // ring still gets a reasonable wait.
        int perReplicaMs = Math.Max(5_000, 30_000 / Math.Max(1, replicas.Count));

        Exception? lastEx = null;
        for (int attempt = 0; attempt < replicas.Count; attempt++)
        {
            string agentTarget = replicas[attempt];
            if (string.IsNullOrWhiteSpace(agentTarget)) continue;

            try
            {
                var client = GrpcChannelFactory.GetClient(
                    target: agentTarget,
                    ctor: chan => new ChunkReferenceService.ChunkReferenceServiceClient(chan),
                    roundRobin: LocalModeDetector.IsLocalMode(),
                    port: 5000
                );

                var req = new GetChunkByKey_Req { ChunkKey = storageGuid };

                var res = await client.GetChunkByKeyAsync(
                    req,
                    deadline: DateTime.UtcNow.AddMilliseconds(perReplicaMs),
                    cancellationToken: ct);

                if (res.Found && res.Chunk != null && res.Chunk.Length > 0)
                {
                    return res.Chunk.ToByteArray();
                }

                // Found=false: this replica doesn't have it (e.g. mid-
                // rebalance, or a write that hadn't propagated when the
                // ring rebuilt). Try the next replica.
            }
            catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled && ct.IsCancellationRequested)
            {
                return null;
            }
            catch (RpcException ex)
            {
                // Drop the cached direct-IP channel on transport failure
                // so the next replica's call doesn't reuse a broken one.
                GrpcChannelFactory.EvictOnFailure(agentTarget, ex);
                lastEx = ex;
                Console.WriteLine($"[ChunkReferenceServiceClient] replica {agentTarget} failed: {ex.Status.StatusCode} - {ex.Status.Detail}");
            }
            catch (Exception ex)
            {
                lastEx = ex;
                Console.WriteLine($"[ChunkReferenceServiceClient] replica {agentTarget} error: {ex.Message}");
            }
        }

        // All replicas tried, none had it. Most common cause is the
        // chunk was never stored (caller's storage_guid is wrong) or the
        // R replicas all happened to be down — the latter is rare with
        // R=3 and triggers anti-entropy on the next pass.
        if (lastEx != null)
        {
            Console.WriteLine($"[ChunkReferenceServiceClient] all {replicas.Count} replicas failed for {storageGuid[..Math.Min(8, storageGuid.Length)]}…");
        }
        return null;
    }
}
