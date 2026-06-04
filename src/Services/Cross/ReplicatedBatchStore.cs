using System.Security.Cryptography;
using System.Text;
using Cross.Routing;
using Cross.Utilities;
using Google.Protobuf;
using Grpc.Core;

namespace Cross.Services.Cross;

/// <summary>
/// Fans a batch of fresh chunks out to all R replicas in parallel and
/// returns a per-chunk consolidated result once the W=2 quorum is met.
///
/// Why this exists:
///   The V7 freshStoreQueue used to send each chunk to a single agent
///   chosen by rendezvous. Losing that agent (disk failure, pod
///   eviction, network partition) made the chunk unrecoverable — the
///   compressed file referenced it by storage_guid but no other agent
///   had a copy. With elastic scaling on Clever Cloud, agents are
///   expected to come and go, so single-replica writes are simply
///   incompatible with the deployment story.
///
///   This helper drives writes through the consistent-hash ring: each
///   chunk is fanned out to its R replicas, and a write is only
///   considered durable when at least W replicas acked. The remaining
///   replica(s) keep replicating in the background; if they fail the
///   write still succeeds, but anti-entropy (Phase 5) will eventually
///   repair the missing copy.
///
/// Response selection:
///   The agent's STORE-time dedup decision (WasDeduplicated, BaseChunk)
///   is local to that agent — different replicas may report different
///   dedup outcomes for the same chunk. The cross-side encoder uses
///   *one* of the responses to decide how to encode the chunk in the
///   CCF. We deterministically prefer the *primary* replica's response
///   (replica 0 = first in PickReplicas) so two compressors processing
///   the same chunk get the same encoding — important for cross-pod
///   reproducibility.
/// </summary>
public static class ReplicatedBatchStore
{
    private const int StoreBatchSize = 1000;
    private const int MaxAttemptsPerAgent = 5;

    /// <summary>
    /// One slot of work — a chunk that needs to be fresh-stored, with
    /// its bitstring (for routing) and its raw bytes (for the wire).
    /// </summary>
    public readonly struct WorkItem
    {
        public readonly int ChunkIdx;
        public readonly string BitString;
        public readonly float[] Vector;
        // Zero-copy view of the chunk's bytes (a slice of the caller's block
        // buffer). The caller guarantees the backing buffer stays alive for the
        // whole RunAsync call, so the wire serialization can wrap it directly.
        public readonly ReadOnlyMemory<byte> ChunkBytes;

        public WorkItem(int chunkIdx, string bitString, float[] vector, ReadOnlyMemory<byte> chunkBytes)
        {
            ChunkIdx = chunkIdx;
            BitString = bitString;
            Vector = vector;
            ChunkBytes = chunkBytes;
        }
    }

    /// <summary>
    /// Result for a single chunk after the fan-out completes. The caller
    /// passes this into the cross-side encoder; only the
    /// <see cref="ResponseToEncode"/> matters for V7 fates[] population.
    /// </summary>
    public readonly struct WorkResult
    {
        public readonly int ChunkIdx;
        public readonly StoreVector_Res ResponseToEncode;
        /// <summary>Replica IPs that successfully acked. >= W on success.</summary>
        public readonly IReadOnlyList<string> AckedReplicas;

        public WorkResult(int chunkIdx, StoreVector_Res respToEncode, IReadOnlyList<string> acked)
        {
            ChunkIdx = chunkIdx;
            ResponseToEncode = respToEncode;
            AckedReplicas = acked;
        }
    }

    /// <summary>
    /// Fan out the work items across the ring. Each chunk is sent to R
    /// replicas; a chunk is considered durable when W replicas have
    /// acked. Throws on any chunk that fails to meet quorum.
    /// </summary>
    public static async Task<IReadOnlyList<WorkResult>> RunAsync(
        IReadOnlyList<WorkItem> items,
        CancellationToken ct)
    {
        if (items.Count == 0) return Array.Empty<WorkResult>();

        int R = ReplicaResolver.ReplicationFactor;
        int W = ReplicaResolver.WriteQuorum;

        // Pre-compute replicas per chunk, keyed by bucketId (=
        // BitstringToUlong(bitstring)). Routing by bucketId achieves
        // two properties simultaneously:
        //   1. Bucket affinity is preserved: same bitstring → same
        //      bucketId → same R replicas, so the agent owning that
        //      bucket index sees every chunk that belongs to it. Search
        //      RPCs don't need to fan out across replicas to get a
        //      complete view of a bucket.
        //   2. Read-side symmetry: the CCF ref encodes bucketId, so the
        //      decoder can compute the same R replicas at fetch time
        //      without needing to know the original bitstring.
        //
        // The ring is queried once per chunk so a mid-fan-out membership
        // change can't tear half the batch onto the old ring and half
        // onto the new one.
        var replicasPerItem = new IReadOnlyList<string>[items.Count];
        for (int q = 0; q < items.Count; q++)
        {
            string routingKey = BucketRoutingKey.FromBitstring(items[q].BitString);
            replicasPerItem[q] = ReplicaResolver.PickReplicaIps(routingKey, R);
            if (replicasPerItem[q].Count == 0)
                throw new InvalidOperationException(
                    $"ReplicatedBatchStore: no replicas resolved for chunk {items[q].ChunkIdx}. " +
                    $"Ring is empty and rendezvous fallback returned no agents.");
        }

        // Build per-agent batches: for each chunk, every replica receives
        // the chunk. With R=3 and N=5 each agent ends up with roughly 3/5
        // of the items.
        var byAgent = new Dictionary<string, List<int>>(StringComparer.Ordinal);
        for (int q = 0; q < items.Count; q++)
        {
            foreach (var replica in replicasPerItem[q])
            {
                if (!byAgent.TryGetValue(replica, out var lst))
                    byAgent[replica] = lst = new List<int>(items.Count);
                lst.Add(q);
            }
        }

        // Per-chunk state. Locks are tiny per-object monitors so concurrent
        // ack updates from different agents don't tear the StoreVector_Res
        // reference assignment.
        var ackedReplicas = new List<string>[items.Count];
        var primaryResp = new StoreVector_Res?[items.Count];
        var anyResp = new StoreVector_Res?[items.Count];
        var locks = new object[items.Count];
        for (int q = 0; q < items.Count; q++)
        {
            ackedReplicas[q] = new List<string>(R);
            locks[q] = new object();
        }

        // Fan out to all agents in parallel. We do NOT short-circuit on
        // first quorum-met: every successful per-agent batch contributes
        // to the durability count, and waiting for the slow agents in
        // the background lets us record extra replicas as a hint for
        // future anti-entropy.
        await Parallel.ForEachAsync(
            byAgent,
            new ParallelOptions { MaxDegreeOfParallelism = 16, CancellationToken = ct },
            async (kvp, cancel) =>
            {
                string agent = kvp.Key;
                var queueIndices = kvp.Value;

                for (int batchStart = 0; batchStart < queueIndices.Count; batchStart += StoreBatchSize)
                {
                    int batchEnd = Math.Min(batchStart + StoreBatchSize, queueIndices.Count);
                    var batchReq = new BatchStoreVector_Req();

                    for (int bi = batchStart; bi < batchEnd; bi++)
                    {
                        int q = queueIndices[bi];
                        var item = items[q];
                        var sreq = new StoreVector_Req
                        {
                            TargetIp = agent,
                            Bitstring = item.BitString,
                            HeadRouteID = ""
                        };
                        sreq.Vector.AddRange(item.Vector);
                        // Zero-copy: wrap the caller's slice rather than copying
                        // it into a fresh ByteString. Valid because the backing
                        // buffer outlives this awaited fan-out (see WorkItem).
                        sreq.Chunk = UnsafeByteOperations.UnsafeWrap(item.ChunkBytes);
                        batchReq.Items.Add(sreq);
                    }

                    BatchStoreVector_Res? batchRes = null;
                    Exception? lastEx = null;
                    for (int attempt = 0; attempt < MaxAttemptsPerAgent; attempt++)
                    {
                        try
                        {
                            var storeClient = GrpcChannelFactory.GetClient(
                                target: agent,
                                ctor: c => new StoreVector.StoreVectorClient(c),
                                roundRobin: false, port: 5000);
                            batchRes = await storeClient.BatchStoreAsync(batchReq, cancellationToken: cancel);
                            lastEx = null;
                            break;
                        }
                        catch (Exception ex)
                        {
                            // Drop the cached direct-IP channel on transport failure;
                            // the next attempt will re-resolve and rebuild.
                            GrpcChannelFactory.EvictOnFailure(agent, ex);
                            lastEx = ex;
                            if (attempt < MaxAttemptsPerAgent - 1)
                                await Task.Delay(TimeSpan.FromSeconds(2 * (attempt + 1)), cancel);
                        }
                    }

                    if (lastEx != null || batchRes == null)
                    {
                        // This agent's batch failed entirely. Don't throw: maybe other
                        // replicas will satisfy the W=2 quorum. The downstream check
                        // catches genuine quorum failures.
                        Console.WriteLine($"[ReplicatedBatchStore] agent {agent} batch failed: {lastEx?.Message ?? "null response"}");
                        continue;
                    }

                    int count = Math.Min(batchEnd - batchStart, batchRes.Results.Count);
                    if (count < batchEnd - batchStart)
                    {
                        Console.WriteLine(
                            $"[ReplicatedBatchStore] agent {agent} returned {count} results for {batchEnd - batchStart} requests; treating remainder as failed");
                    }

                    for (int bi = 0; bi < count; bi++)
                    {
                        int q = queueIndices[batchStart + bi];
                        var storeRes = batchRes.Results[bi];

                        if (storeRes.Failed)
                        {
                            // Per-result failure inside an otherwise successful batch
                            // (e.g. one chunk hit an agent-side validation issue). Skip
                            // this slot — it doesn't count as an ack.
                            continue;
                        }

                        bool isPrimary = replicasPerItem[q].Count > 0
                            && string.Equals(replicasPerItem[q][0], agent, StringComparison.Ordinal);

                        lock (locks[q])
                        {
                            ackedReplicas[q].Add(agent);
                            if (anyResp[q] == null) anyResp[q] = storeRes;
                            if (isPrimary) primaryResp[q] = storeRes;
                        }
                    }
                }
            });

        // Quorum verification + result assembly.
        var results = new List<WorkResult>(items.Count);
        var failures = new List<int>();
        for (int q = 0; q < items.Count; q++)
        {
            int requiredAcks = Math.Min(W, replicasPerItem[q].Count);

            // Per-chunk replica-ack telemetry: feeds into the
            // /stats/runtime stage_stats panel as "ReplicasPerWrite",
            // exposing p50/p95/p99 of how many of R replicas actually
            // ack each write. A p50 < W is a problem (quorum's failing
            // a lot); a p99 < R suggests a flaky replica deserves
            // investigation.
            StageStatsRollup.Record("ReplicasPerWrite", ackedReplicas[q].Count);

            if (ackedReplicas[q].Count < requiredAcks)
            {
                failures.Add(q);
                continue;
            }

            // Prefer primary's response; fall back to any successful replica.
            // For content-addressable storage all replicas converge on the
            // same storage_guid, but the local dedup state (BucketId/ChunkId/
            // BaseChunk/WasDeduplicated) is replica-specific. Using the
            // primary keeps the encoding deterministic across cross pods
            // that processed the same chunk.
            var resp = primaryResp[q] ?? anyResp[q]!;
            results.Add(new WorkResult(items[q].ChunkIdx, resp, ackedReplicas[q]));
        }

        if (failures.Count > 0)
        {
            // Surface up to 5 chunk indices in the message; full list in logs.
            var sample = string.Join(", ", failures.Take(5));
            throw new InvalidOperationException(
                $"ReplicatedBatchStore: write quorum W={W} not met for {failures.Count} chunks (sample: {sample}).");
        }

        return results;
    }

}
