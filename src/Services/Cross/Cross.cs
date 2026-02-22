
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Threading;
using System.Text;
using Cross.Interfaces.Cross;
using Cross.Modules;
using Cross.Utilities;
using GatewayService;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

using Cross.Services.Cache;
using Grpc.Core;

namespace Cross.Services.Cross;

public class CrossService : ICross
{
    // v2.0.0: Flat error encoding. One continuous delta stream across the entire stitched file.
    // v2.0.1: BucketId is now a deterministic ulong derived from the 64-bit bitstring (not auto-increment).
    // v2.1.0: SHA256 integrity hash appended after trim chunk. Decompression verifies output matches hash.
    //         Backwards compatible: v2.0.0 files without a hash are still decompressible (hash check skipped).
    private const string EncodingVersion = "v2.1.0";
    private static readonly string[] SupportedVersions = { "v2.0.0", "v2.1.0" };
    string headID = "";
    // OPTIMIZATION: Increased cache size from 10k to 100k for better hit rate
    private static readonly SearchCacheService _searchCache = new SearchCacheService(maxCacheSize: 100000);
    private readonly global::Cross.Services.Grpc.Agent.ChunkReferenceServiceClient _chunkReferenceClient = new();

    /// <summary>
    /// Convert a ulong back to a 64-char '0'/'1' bitstring.
    /// Inverse of the agent's BitstringToUlong — used to derive the correct agent for decompression.
    /// </summary>
    private static string UlongToBitstring(ulong packed)
    {
        char[] chars = new char[64];
        for (int i = 0; i < 64; i++)
            chars[i] = (packed & (1UL << i)) != 0 ? '1' : '0';
        return new string(chars);
    }

    private async Task initCLMS(string _name, string _id)
    {
        headID = await ClmsHandler.RegisterHeadRoute();
        await ClmsHandler.RegisterRoutePoint(headID, _name, _id);
    }

    private async Task addEvent(string _step, string _message, string _type = "step", string _level = "1")
    {
        // Skip CLMS events when running in local mode (ClmsHandler not initialized)
        if (ClmsHandler._instance == null || string.IsNullOrEmpty(headID))
        {
            return;
        }
        await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = _level, stepName = _step, type = _type, message = _message
        });
    }

    private (List<byte[]>, List<byte>) SplitChunks(byte[] _bytes, int _chunkSize)
    {
        List<byte[]> toRet = Misc.SplitFile(_bytes, _chunkSize);
        List<byte> trimmedChunk = new List<byte>();

        if (toRet.Count > 0 && toRet[^1].Length < _chunkSize)
        {
            trimmedChunk.AddRange(toRet[^1]);
        toRet.RemoveAt(toRet.Count - 1);
        }

        // CRITICAL: Validate all chunks are full-size - empty chunks should NEVER exist
        for (int i = 0; i < toRet.Count; i++)
        {
            if (toRet[i] == null || toRet[i].Length == 0)
            {
                throw new InvalidOperationException($"FATAL: SplitFile returned empty chunk at index {i}. This should NEVER happen.");
            }
            if (toRet[i].Length != _chunkSize)
            {
                throw new InvalidOperationException($"FATAL: SplitFile returned chunk at index {i} with size {toRet[i].Length}, expected {_chunkSize}. Trim chunks should have been removed.");
            }
        }

        return (toRet, trimmedChunk);
    }

    private static byte[] BuildCompressedPayload(
        string version,
        byte[] references,
        byte[] errorDictionary,
        byte[] trimChunk,
        byte[] originalFileHash)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        var versionBytes = Encoding.UTF8.GetBytes(version);
        bw.Write(versionBytes.Length);
        bw.Write(versionBytes);
        bw.Write(references.Length);
        bw.Write(errorDictionary.Length);
        bw.Write(trimChunk.Length);          // NEW in v2.1.0: trim chunk length (so we know where hash starts)
        bw.Write(references);
        bw.Write(errorDictionary);
        bw.Write(trimChunk);
        bw.Write(originalFileHash.Length);   // 32 for SHA256
        bw.Write(originalFileHash);
        bw.Flush();

        return ms.ToArray();
    }

    private static (string Version, int ReferencesLength, int ErrorLength, int TrimLength, int PayloadStart) ParseHeader(ReadOnlySpan<byte> payload)
    {
        const int IntSize = sizeof(int);
        if (payload.Length < IntSize * 3)
            throw new InvalidDataException("Compressed payload too short for header.");

        int offset = 0;
        int versionLen = BitConverter.ToInt32(payload.Slice(offset, IntSize));
        offset += IntSize;
        if (versionLen <= 0 || payload.Length < offset + versionLen + (IntSize * 2))
            throw new InvalidDataException("Invalid encoding version header.");

        string version = Encoding.UTF8.GetString(payload.Slice(offset, versionLen));
        offset += versionLen;
        int referencesLength = BitConverter.ToInt32(payload.Slice(offset, IntSize));
        offset += IntSize;
        int errorLength = BitConverter.ToInt32(payload.Slice(offset, IntSize));
        offset += IntSize;

        if (referencesLength < 0 || errorLength < 0)
            throw new InvalidDataException("Negative section length in compressed payload.");

        // v2.1.0+: trim chunk length is stored in the header
        int trimLength = -1; // -1 means "not present" (v2.0.0 compat)
        if (version == "v2.1.0")
        {
            trimLength = BitConverter.ToInt32(payload.Slice(offset, IntSize));
            offset += IntSize;
        }

        return (version, referencesLength, errorLength, trimLength, offset);
    }

    private List<QueryObject> GetQueryObjects(List<byte[]> _fileChunks, List<float[]> _vectors, List<string> _bitStrings)
    {
        List<QueryObject> toRet = new List<QueryObject>();
        for(int i = 0; i < _fileChunks.Count; i++)
        {
            QueryObject toAdd = new QueryObject()
            {
                Index = i,
                BucketString = _bitStrings[i],
                // OPTIMIZATION: Don't send chunk bytes - reduces network traffic by ~80%
                // Cross will store chunks in background after returning file
                Chunk = ByteString.Empty
            };
            toAdd.Vector.AddRange(_vectors[i]);

            toRet.Add(toAdd);
        }
        return toRet;
    }

    public List<QueryObject[]> BatchQueries(List<QueryObject> _queries, int _size)
    {
        if (_size <= 0) 
            throw new ArgumentException("Batch size must be > 0", nameof(_size));

        var toRet = new List<QueryObject[]>();
        int total = _queries.Count;
        // number of batches = ceil(total / _size)
        int batchCount = (total + _size - 1) / _size;

        for (int i = 0; i < batchCount; i++)
        {
            int start = i * _size;
            // for the last batch, the remaining count might be less than _size
            int count = Math.Min(_size, total - start);
            toRet.Add(_queries.GetRange(start, count).ToArray());
        }

        return toRet;
    }

    // Local/in-process helper: returns compressed bytes plus per-file reference stats
    public async Task<(byte[] CompressedBytes, int ReferencesFound, int TotalChunks)> CompressFileWithStats(byte[] _file)
    {
        using var rootSpan = Observability.StartStage("CompressFileWithStats");
        // Divide into (N) chunks
        var totalSw = Stopwatch.StartNew();
        List<byte[]> fileChunks;
        List<byte> trimmedChunk;
        {
            using var stage = Observability.StartStage("SplitChunks");
            var swStage = Stopwatch.StartNew();
            (fileChunks, trimmedChunk) = SplitChunks(_file, Globals.chunkSize);
            swStage.Stop();
            Console.WriteLine($"[Compress] Split: {swStage.ElapsedMilliseconds}ms, {fileChunks.Count} chunks ({_file.Length} bytes)");
            Observability.RecordStage("SplitChunks", swStage.Elapsed.TotalMilliseconds, ("chunk_count", fileChunks.Count));
        }

        // Vectorize
        List<float[]> vectors;
        {
            using var stage = Observability.StartStage("Vectorize");
            var swStage = Stopwatch.StartNew();
            vectors = Misc.Compute64ElementLSHVectors(fileChunks);
            swStage.Stop();
            Console.WriteLine($"[Compress] Vectorize: {swStage.ElapsedMilliseconds}ms");
            Observability.RecordStage("Vectorize", swStage.Elapsed.TotalMilliseconds, ("chunk_count", vectors.Count));
        }

        // Extract bitstring
        List<string> bitStrings;
        {
            using var stage = Observability.StartStage("ExtractBucketKeys");
            var swStage = Stopwatch.StartNew();
            bitStrings = Misc.ComputeBitStringFromVectors(vectors);
            swStage.Stop();
            Console.WriteLine($"[Compress] BitStrings: {swStage.ElapsedMilliseconds}ms");
            Observability.RecordStage("ExtractBucketKeys", swStage.Elapsed.TotalMilliseconds, ("bucket_count", bitStrings.Count));
        }

        // ── DIRECT-TO-AGENT SEARCH (bypasses gateway entirely) ──
        // CRITICAL: Each of the 65 neighbor buckets may live on a DIFFERENT agent.
        // We must route each bucket to its owning agent, then merge results per chunk.
        // Without this, we only search ~20% of the intended search space (1/N agents).
        Stopwatch sw = Stopwatch.StartNew();
        var phaseSw = Stopwatch.StartNew();

        const float MIN_THRESH = 0.60f;

        // Keep chunk mapping for background storage
        Dictionary<int, byte[]> chunkMap = new Dictionary<int, byte[]>();
        for (int i = 0; i < fileChunks.Count; i++)
        {
            if (fileChunks[i] == null || fileChunks[i].Length == 0)
                throw new InvalidOperationException($"FATAL: Empty chunk at index {i}.");
            if (fileChunks[i].Length != Globals.chunkSize)
                throw new InvalidOperationException($"FATAL: Chunk at index {i} has size {fileChunks[i].Length}, expected {Globals.chunkSize}.");
            chunkMap[i] = fileChunks[i];
        }

        // ── Phase 1: Compute neighbors + per-bucket agent routing ──
        phaseSw.Restart();
        var allBuckets = new List<string>[fileChunks.Count];     // 65 buckets per chunk
        var mainAgents = new string[fileChunks.Count];            // agent owning the main bucket
        // queryInfos is used later by StoreChunks section
        var queryInfos = new (float[] vector, string bitString)[fileChunks.Count];

        Parallel.For(0, fileChunks.Count, i =>
        {
            allBuckets[i] = RendezvousRouter.GetNeighbouringBuckets(bitStrings[i], vectors[i]);
            mainAgents[i] = RendezvousRouter.PickAgent(bitStrings[i]);
            queryInfos[i] = (vectors[i], bitStrings[i]);
        });

        // Route each bucket to its owning agent:
        // Agent → Dict<chunkIndex, List<buckets owned by this agent>>
        var agentChunkBuckets = new Dictionary<string, Dictionary<int, List<string>>>();

        for (int i = 0; i < fileChunks.Count; i++)
        {
            foreach (var bucket in allBuckets[i])
            {
                var agent = RendezvousRouter.PickAgent(bucket);
                if (!agentChunkBuckets.TryGetValue(agent, out var chunkBuckets))
                {
                    chunkBuckets = new Dictionary<int, List<string>>();
                    agentChunkBuckets[agent] = chunkBuckets;
                }
                if (!chunkBuckets.TryGetValue(i, out var bucketList))
                {
                    bucketList = new List<string>(16);
                    chunkBuckets[i] = bucketList;
                }
                bucketList.Add(bucket);
            }
        }
        Console.WriteLine($"[Compress] Route: {phaseSw.ElapsedMilliseconds}ms, {fileChunks.Count} chunks → {agentChunkBuckets.Count} agents");

        // ── Phase 2: Fire BatchGet per agent — each gets ONLY its own buckets ──
        var sorted = new QueryResponseObject?[fileChunks.Count];
        {
            using var stage = Observability.StartStage("SearchBuckets");
            phaseSw.Restart();

            // Per-chunk best match tracking (merge across agents)
            var bestSims = new float[fileChunks.Count];
            Array.Fill(bestSims, -1f);

            // Split large batches to avoid gRPC message size limits (default 4MB).
            // For 14,745 chunks × 65 buckets = ~958k queries, a single batch would be ~50MB+.
            // Split into chunks of max 1000 queries per batch.
            const int MAX_QUERIES_PER_BATCH = 1000;
            var batchTasks = new List<Task>();

            foreach (var kv in agentChunkBuckets)
            {
                var agent = kv.Key;
                var chunkBuckets = kv.Value; // Dict<chunkIndex, buckets on this agent>

                // Convert to list for chunking
                var allQueries = chunkBuckets.Select(kvp => (chunkIdx: kvp.Key, buckets: kvp.Value)).ToList();

                // Split into batches of MAX_QUERIES_PER_BATCH
                for (int batchStart = 0; batchStart < allQueries.Count; batchStart += MAX_QUERIES_PER_BATCH)
                {
                    int batchEnd = Math.Min(batchStart + MAX_QUERIES_PER_BATCH, allQueries.Count);
                    var batchQueries = allQueries.Skip(batchStart).Take(batchEnd - batchStart).ToList();

                    batchTasks.Add(Task.Run(async () =>
                    {
                        try
                        {
                            var client = GrpcChannelFactory.GetClient(
                                target: agent,
                                ctor: chan => new SearchVector.SearchVectorClient(chan),
                                roundRobin: false, port: 5000);

                            var batchReq = new BatchSearchVector_Req();
                            var indexMap = new List<int>(batchQueries.Count);

                            foreach (var (chunkIdx, buckets) in batchQueries)
                            {
                                var req = new SearchVector_Req
                                {
                                    Index = chunkIdx,
                                    MinimumSimilarity = MIN_THRESH,
                                    K = 1
                                };
                                req.Vector.AddRange(vectors[chunkIdx]);
                                req.Bitstrings.AddRange(buckets);
                                batchReq.Queries.Add(req);
                                indexMap.Add(chunkIdx);
                            }

                            var batchRes = await client.BatchGetAsync(batchReq,
                                deadline: DateTime.UtcNow.AddSeconds(10));

                            // Map results — only update sorted[idx] if this agent found a BETTER match
                            for (int j = 0; j < indexMap.Count && j < batchRes.Results.Count; j++)
                            {
                                var idx = indexMap[j];
                                var res = batchRes.Results[j];

                                if (!res.Save && res.Results.Count > 0)
                                {
                                    var best = res.Results[0];
                                    float sim = best.Similarity;
                                    if (sim >= MIN_THRESH && sim > bestSims[idx])
                                    {
                                        // Thread-safe: compare-and-swap the best similarity
                                        float current;
                                        do
                                        {
                                            current = Volatile.Read(ref bestSims[idx]);
                                            if (sim <= current) break; // Another agent found better
                                        } while (Interlocked.CompareExchange(ref bestSims[idx], sim, current) != current);

                                        if (sim > current)
                                        {
                                            sorted[idx] = new QueryResponseObject
                                            {
                                                BucketId = best.BucketId,
                                                BucketKey = (ulong)best.BucketKey,
                                                Similarity = sim,
                                                Chunk = best.Chunk, // base chunk bytes from agent MRU cache
                                                Index = idx,
                                                Duplicate = true,
                                                NeedToStore = false,
                                                TargetAgent = agent
                                            };
                                        }
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Compress] BatchGet to {agent} failed: {ex.Message}");
                            // Don't mark as need-to-store here — other agents may have matches
                        }
                    }));
                }
            }

            await Task.WhenAll(batchTasks);
            phaseSw.Stop();
            Console.WriteLine($"[Compress] Search: {phaseSw.ElapsedMilliseconds}ms across {agentChunkBuckets.Count} agents");
            Observability.RecordStage("SearchBuckets", phaseSw.Elapsed.TotalMilliseconds,
                ("agents", agentChunkBuckets.Count), ("queries", fileChunks.Count));
        }

        // Fill any chunks that NO agent had a match for → need to store on main agent
        for (int i = 0; i < sorted.Length; i++)
        {
            if (sorted[i] == null)
            {
                sorted[i] = new QueryResponseObject
                {
                    BucketId = 0, BucketKey = 0,
                    Similarity = 1.0f, Chunk = ByteString.Empty,
                    Index = i, Duplicate = true,
                    NeedToStore = true, TargetAgent = mainAgents[i]
                };
            }
        }

        // ── BATCH STORE: one gRPC call per agent instead of one per chunk ──
        // Rendezvous hash already told us which agent owns each chunk.
        // Group by agent, send ONE BatchStore per agent → ~5 calls instead of ~8,000.
        {
            using var stage = Observability.StartStage("StoreChunks");
            var swStage = Stopwatch.StartNew();

            // Collect all chunks that need storing, grouped by target agent
            var storeGroups = new Dictionary<string, List<(int index, QueryResponseObject response, byte[] chunk, float[] vector, string bucketString)>>();
            for (int i = 0; i < sorted.Length; i++)
            {
                if (sorted[i] == null || !sorted[i].NeedToStore) continue;

                if (!chunkMap.TryGetValue(i, out var chunkBytes))
                    throw new InvalidOperationException($"FATAL: chunkMap missing entry for index {i}.");
                if (chunkBytes == null || chunkBytes.Length == 0)
                    throw new InvalidOperationException($"FATAL: Empty chunk at index {i}.");
                if (chunkBytes.Length != Globals.chunkSize)
                    throw new InvalidOperationException($"FATAL: Chunk at index {i} has size {chunkBytes.Length}, expected {Globals.chunkSize}.");
                if (string.IsNullOrWhiteSpace(sorted[i].TargetAgent))
                    throw new InvalidOperationException($"FATAL: TargetAgent is empty for chunk at index {i}.");

                var agent = sorted[i].TargetAgent;
                if (!storeGroups.TryGetValue(agent, out var list))
                {
                    list = new List<(int, QueryResponseObject, byte[], float[], string)>();
                    storeGroups[agent] = list;
                }
                list.Add((i, sorted[i], chunkBytes, vectors[i], bitStrings[i]));
            }

            // Fire BatchStore per agent — split large batches to avoid gRPC message size limits.
            // Each chunk is ~5120 bytes + vector + metadata. 14,745 chunks = ~75MB+ per batch.
            // Split into chunks of max 1000 items per batch.
            const int MAX_STORES_PER_BATCH = 1000;
            int totalStored = 0;
            if (storeGroups.Count > 0)
            {
                var batchTasks = new List<Task>();

                foreach (var kv in storeGroups)
                {
                    var agent = kv.Key;
                    var items = kv.Value;

                    // Split into batches of MAX_STORES_PER_BATCH
                    for (int batchStart = 0; batchStart < items.Count; batchStart += MAX_STORES_PER_BATCH)
                    {
                        int batchEnd = Math.Min(batchStart + MAX_STORES_PER_BATCH, items.Count);
                        var batchItems = items.Skip(batchStart).Take(batchEnd - batchStart).ToList();

                        batchTasks.Add(Task.Run(async () =>
                        {
                            // Build batch request OUTSIDE try so retry can reuse it
                            var batchReq = new BatchStoreVector_Req();
                            foreach (var item in batchItems)
                            {
                                var req = new StoreVector_Req
                                {
                                    TargetIp = agent,
                                    Bitstring = item.bucketString,
                                    HeadRouteID = ""
                                };
                                req.Vector.AddRange(item.vector);
                                req.Chunk = ByteString.CopyFrom(item.chunk);
                                batchReq.Items.Add(req);
                            }

                            try
                            {
                                var client = GrpcChannelFactory.GetClient(
                                    target: agent,
                                    ctor: chan => new StoreVector.StoreVectorClient(chan),
                                    roundRobin: false, port: 5000);

                                var batchRes = await client.BatchStoreAsync(batchReq,
                                    deadline: DateTime.UtcNow.AddSeconds(20));

                                for (int j = 0; j < batchItems.Count && j < batchRes.Results.Count; j++)
                                {
                                    batchItems[j].response.BucketId = batchRes.Results[j].Id;
                                    batchItems[j].response.BucketKey = batchRes.Results[j].Index;
                                }
                                Interlocked.Add(ref totalStored, batchItems.Count);
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[Compress] BatchStore to {agent} failed: {ex.Message}, retrying SAME agent...");
                                // CRITICAL: NEVER store on a different agent!
                                // PickAgent(bitstring) always routes to THIS agent during decompression.
                                // Storing on a different agent means decompression can't find the chunk.
                                try
                                {
                                    await Task.Delay(200); // Brief backoff before retry
                                    var retryClient = GrpcChannelFactory.GetClient(
                                        target: agent,
                                        ctor: chan => new StoreVector.StoreVectorClient(chan),
                                        roundRobin: false, port: 5000);
                                    var retryRes = await retryClient.BatchStoreAsync(batchReq,
                                        deadline: DateTime.UtcNow.AddSeconds(15));
                                    for (int j = 0; j < batchItems.Count && j < retryRes.Results.Count; j++)
                                    {
                                        batchItems[j].response.BucketId = retryRes.Results[j].Id;
                                        batchItems[j].response.BucketKey = retryRes.Results[j].Index;
                                    }
                                    Interlocked.Add(ref totalStored, batchItems.Count);
                                    Console.WriteLine($"[Compress] Retry store to {agent} OK for {batchItems.Count} chunks");
                                }
                                catch (Exception retryEx)
                                {
                                    Console.WriteLine($"[Compress] Retry store to {agent} also failed: {retryEx.Message}");
                                    // Primary agent unreachable — clear refs so diff encodes the full chunk
                                    foreach (var item in batchItems)
                                    { item.response.BucketId = 0; item.response.BucketKey = 0; }
                                }
                            }
                        }));
                    }
                }

                await Task.WhenAll(batchTasks);
            }

            swStage.Stop();
            Console.WriteLine($"[Compress] Store: {swStage.ElapsedMilliseconds}ms, {totalStored} chunks across {storeGroups.Count} agents");
            Observability.RecordStage("StoreChunks", swStage.Elapsed.TotalMilliseconds,
                ("agents", storeGroups.Count), ("chunks_stored", totalStored));
        }

        // Stats: "references found" = chunks that re-used an existing base (not newly stored chunks).
        // Count only when Duplicate=true AND similarity < 1.0 (excludes newly stored chunks where similarity=1.0).
        int referencesFound = sorted.Count(r => r != null && r.Duplicate && r.Similarity < 1.0f);
        int totalChunks = sorted.Length;
        int nullCount = sorted.Count(r => r == null);
        int zeroIdCount = sorted.Count(r => r != null && r.BucketId == 0 && r.BucketKey == 0);
        int validRefCount = sorted.Count(r => r != null && r.BucketId > 0);
        int emptyChunkRefs = sorted.Count(r => r != null && r.BucketId > 0 && (r.Chunk == null || r.Chunk.Length == 0));
        // Removed Console.WriteLine for performance (stats available via observability)

        // ── FLAT ERROR ENCODING PREP ──
        // References are built once after all re-stores are finalized.
        // Error encoding is a single flat stream across the entire stitched file.
        int diffCount = 0;
        int emptyDiffCount = 0;
        int zeroRefCount = 0;
        var bloatedDiffRestore = new List<int>();
        {
            using var stage = Observability.StartStage("DiffEncode");
            phaseSw.Restart();

            // ── BASE CHUNK FETCH (fallback only) ──
            // Agent search now returns base chunk bytes from its MRU cache (O(1)).
            // This fetch is only needed for the RARE case where the agent's MRU cache
            // evicted the chunk (>512MB of chunks on one agent). Most files will have
            // zero entries in baseChunkFetchIndices.
            var baseChunkFetchIndices = new List<int>();
            for (int i = 0; i < sorted.Length; i++)
        {
            if (sorted[i] == null)
            {
                sorted[i] = new QueryResponseObject()
                {
                        BucketId = 0, BucketKey = 0, Similarity = 0,
                        Chunk = ByteString.Empty, Index = i, Duplicate = false
                    };
                }

                bool isExactMatch = sorted[i].Similarity >= 0.999999f;
                bool skipDiff = (sorted[i].NeedToStore || isExactMatch) && sorted[i].BucketId != 0;

                if (!skipDiff && sorted[i].BucketId != 0
                    && (sorted[i].Chunk == null || sorted[i].Chunk.Length == 0))
                {
                    // This chunk matched a reference but has no base bytes — need to fetch
                    baseChunkFetchIndices.Add(i);
                }
            }

            // Parallel fetch all needed base chunks from the CORRECT agent.
            // Gateway sets TargetAgent for ALL responses so we know exactly where the data lives.
            // This prevents the old bug where round-robin hit the wrong agent → recursive cross-agent search → timeout.
            if (baseChunkFetchIndices.Count > 0)
            {
                var fetchSw = Stopwatch.StartNew();
                var fetchedChunks = new byte[sorted.Length][];
                var fetchTasks = baseChunkFetchIndices.Select(async idx =>
                {
                    try
                    {
                        // Route to the specific agent that owns this chunk (set by gateway)
                        string? targetAgent = sorted[idx].TargetAgent;
                        var chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(
                            sorted[idx].BucketId, sorted[idx].BucketKey,
                            targetAgent: string.IsNullOrWhiteSpace(targetAgent) ? null : targetAgent);
                        if (chunk != null && chunk.Length > 0)
                            fetchedChunks[idx] = chunk;
                    }
                    catch { /* Will fall back to zeros */ }
                });
                await Task.WhenAll(fetchTasks);
                fetchSw.Stop();
                Observability.RecordStage("FetchBaseChunks", fetchSw.Elapsed.TotalMilliseconds,
                    ("count", baseChunkFetchIndices.Count));

                // Inject fetched chunks into sorted results
                for (int i = 0; i < sorted.Length; i++)
                {
                    if (fetchedChunks[i] != null)
                        sorted[i].Chunk = ByteString.CopyFrom(fetchedChunks[i]);
                }

                // ── CRITICAL: Handle failed base chunk fetches ──
                // If we have a valid reference (BucketId != 0) but couldn't fetch the base chunk,
                // diffing against zeros would cause DATA CORRUPTION on decompression.
                // Fix: store the chunk as its own reference (empty diff = base == original).
                var failedFetchStore = new List<(int index, QueryResponseObject resp, byte[] chunk, string agent, float[] vector, string bucket)>();
                foreach (var idx in baseChunkFetchIndices)
                {
                    if (fetchedChunks[idx] != null) continue; // fetch succeeded
                    if (sorted[idx].BucketId == 0) continue;  // already no ref, zeros diff is correct

                    // Base fetch FAILED with a valid reference → must store chunk to avoid corruption
                    if (chunkMap.TryGetValue(idx, out var chunkBytes) && chunkBytes != null && chunkBytes.Length == Globals.chunkSize)
                    {
                        string agent = sorted[idx].TargetAgent ?? "";
                        if (!string.IsNullOrWhiteSpace(agent))
                        {
                            failedFetchStore.Add((idx, sorted[idx], chunkBytes, agent, queryInfos[idx].vector, queryInfos[idx].bitString));
                }
                else
                {
                            // No agent to store to — clear reference so zeros-diff is at least correct
                            sorted[idx].BucketId = 0;
                            sorted[idx].BucketKey = 0;
                        }
                    }
                }

                // Store failed-fetch chunks to get valid self-references (base == original → empty diff)
                if (failedFetchStore.Count > 0)
                {
                    var fixSw = Stopwatch.StartNew();
                    var fixTasks = failedFetchStore.Select(async item =>
                    {
                        try
                        {
                            var client = GrpcChannelFactory.GetClient(
                                target: item.agent,
                                ctor: chan => new StoreVector.StoreVectorClient(chan),
                                roundRobin: false, port: 5000);
                            var storeReq = new StoreVector_Req
                            {
                                TargetIp = item.agent,
                                Bitstring = item.bucket,
                                HeadRouteID = ""
                            };
                            storeReq.Vector.AddRange(item.vector);
                            storeReq.Chunk = ByteString.CopyFrom(item.chunk);
                            var storeRes = await client.StoreAsync(storeReq, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(10)));
                            item.resp.BucketId = storeRes.Id;
                            item.resp.BucketKey = storeRes.Index;
                            item.resp.NeedToStore = true;
                            item.resp.Similarity = 1.0f; // base == original → empty diff
                        }
                        catch
                        {
                            // Store also failed — clear reference (zeros-diff is correct for (0,0) refs)
                            item.resp.BucketId = 0;
                            item.resp.BucketKey = 0;
                        }
                    }).ToList();
                    await Task.WhenAll(fixTasks);
                    fixSw.Stop();
                    Console.WriteLine($"[Compress] Fixed {failedFetchStore.Count} failed base fetches by storing chunks ({fixSw.ElapsedMilliseconds}ms)");
                }
            }

            // ── Bloat guard pre-pass (count-only, no list allocation) ──
            // We only need to detect chunks whose diff would be larger than the raw chunk.
            // The actual diff is computed once in the flat encoding pass below.
            for (int i = 0; i < sorted.Length; i++)
            {
                if (sorted[i].BucketId == 0 && sorted[i].BucketKey == 0)
                {
                    zeroRefCount++;
                    continue;
                }

                bool isExactMatch = sorted[i].Similarity >= 0.999999f;
                if ((sorted[i].NeedToStore || isExactMatch) && sorted[i].BucketId != 0)
                {
                    emptyDiffCount++;
                    continue;
                }

                // Count diffs only — no list allocation needed
                byte[] baseChunk;
                if (sorted[i].Chunk != null && sorted[i].Chunk.Length > 0)
                    baseChunk = sorted[i].Chunk.ToByteArray();
                else
                    baseChunk = new byte[Globals.chunkSize];

                int pairCount = Misc.GetErrorEncodingCount(fileChunks[i], baseChunk);

                // BLOAT GUARD: if per-chunk diff bytes would exceed chunk size, re-store as self-reference
                if (pairCount * 6 >= Globals.chunkSize)
                {
                    sorted[i].NeedToStore = true;
                    sorted[i].Similarity = 1.0f;
                    bloatedDiffRestore.Add(i);
                    emptyDiffCount++;
                }
                else
                {
                    if (pairCount == 0) emptyDiffCount++;
                    else diffCount++;
                }
            }
            phaseSw.Stop();
            Console.WriteLine($"[Compress] DiffEncode: {phaseSw.ElapsedMilliseconds}ms, empty={emptyDiffCount}, diffs={diffCount}, zeroRef={zeroRefCount}, bloated={bloatedDiffRestore.Count}");
            Observability.RecordStage("DiffEncode", phaseSw.Elapsed.TotalMilliseconds,
                ("chunk_count", sorted.Length), ("empty_diff", emptyDiffCount), ("non_empty_diff", diffCount),
                ("zero_ref", zeroRefCount), ("bloated_restore", bloatedDiffRestore.Count));
        }

        // ── RE-STORE pass for chunks whose diff would bloat the file ──
        // These chunks matched a reference but the byte-level diff was larger than raw chunk.
        // Store them as their own base → empty diff. Grouped by agent for BatchStore.
        if (bloatedDiffRestore.Count > 0)
        {
            phaseSw.Restart();
            var reStoreGroups = new Dictionary<string, List<(int index, QueryResponseObject response, byte[] chunk, float[] vector, string bucketString)>>();
            foreach (var i in bloatedDiffRestore)
            {
                if (!chunkMap.TryGetValue(i, out var chunkBytes) || chunkBytes == null) continue;
                var agent = sorted[i].TargetAgent ?? mainAgents[i];
                if (!reStoreGroups.TryGetValue(agent, out var list))
                {
                    list = new List<(int, QueryResponseObject, byte[], float[], string)>();
                    reStoreGroups[agent] = list;
                }
                list.Add((i, sorted[i], chunkBytes, vectors[i], bitStrings[i]));
            }

            // Split large batches to avoid gRPC message size limits
            const int MAX_RESTORE_PER_BATCH = 1000;
            var reStoreTasks = new List<Task>();

            foreach (var kv in reStoreGroups)
            {
                var agent = kv.Key;
                var items = kv.Value;

                // Split into batches of MAX_RESTORE_PER_BATCH
                for (int batchStart = 0; batchStart < items.Count; batchStart += MAX_RESTORE_PER_BATCH)
                {
                    int batchEnd = Math.Min(batchStart + MAX_RESTORE_PER_BATCH, items.Count);
                    var batchItems = items.Skip(batchStart).Take(batchEnd - batchStart).ToList();

                    reStoreTasks.Add(Task.Run(async () =>
                    {
                        // Build batch request OUTSIDE try so retry can reuse it
                        var batchReq = new BatchStoreVector_Req();
                        foreach (var item in batchItems)
                        {
                            var req = new StoreVector_Req
                            {
                                TargetIp = agent,
                                Bitstring = item.bucketString,
                                HeadRouteID = ""
                            };
                            req.Vector.AddRange(item.vector);
                            req.Chunk = ByteString.CopyFrom(item.chunk);
                            batchReq.Items.Add(req);
                        }

                        try
                        {
                            var client = GrpcChannelFactory.GetClient(
                                target: agent,
                                ctor: chan => new StoreVector.StoreVectorClient(chan),
                                roundRobin: false, port: 5000);

                            var batchRes = await client.BatchStoreAsync(batchReq,
                                deadline: DateTime.UtcNow.AddSeconds(15));

                            for (int j = 0; j < batchItems.Count && j < batchRes.Results.Count; j++)
                            {
                                batchItems[j].response.BucketId = batchRes.Results[j].Id;
                                batchItems[j].response.BucketKey = batchRes.Results[j].Index;
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Compress] Re-store to {agent} failed: {ex.Message}, retrying SAME agent...");
                            // CRITICAL: NEVER store on a different agent — decompression routes by bitstring to THIS agent.
                            try
                            {
                                await Task.Delay(200);
                                var retryClient = GrpcChannelFactory.GetClient(
                                    target: agent,
                                    ctor: chan => new StoreVector.StoreVectorClient(chan),
                                    roundRobin: false, port: 5000);
                                var retryRes = await retryClient.BatchStoreAsync(batchReq,
                                    deadline: DateTime.UtcNow.AddSeconds(10));
                                for (int j = 0; j < batchItems.Count && j < retryRes.Results.Count; j++)
                                {
                                    batchItems[j].response.BucketId = retryRes.Results[j].Id;
                                    batchItems[j].response.BucketKey = retryRes.Results[j].Index;
                                }
                                Console.WriteLine($"[Compress] Retry re-store to {agent} OK for {batchItems.Count} chunks");
                            }
                            catch
                            {
                                // Primary agent unreachable — clear refs so diff encodes the full chunk
                                foreach (var item in batchItems)
                                { item.response.BucketId = 0; item.response.BucketKey = 0; }
                            }
                        }
                    }));
                }
            }

            await Task.WhenAll(reStoreTasks);

            phaseSw.Stop();
            Console.WriteLine($"[Compress] BloatRestore: {phaseSw.ElapsedMilliseconds}ms, {bloatedDiffRestore.Count} chunks re-stored");
        }

        // ── Build references (once, after all re-stores are finalized) ──
        var references = new List<byte>(sorted.Length * (sizeof(ulong) * 2));
        for (int i = 0; i < sorted.Length; i++)
        {
            references.AddRange(BitConverter.GetBytes(sorted[i].BucketId));
            references.AddRange(BitConverter.GetBytes(sorted[i].BucketKey));
        }

        // ── FLAT ERROR ENCODING (v2.0.0) ──
        // Stitch all original chunks and all base chunks into two continuous buffers,
        // run GetErrorEncoding once across the whole thing. Identical chunks contribute
        // zero entries. No per-chunk overhead — just the diffs that exist.
        byte[] errorDictionaryBytes;
        {
            using var stage = Observability.StartStage("FlatEncode");
            phaseSw.Restart();

            int totalBytes = sorted.Length * Globals.chunkSize;
            byte[] originalBuffer = new byte[totalBytes];
            byte[] baseBuffer = new byte[totalBytes];

            for (int i = 0; i < sorted.Length; i++)
            {
                int off = i * Globals.chunkSize;

                // Original chunk
                Buffer.BlockCopy(fileChunks[i], 0, originalBuffer, off, Globals.chunkSize);

                // Determine base chunk
                bool isExactMatch = sorted[i].Similarity >= 0.999999f;
                if ((sorted[i].NeedToStore || isExactMatch) && sorted[i].BucketId != 0)
                {
                    // base == original → copy original (zero diff)
                    Buffer.BlockCopy(fileChunks[i], 0, baseBuffer, off, Globals.chunkSize);
                }
                else if (sorted[i].Chunk != null && sorted[i].Chunk.Length > 0)
                {
                    // Matched chunk with base bytes from agent
                    var baseBytes = sorted[i].Chunk.ToByteArray();
                    Buffer.BlockCopy(baseBytes, 0, baseBuffer, off, Math.Min(baseBytes.Length, Globals.chunkSize));
                }
                // else: base is zeros (already zeroed by new byte[])
            }

            // ONE flat pass across the entire file
            var flatDiff = Misc.GetErrorEncoding(originalBuffer, baseBuffer);

            // ── INTEGRITY CHECK 1: full round-trip verification ──
            // Apply patches to a COPY of baseBuffer and verify byte-for-byte match
            // against originalBuffer. This catches diff-computation bugs, wrong-base
            // chunks, or any logic error that the weaker range check would miss.
            {
                byte[] verifyBuf = new byte[baseBuffer.Length];
                Buffer.BlockCopy(baseBuffer, 0, verifyBuf, 0, baseBuffer.Length);

                int cursor = 0;
                foreach (var (key, value) in flatDiff)
                {
                    cursor += key;
                    if (cursor < 0 || cursor >= verifyBuf.Length)
                        throw new InvalidDataException(
                            $"Compression integrity: patch cursor {cursor} out of range (buffer {verifyBuf.Length}).");

                    int patched = verifyBuf[cursor] + value;
                    if (patched < 0 || patched > 255)
                    {
                        Console.WriteLine($"[Compress] INTEGRITY FAIL: base[{cursor}]={verifyBuf[cursor]} + delta={value} = {patched} (chunk {cursor / Globals.chunkSize}, BucketId={sorted[cursor / Globals.chunkSize].BucketId})");
                        throw new InvalidDataException(
                            $"Compression integrity check failed: patch at {cursor} produces out-of-range byte {patched}.");
                    }
                    verifyBuf[cursor] = (byte)patched;
                }

                // Byte-for-byte comparison: patched base must equal original
                if (!verifyBuf.AsSpan().SequenceEqual(originalBuffer.AsSpan()))
                {
                    for (int i = 0; i < verifyBuf.Length; i++)
                    {
                        if (verifyBuf[i] != originalBuffer[i])
                        {
                            int chunkIdx = i / Globals.chunkSize;
                            Console.WriteLine($"[Compress] INTEGRITY FAIL: byte {i} (chunk {chunkIdx}, BucketId={sorted[chunkIdx].BucketId}): expected 0x{originalBuffer[i]:X2}, got 0x{verifyBuf[i]:X2}");
                            break;
                        }
                    }
                    throw new InvalidDataException(
                        "Compression integrity check failed: patched base buffer does not match original file. " +
                        "This indicates a base-chunk mismatch or diff-encoding bug.");
                }
            }

            // Serialize: <int totalPairCount> + <int deltaIndex><short delta> × totalPairCount
            using (var errorMs = new MemoryStream(4 + flatDiff.Count * 6))
            using (var errorWriter = new BinaryWriter(errorMs, Encoding.UTF8, leaveOpen: true))
            {
                errorWriter.Write(flatDiff.Count);
                foreach (var pair in flatDiff)
                {
                    errorWriter.Write(pair.key);
                    errorWriter.Write((short)pair.value);
                }
                errorWriter.Flush();
                errorDictionaryBytes = errorMs.ToArray();
            }

            // ── INTEGRITY CHECK 2: serialization round-trip ──
            // Deserialize what we just wrote and verify it matches the source diffs.
            // Catches truncation (int→short overflow), off-by-one, or stream bugs.
            {
                using var verifyMs = new MemoryStream(errorDictionaryBytes, writable: false);
                using var verifyReader = new BinaryReader(verifyMs, Encoding.UTF8, leaveOpen: true);
                int verifyCount = verifyReader.ReadInt32();
                if (verifyCount != flatDiff.Count)
                    throw new InvalidDataException(
                        $"Serialization integrity: pair count mismatch ({verifyCount} vs {flatDiff.Count}).");

                for (int i = 0; i < verifyCount; i++)
                {
                    int rKey = verifyReader.ReadInt32();
                    short rVal = verifyReader.ReadInt16();
                    if (rKey != flatDiff[i].key || rVal != (short)flatDiff[i].value)
                        throw new InvalidDataException(
                            $"Serialization integrity: pair[{i}] mismatch. Expected ({flatDiff[i].key},{flatDiff[i].value}), got ({rKey},{rVal}).");
                }
            }

            phaseSw.Stop();
            Console.WriteLine($"[Compress] FlatEncode: {phaseSw.ElapsedMilliseconds}ms, pairs={flatDiff.Count}, bytes={errorDictionaryBytes.Length}");
            Observability.RecordStage("FlatEncode", phaseSw.Elapsed.TotalMilliseconds,
                ("pairs", flatDiff.Count), ("bytes", errorDictionaryBytes.Length));
        }

        byte[] toReturn;
        {
            using var stage = Observability.StartStage("Serialize");
            var swStage = Stopwatch.StartNew();
            // SHA256 integrity hash of the ORIGINAL file — verified during decompression
            byte[] originalHash = SHA256.HashData(_file);
            toReturn = BuildCompressedPayload(
                EncodingVersion,
                references.ToArray(),
                errorDictionaryBytes,
                trimmedChunk.ToArray(),
                originalHash);
            swStage.Stop();
            Observability.RecordStage("Serialize", swStage.Elapsed.TotalMilliseconds, ("output_bytes", toReturn.Length));
        }

        // Return
        totalSw.Stop();
        int matchCount = sorted.Count(r => r != null && !r.NeedToStore && r.BucketId != 0);
        Console.WriteLine($"[Compress] DONE: {totalSw.ElapsedMilliseconds}ms total, in={_file.Length} out={toReturn.Length} ratio={toReturn.Length/(double)_file.Length:F3}, refs={referencesFound}, matches={matchCount}, stored={sorted.Count(r => r != null && r.NeedToStore)}");
        return (toReturn, referencesFound, totalChunks);
    }

    public async Task<byte[]> CompressFile(byte[] _file)
    {
        var res = await CompressFileWithStats(_file);
        return res.CompressedBytes;
    }

    public async Task<byte[]> DecompressFile(byte[] file)
    {
        using var rootSpan = Observability.StartStage("DecompressFile");
        if (file == null || file.Length == 0)
            throw new ArgumentException("Compressed input is empty.", nameof(file));

        var parseSw = Stopwatch.StartNew();
        var header = ParseHeader(file);
        parseSw.Stop();
        Observability.RecordStage("Deserialize", parseSw.Elapsed.TotalMilliseconds);

        if (!SupportedVersions.Contains(header.Version))
            throw new InvalidDataException($"Unsupported encoding version '{header.Version}'. Expected one of: {string.Join(", ", SupportedVersions)}.");

        int referencesOffset = header.PayloadStart;
        int errorOffset = referencesOffset + header.ReferencesLength;
        int trimOffset = errorOffset + header.ErrorLength;

        if (trimOffset > file.Length)
            throw new InvalidDataException("Compressed payload section lengths exceed file size.");
        if (header.ReferencesLength % (sizeof(ulong) * 2) != 0)
            throw new InvalidDataException("Reference section length is invalid.");

        int chunkCount = header.ReferencesLength / (sizeof(ulong) * 2);
        var references = new (ulong BucketId, ulong BucketIndex)[chunkCount];
        for (int i = 0; i < chunkCount; i++)
        {
            int off = referencesOffset + (i * sizeof(ulong) * 2);
            ulong bucketId = BitConverter.ToUInt64(file, off);
            ulong bucketIndex = BitConverter.ToUInt64(file, off + sizeof(ulong));
            references[i] = (bucketId, bucketIndex);
        }

        // ── Parse flat error encoding (v2.0.0) ──
        // <int totalPairCount> then <int deltaIndex><short delta> × totalPairCount
        (int deltaIndex, short delta)[] patches;
        using (var errorMs = new MemoryStream(file, errorOffset, header.ErrorLength, writable: false))
        using (var reader = new BinaryReader(errorMs, Encoding.UTF8, leaveOpen: true))
        {
            int totalPairCount = reader.ReadInt32();
            if (totalPairCount < 0)
                throw new InvalidDataException($"Invalid negative total pair count: {totalPairCount}");

            patches = new (int, short)[totalPairCount];
            for (int i = 0; i < totalPairCount; i++)
            {
                patches[i] = (reader.ReadInt32(), reader.ReadInt16());
            }
        }

        // ── Fetch all base chunks in parallel ──
        // BucketId is now a deterministic ulong derived from the 64-bit bitstring.
        // Convert BucketId → bitstring → RendezvousRouter.PickAgent() → query the correct agent.
        var fetchSw = Stopwatch.StartNew();
        var baseChunks = new byte[chunkCount][];
        var fetchTasks = new Task[chunkCount];
        int primaryHits = 0;
        int fallbackHits = 0;
        for (int i = 0; i < chunkCount; i++)
        {
            var reference = references[i];
            if (reference.BucketId == 0 && reference.BucketIndex == 0)
            {
                baseChunks[i] = new byte[Globals.chunkSize];
                fetchTasks[i] = Task.CompletedTask;
            }
            else
            {
                int idx = i;
                var refCopy = reference;
                fetchTasks[i] = Task.Run(async () =>
                {
                    // Derive bitstring from bucket ID → deterministic agent routing (stable by node name)
                    string bitstring = UlongToBitstring(refCopy.BucketId);
                    string targetAgent = RendezvousRouter.PickAgent(bitstring);

                    // 1) Try the primary (node-name-routed) agent
                    byte[]? chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(
                        refCopy.BucketId, refCopy.BucketIndex, targetAgent);

                    if (chunk != null)
                    {
                        Interlocked.Increment(ref primaryHits);
                    }
                    else
                    {
                        // CRITICAL: Only retry the SAME primary agent. NEVER query other agents.
                        // Other agents may have STALE data at the same (bucketId, bucketIndex)
                        // from a previous routing era — returning the WRONG chunk and causing
                        // silent data corruption (SHA256 mismatch).
                        for (int retry = 0; retry < 3; retry++)
                        {
                            await Task.Delay(100 * (retry + 1)); // 100ms, 200ms, 300ms
                            chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(
                                refCopy.BucketId, refCopy.BucketIndex, targetAgent);
                            if (chunk != null)
                            {
                                Interlocked.Increment(ref fallbackHits);
                                break;
                            }
                        }
                    }

                    if (chunk == null)
                        throw new InvalidDataException(
                            $"Missing base chunk for reference ({refCopy.BucketId}, {refCopy.BucketIndex}). " +
                            $"Agent={targetAgent}, Bitstring={bitstring}");

                    baseChunks[idx] = chunk;
                });
            }
        }
        await Task.WhenAll(fetchTasks);
        fetchSw.Stop();
        int zeroRefChunks = references.Count(r => r.BucketId == 0 && r.BucketIndex == 0);
        Console.WriteLine($"[Decompress] FetchBaseChunks: {fetchSw.ElapsedMilliseconds}ms, {chunkCount} chunks, primary={primaryHits}, fallback={fallbackHits}, zeroRef={zeroRefChunks}");
        Observability.RecordStage("FetchBaseChunks", fetchSw.Elapsed.TotalMilliseconds, ("chunk_count", chunkCount));

        // ── Stitch base chunks into one continuous buffer ──
        var applySw = Stopwatch.StartNew();
        byte[] baseBuffer = new byte[chunkCount * Globals.chunkSize];
        for (int i = 0; i < chunkCount; i++)
        {
            if (baseChunks[i].Length != Globals.chunkSize)
                throw new InvalidDataException(
                    $"Base chunk {i} has length {baseChunks[i].Length}, expected {Globals.chunkSize}.");
            Buffer.BlockCopy(baseChunks[i], 0, baseBuffer, i * Globals.chunkSize, Globals.chunkSize);
        }

        // ── Apply flat error encoding across the entire buffer ──
        int cursor = 0;
        foreach (var (deltaIndex, delta) in patches)
        {
            cursor += deltaIndex;
            if (cursor < 0 || cursor >= baseBuffer.Length)
                throw new InvalidDataException(
                    $"Patch cursor {cursor} out of range (buffer size {baseBuffer.Length}).");

            int patched = baseBuffer[cursor] + delta;
            if (patched < 0 || patched > 255)
                throw new InvalidDataException($"Patched byte {patched} out of range at cursor {cursor}.");
            baseBuffer[cursor] = (byte)patched;
        }

        // ── Assemble output: reconstructed buffer + trim chunk ──
        int trimLength;
        byte[]? expectedHash = null;

        if (header.TrimLength >= 0)
        {
            // v2.1.0+: trimLength is explicit in header; hash follows trim chunk
            trimLength = header.TrimLength;
            int hashOffset = trimOffset + trimLength;
            if (hashOffset + sizeof(int) <= file.Length)
            {
                int hashLen = BitConverter.ToInt32(file, hashOffset);
                if (hashLen > 0 && hashOffset + sizeof(int) + hashLen <= file.Length)
                {
                    expectedHash = new byte[hashLen];
                    Buffer.BlockCopy(file, hashOffset + sizeof(int), expectedHash, 0, hashLen);
                }
            }
        }
        else
        {
            // v2.0.0: no trimLength in header, trim runs to end of file
            trimLength = file.Length - trimOffset;
        }

        byte[] result = new byte[baseBuffer.Length + trimLength];
        Buffer.BlockCopy(baseBuffer, 0, result, 0, baseBuffer.Length);
        if (trimLength > 0)
            Buffer.BlockCopy(file, trimOffset, result, baseBuffer.Length, trimLength);

        // ── SHA256 integrity verification (v2.1.0+) ──
        if (expectedHash != null)
        {
            byte[] actualHash = SHA256.HashData(result);
            if (!actualHash.AsSpan().SequenceEqual(expectedHash))
            {
                Console.WriteLine($"[Decompress] ❌ INTEGRITY FAILURE: SHA256 mismatch. File is corrupted.");
                Console.WriteLine($"[Decompress]    Expected: {Convert.ToHexString(expectedHash)}");
                Console.WriteLine($"[Decompress]    Got:      {Convert.ToHexString(actualHash)}");
                throw new InvalidDataException(
                    "Decompression integrity check failed: SHA256 hash of reconstructed file does not match the original. " +
                    "This means at least one base chunk was corrupted or missing.");
            }
            Console.WriteLine($"[Decompress] ✅ SHA256 integrity verified");
        }

        applySw.Stop();
        Observability.RecordStage("ApplyPatchAndEgress", applySw.Elapsed.TotalMilliseconds,
            ("chunk_count", chunkCount), ("patches", patches.Length));

        return result;
    }

    public async Task<byte[]> _CompressFile(byte[] _file)
    {
        // CLMS
        await initCLMS("Cross", "A2");
        await addEvent("Preprocessing", "Starting compression process. Splitting file");

        Stopwatch sw = Stopwatch.StartNew();
        // Divide into (N) chunks.
        (List<byte[]> fileChunks, List<byte> trimmedChunk) = SplitChunks(_file, Globals.chunkSize);

        // Vectorize
        await addEvent("Preprocessing", "Vectorizing chunks");
        List<float[]> vectors = Misc.Compute64ElementLSHVectors(fileChunks);

        // Extract bitstring
        await addEvent("Preprocessing", "Extracting (bucket id) bitstring");
        List<string> bitStrings = Misc.ComputeBitStringFromVectors(vectors);

        // Search
        await addEvent("Preprocessing", "Preparing query request");

        List<QueryObject> queryObjects = new List<QueryObject>();
        for (int i = 0; i < vectors.Count; i++)
        {
            QueryObject qo = new QueryObject() { BucketString = bitStrings[i] };
            qo.Vector.AddRange(vectors[i]);
            qo.Chunk = ByteString.CopyFrom(fileChunks[i]);
            qo.Index = i;
            queryObjects.Add(qo);
        }

        await addEvent("Searching", $"Sending search request {DateTime.Now.ToString("HH:mm:ss tt")}");
        ConcurrentBag<QueryResponseObject> queryResponseObjects = new ConcurrentBag<QueryResponseObject>();

        // No cap: Use unlimited parallelism for chunk processing
        // System will naturally limit based on available CPU
        int baseParallelism = Environment.ProcessorCount * 2; // 2x for parallel processing
        int optimalParallelism = DynamicResourceManager.GetOptimalParallelism(baseParallelism);
        ParallelOptions options = new ParallelOptions() 
        { 
            MaxDegreeOfParallelism = -1 // -1 = unlimited, let system handle it
        };
        await Parallel.ForAsync(0, queryObjects.Count, options, async (i, ct) =>
        {
            QueryRequest request = new QueryRequest();
            request.HeadRouteID = headID;
            request.QueryObjects.Add(queryObjects[i]);

            QueryResponse response = await Globals.searchAllServiceClient.SearchAllAsync(request);
            for (int j = 0; j < response.Results.Count; j++)
            {
                queryResponseObjects.Add(response.Results[j]);
            }
        });

        // Sort results
        await addEvent("PostProcessing", $"Sorting results {DateTime.Now.ToString("HH:mm:ss tt")}");

        List<List<QueryResponseObject>> chunk_results = Misc.CreateList(vectors.Count, () => new List<QueryResponseObject>());
        foreach (var responseObject in queryResponseObjects)
        {
            chunk_results[responseObject.Index].Add(responseObject);
        }

        if (ClmsHandler._instance != null && !string.IsNullOrEmpty(headID))
        {
            _ = ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent()
            {
                level = "1",
                stepName = "PostProcessing",
                type = "step",
                message = $"Comparing results {DateTime.Now.ToString("HH:mm:ss tt")}"
            });
        }
        Console.WriteLine($"|Compare res|: final_res_len: {chunk_results.Count}");

        List<QueryResponseObject> final_results = Misc.CreateList(vectors.Count, () => new QueryResponseObject());
        List<List<(int key, int value)>> final_error_results = Misc.CreateList(vectors.Count, () => new List<(int, int)>());

        // DYNAMIC: Adjust parallelism based on current CPU and memory usage
        int baseParallelism2 = (int)(Environment.ProcessorCount * 0.75);
        int optimalParallelism2 = DynamicResourceManager.GetOptimalParallelism(baseParallelism2);
        var parallelOptions2 = new ParallelOptions
        {
            MaxDegreeOfParallelism = Math.Max(1, optimalParallelism2)
        };

        await Parallel.ForEachAsync(
            Enumerable.Range(0, chunk_results.Count),
            parallelOptions2,
            (i, ct) =>
            {
                var candidates = chunk_results[i];

                if (candidates.Count == 0)
                {
                    Console.WriteLine($"ERROR: Gateway didnt return a response for this index [{i}]");
                }
                else if (candidates.Count == 1)
                {
                    final_results[i] = candidates[0];
                }
                else
                {
                    // 1) Pre-convert all ByteStrings to byte[]
                    var chunkBytesArray = candidates
                        .Select(r => r.Chunk.ToByteArray())
                        .ToArray();

                    // 2) First pass: find best by error COUNT only
                    int bestErrorCount = Globals.chunkSize;
                    int bestIndex = -1;
                    for (int j = 0; j < chunkBytesArray.Length; j++)
                    {
                        var count = Misc.GetErrorEncoding(
                            fileChunks[i],
                            chunkBytesArray[j]
                        ).Count;

                        if (count < bestErrorCount)
                        {
                            bestErrorCount = count;
                            bestIndex = j;
                        }
                    }

                    // 3) Second pass: full encoding for the best candidate
                    var bestEncoding = Misc.GetErrorEncoding(
                        fileChunks[i],
                        chunkBytesArray[bestIndex]
                    );

                    final_results[i] = candidates[bestIndex];
                    final_error_results[i] = bestEncoding;
                }

                return ValueTask.CompletedTask;
            }
        );

        // Encode results
        if (ClmsHandler._instance != null && !string.IsNullOrEmpty(headID))
        {
            await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent()
            {
                level = "1",
                stepName = "Encoding",
                type = "step",
                message = "Encoding results"
            });
        }
        Console.WriteLine($"|Encode res|: final_res_len: {final_results.Count}");
        List<M_EncodedResult> encoded_objects = new List<M_EncodedResult>();
        for (int i = 0; i < final_results.Count; i++)
        {
            encoded_objects.Add(new M_EncodedResult()
            {
                bucket_id = final_results[i].BucketId,
                error_encoding = final_error_results[i]
            });
        }

        List<byte> first_bytes = new List<byte>();
        List<byte> error_bytes = new List<byte>();
        int error_bytes_offset = 0;
        for (int i = 0; i < encoded_objects.Count; i++)
        {
            first_bytes.AddRange(BitConverter.GetBytes(encoded_objects[i].bucket_id));
            (byte[], int) _errors = Misc.GetErrorEncodingBytes(encoded_objects[i].error_encoding, error_bytes_offset);
            error_bytes_offset = _errors.Item2;
            error_bytes.AddRange(_errors.Item1);
        }

        // Add Dictionary and trimming
        if (ClmsHandler._instance != null && !string.IsNullOrEmpty(headID))
        {
            await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent()
            {
                level = "1",
                stepName = "Encoding",
                type = "step",
                message = "Adding Dictionary and trimming"
            });
        }
        Console.WriteLine($"|Add Dict|: first_bytes: {first_bytes.Count}, error_bytes: {error_bytes.Count}");
        List<byte> output_bytes =
        [
            .. BitConverter.GetBytes((long)first_bytes.Count),
            .. BitConverter.GetBytes((long)error_bytes.Count),
            .. first_bytes,
            .. error_bytes,
            .. trimmedChunk
        ];

        // Return
        if (ClmsHandler._instance != null && !string.IsNullOrEmpty(headID))
        {
            await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent()
            {
                level = "1",
                stepName = "Final",
                type = "step",
                message = "Returning compressed file"
            });
            ClmsHandler._instance.routePoints[headID].Status = "Success";
            await ClmsHandler.SendRoutePoint(headID);
        }

        sw.Stop();
        Console.WriteLine($"Total compression time: {sw.ElapsedMilliseconds}ms");
        return output_bytes.ToArray();
    }

    public async Task<byte[]> _DecompressFile(byte[] file)
    {
        const int LongSize = sizeof(long);
        const int ULongSize = sizeof(ulong);
        const int IntSize = sizeof(int);

        if (file == null || file.Length < 2 * LongSize)
            throw new ArgumentException("Input too short to contain sizes", nameof(file));

        long refSize = BitConverter.ToInt64(file, 0);
        long errSize = BitConverter.ToInt64(file, LongSize);

        int refOffset = 2 * LongSize;
        int errOffset = refOffset + (int)refSize;
        if (file.Length < errOffset + errSize)
            throw new ArgumentException("Declared sizes exceed file length", nameof(file));

        var bucketRow = new Dictionary<ulong, ulong>();
        for (int i = 0; i + 2 * ULongSize <= refSize; i += 2 * ULongSize)
        {
            ulong bucketId = BitConverter.ToUInt64(file, refOffset + i);
            ulong rowId = BitConverter.ToUInt64(file, refOffset + i + ULongSize);
            bucketRow[bucketId] = rowId;
        }

        var errorEncoding = new Dictionary<int, int>();
        for (int i = 0; i + 2 * IntSize <= errSize; i += 2 * IntSize)
        {
            int index = BitConverter.ToInt32(file, errOffset + i);
            int offset = BitConverter.ToInt32(file, errOffset + i + IntSize);
            errorEncoding[index] = offset;
        }

        // Get chunks at bucket_id, row_id
        // Correct using error_encodings
        // Return
        return new byte[10];
    }
}
