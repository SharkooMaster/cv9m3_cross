
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
using ZstdSharp;

using Cross.Models;
using Cross.Services.Cache;
using Grpc.Core;

namespace Cross.Services.Cross;

public class CrossService : ICross
{
    // v2.0.0: Flat error encoding. One continuous delta stream across the entire stitched file.
    // v2.0.1: BucketId is now a deterministic ulong derived from the 64-bit bitstring (not auto-increment).
    // v2.1.0: SHA256 integrity hash appended after trim chunk. Decompression verifies output matches hash.
    //         Backwards compatible: v2.0.0 files without a hash are still decompressible (hash check skipped).
    // v3.0.0: Content-addressable references. Each ref is (bucketId, storageGuid) where storageGuid = SHA256(chunk).
    //         storageGuid is globally unique (content-addressable). Agents die/restart/scale → no data loss.
    //         Decompression uses GetChunkByKey(storageGuid) for O(1) lookup. Safe multi-agent fallback.
    // v3.1.0: Error dictionary is zstd-compressed. Header stores compressed length;
    //         decompression inflates before applying patches.
    // v5.0.0: Compact ref table with (bucketId, bucketIndex) as ulong pairs replaces inline 32-byte
    //         SHA256 storageGuids. Per-chunk refs use uint16 table indices into the ref table.
    //         Uses standard header (ParseHeader), zstd on error dict, SHA256 hash — same as v3.1.0.
    private const string EncodingVersion = "v5.0.0";
    private static readonly string[] SupportedVersions = { "v2.0.0", "v2.1.0", "v3.0.0", "v3.1.0", "v5.0.0" };
    string headID = "";
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

        // v3.1.0+: zstd-compress the error dictionary (RLE runs are highly compressible)
        byte[] errorPayload;
        if (version == "v3.1.0" || version == "v5.0.0")
        {
            using var compressor = new Compressor(3);
            errorPayload = compressor.Wrap(errorDictionary).ToArray();
            bw.Write(errorPayload.Length);      // compressed length (stored in header)
            bw.Write(errorDictionary.Length);    // original length (needed for decompression buffer)
        }
        else
        {
            errorPayload = errorDictionary;
            bw.Write(errorPayload.Length);
        }

        bw.Write(trimChunk.Length);
        bw.Write(references);
        bw.Write(errorPayload);
        bw.Write(trimChunk);
        bw.Write(originalFileHash.Length);   // 32 for SHA256
        bw.Write(originalFileHash);
        bw.Flush();

        return ms.ToArray();
    }

    /// <summary>
    /// v5.0.0: Build compact references as byte[] using a ref table + ushort indices.
    /// The caller passes this to BuildCompressedPayload which handles the standard header,
    /// error-dict zstd, SHA256 hash, etc.
    /// </summary>
    private static byte[] BuildV5References(
        QueryResponseObject?[] sorted,
        ConcurrentDictionary<int, MosaicChunkInfo> mosaicInfos)
    {
        int chunkCount = sorted.Length;

        var refTable = new List<(ulong BucketId, ulong BucketIndex)>();
        var refTableLookup = new Dictionary<(ulong, ulong), ushort>();

        ushort GetOrAddRef(ulong bucketId, ulong bucketKey)
        {
            var key = (bucketId, bucketKey);
            if (refTableLookup.TryGetValue(key, out ushort idx))
                return idx;
            ushort newIdx = (ushort)refTable.Count;
            refTable.Add(key);
            refTableLookup[key] = newIdx;
            return newIdx;
        }

        // Pre-populate ref table
        for (int i = 0; i < chunkCount; i++)
        {
            if (sorted[i]!.BucketId == 0) continue;
            if (mosaicInfos.TryGetValue(i, out var mosaic) && mosaic.Donors.Count > 0)
            {
                foreach (var (dBucketId, dBucketKey) in mosaic.Donors)
                    GetOrAddRef(dBucketId, dBucketKey);
            }
            else
            {
                GetOrAddRef(sorted[i]!.BucketId, sorted[i]!.BucketKey);
            }
        }

        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        if (refTable.Count > ushort.MaxValue)
            throw new InvalidOperationException(
                $"v5 ref table has {refTable.Count} entries, exceeding the ushort max of {ushort.MaxValue}. " +
                "This block cannot be encoded in v5 format.");

        // Header: chunkCount + refTableSize + ref table entries
        bw.Write(chunkCount);
        bw.Write((ushort)refTable.Count);
        foreach (var (bucketId, bucketIndex) in refTable)
        {
            bw.Write(bucketId);
            bw.Write(bucketIndex);
        }

        // Per-chunk references
        for (int i = 0; i < chunkCount; i++)
        {
            if (mosaicInfos.TryGetValue(i, out var mosaic) && mosaic.Donors.Count > 0)
            {
                bw.Write((byte)0x04);
                bw.Write((byte)mosaic.Donors.Count);
                foreach (var (dBucketId, dBucketKey) in mosaic.Donors)
                    bw.Write(GetOrAddRef(dBucketId, dBucketKey));
                bw.Write(mosaic.MatchBitmap);
                bw.Write(mosaic.Selectors);
                bw.Write(mosaic.DonorPositions);
                continue;
            }

            if (sorted[i]!.BucketId == 0)
            {
                bw.Write((byte)0x00);
                continue;
            }

            ushort tableIdx = GetOrAddRef(sorted[i]!.BucketId, sorted[i]!.BucketKey);
            if (sorted[i]!.NeedToStore)
            {
                bw.Write((byte)0x02);
                bw.Write(tableIdx);
            }
            else
            {
                bw.Write((byte)0x01);
                bw.Write(tableIdx);
            }
        }

        bw.Flush();
        return ms.ToArray();
    }

    private static (string Version, int ReferencesLength, int ErrorLength, int ErrorOriginalLength, int TrimLength, int PayloadStart) ParseHeader(ReadOnlySpan<byte> payload)
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

        // v3.1.0: error dictionary is zstd-compressed; header has [compressedLen][originalLen]
        int errorOriginalLength = errorLength; // for uncompressed versions, original == stored
        if (version == "v3.1.0" || version == "v5.0.0")
        {
            errorOriginalLength = BitConverter.ToInt32(payload.Slice(offset, IntSize));
            offset += IntSize;
        }

        // v2.1.0+: trim chunk length is stored in the header
        int trimLength = -1; // -1 means "not present" (v2.0.0 compat)
        if (version == "v2.1.0" || version == "v3.0.0" || version == "v3.1.0" || version == "v5.0.0")
        {
            trimLength = BitConverter.ToInt32(payload.Slice(offset, IntSize));
            offset += IntSize;
        }

        return (version, referencesLength, errorLength, errorOriginalLength, trimLength, offset);
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

    /// <summary>
    /// Pick the group member whose raw bytes are closest to the byte-wise mean.
    /// This minimizes the average diff size for all members diffing against the rep.
    /// O(G * chunkSize) — pure CPU, no allocations beyond one float[chunkSize] buffer.
    /// </summary>
    private static int PickRepresentative(List<byte[]> chunks, List<int> memberIndices)
    {
        int cs = Globals.chunkSize;

        float[] mean = new float[cs];
        foreach (var idx in memberIndices)
            for (int b = 0; b < cs; b++)
                mean[b] += chunks[idx][b];
        float inv = 1f / memberIndices.Count;
        for (int b = 0; b < cs; b++)
            mean[b] *= inv;

        int bestIdx = memberIndices[0];
        float bestDist = float.MaxValue;
        foreach (var idx in memberIndices)
        {
            float dist = 0f;
            for (int b = 0; b < cs; b++)
                dist += MathF.Abs(chunks[idx][b] - mean[b]);
            if (dist < bestDist)
            {
                bestDist = dist;
                bestIdx = idx;
            }
        }
        return bestIdx;
    }

    // Local/in-process helper: returns compressed bytes plus per-file reference stats
    public async Task<(byte[] CompressedBytes, int ReferencesFound, int TotalChunks, long DatacenterBytesStored)> CompressFileWithStats(byte[] _file)
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

        // ── ENTROPY COMPUTATION (Level 2 Mosaic gate) ──
        float[]? entropies = null;
        if (Globals.EnableMosaicDedup)
        {
            var swEntropy = Stopwatch.StartNew();
            entropies = Misc.ComputeEntropies(fileChunks);
            swEntropy.Stop();
            int highEntropyCount = entropies.Count(e => e >= Globals.MosaicEntropyThreshold);
            Console.WriteLine($"[Compress] Entropy: {swEntropy.ElapsedMilliseconds}ms, " +
                $"{highEntropyCount}/{fileChunks.Count} chunks above {Globals.MosaicEntropyThreshold:F1} threshold");
        }

        // ── LOCAL CHUNK CLUSTERING ──
        // Group chunks with identical LSH bitstrings. Chunks sharing a bitstring have
        // the same LSH projection signs → high vector similarity. We pick one
        // representative per group and only search/store representatives on agents.
        // Non-representatives diff against their representative's base chunk.
        // This captures intra-file dedup (impossible before — chunks aren't stored
        // when later chunks search) and reduces agent I/O by the grouping factor.
        var representativeSet = new HashSet<int>(fileChunks.Count);
        var groupRepresentative = new int[fileChunks.Count];

        if (Globals.EnableChunkClustering)
        {
            using var clusterStage = Observability.StartStage("ChunkClustering");
            var clusterSw = Stopwatch.StartNew();

            var bitstringGroups = new Dictionary<string, List<int>>(fileChunks.Count / 4);
            for (int i = 0; i < fileChunks.Count; i++)
            {
                if (!bitstringGroups.TryGetValue(bitStrings[i], out var group))
                {
                    group = new List<int>(4);
                    bitstringGroups[bitStrings[i]] = group;
                }
                group.Add(i);
            }

            foreach (var kv in bitstringGroups)
            {
                var members = kv.Value;
                if (members.Count == 1)
                {
                    groupRepresentative[members[0]] = members[0];
                    representativeSet.Add(members[0]);
                    continue;
                }

                int repIdx = PickRepresentative(fileChunks, members);
                representativeSet.Add(repIdx);
                foreach (var m in members)
                    groupRepresentative[m] = repIdx;
            }

            clusterSw.Stop();
            int totalGroups = bitstringGroups.Count;
            int singletons = bitstringGroups.Count(g => g.Value.Count == 1);
            Console.WriteLine($"[Compress] Clustering: {fileChunks.Count} chunks → {totalGroups} groups " +
                $"({singletons} singletons, {totalGroups - singletons} multi-member), " +
                $"{representativeSet.Count} representatives to search/store ({clusterSw.ElapsedMilliseconds}ms)");
            Observability.RecordStage("ChunkClustering", clusterSw.Elapsed.TotalMilliseconds,
                ("chunks", fileChunks.Count), ("groups", totalGroups), ("singletons", singletons),
                ("representatives", representativeSet.Count));
        }
        else
        {
            for (int i = 0; i < fileChunks.Count; i++)
            {
                groupRepresentative[i] = i;
                representativeSet.Add(i);
            }
        }

        // ── DIRECT-TO-AGENT SEARCH (bypasses gateway entirely) ──
        // CRITICAL: Each of the 65 neighbor buckets may live on a DIFFERENT agent.
        // We must route each bucket to its owning agent, then merge results per chunk.
        // Without this, we only search ~20% of the intended search space (1/N agents).
        Stopwatch sw = Stopwatch.StartNew();
        var phaseSw = Stopwatch.StartNew();

        float MIN_THRESH = Globals.SearchSimilarityThreshold;

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
        // Only compute for representatives — non-reps inherit their rep's result.
        phaseSw.Restart();
        var allBuckets = new List<string>[fileChunks.Count];     // 65 buckets per chunk
        var mainAgents = new string[fileChunks.Count];            // agent owning the main bucket
        var queryInfos = new (float[] vector, string bitString)[fileChunks.Count];

        Parallel.For(0, fileChunks.Count, i =>
        {
            queryInfos[i] = (vectors[i], bitStrings[i]);
            mainAgents[i] = RendezvousRouter.PickAgent(bitStrings[i]);
            if (representativeSet.Contains(i))
                allBuckets[i] = RendezvousRouter.GetNeighbouringBuckets(bitStrings[i], vectors[i]);
        });

        // Route each bucket to its owning agent (representatives only)
        var agentChunkBuckets = new Dictionary<string, Dictionary<int, List<string>>>();

        for (int i = 0; i < fileChunks.Count; i++)
        {
            if (!representativeSet.Contains(i)) continue;

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
        Console.WriteLine($"[Compress] Route: {phaseSw.ElapsedMilliseconds}ms, {representativeSet.Count} reps (of {fileChunks.Count} chunks) → {agentChunkBuckets.Count} agents");

        // ── Phase 2: Fire BatchGet per agent — each gets ONLY its own buckets ──
        var sorted = new QueryResponseObject?[fileChunks.Count];
        // Mosaic: collect top-K candidates per high-entropy chunk (keyed by chunk index)
        var mosaicCandidates = new ConcurrentDictionary<int, List<SearchVectorObject>>();
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
                                bool isHighEntropy = entropies != null
                                    && chunkIdx < entropies.Length
                                    && entropies[chunkIdx] >= Globals.MosaicEntropyThreshold;
                                var req = new SearchVector_Req
                                {
                                    Index = chunkIdx,
                                    MinimumSimilarity = MIN_THRESH,
                                    K = isHighEntropy ? Globals.MosaicTopK : 1
                                };
                                req.Vector.AddRange(vectors[chunkIdx]);
                                req.Bitstrings.AddRange(buckets);
                                batchReq.Queries.Add(req);
                                indexMap.Add(chunkIdx);
                            }

                            BatchSearchVector_Result? batchRes = null;
                            // Try with generous deadline; retry once on timeout
                            for (int attempt = 0; attempt < 2 && batchRes == null; attempt++)
                            {
                                try
                                {
                                    int deadlineSec = attempt == 0 ? 30 : 45;
                                    batchRes = await client.BatchGetAsync(batchReq,
                                        deadline: DateTime.UtcNow.AddSeconds(deadlineSec));
                                }
                                catch (global::Grpc.Core.RpcException rpcEx) when (rpcEx.StatusCode == global::Grpc.Core.StatusCode.DeadlineExceeded)
                                {
                                    if (attempt == 0)
                                    {
                                        Console.WriteLine($"[Compress] BatchGet to {agent} deadline ({batchReq.Queries.Count} queries), retrying...");
                                        await Task.Delay(500);
                                    }
                                    else
                                    {
                                        Console.WriteLine($"[Compress] BatchGet to {agent} deadline on retry, skipping batch");
                                    }
                                }
                            }

                            if (batchRes == null) return; // skip this batch — agent overloaded

                            // Map results — only update sorted[idx] if this agent found a BETTER match
                            for (int j = 0; j < indexMap.Count && j < batchRes!.Results.Count; j++)
                            {
                                var idx = indexMap[j];
                                var res = batchRes.Results[j];

                                if (!res.Save && res.Results.Count > 0)
                                {
                                    var best = res.Results[0];
                                    float sim = best.Similarity;
                                    if (sim >= MIN_THRESH && sim > bestSims[idx])
                                    {
                                        float current;
                                        do
                                        {
                                            current = Volatile.Read(ref bestSims[idx]);
                                            if (sim <= current) break;
                                        } while (Interlocked.CompareExchange(ref bestSims[idx], sim, current) != current);

                                        if (sim > current)
                                        {
                                            sorted[idx] = new QueryResponseObject
                                            {
                                                BucketId = best.BucketId,
                                                BucketKey = (ulong)best.BucketKey,
                                                Similarity = sim,
                                                Chunk = best.Chunk,
                                                Index = idx,
                                                Duplicate = true,
                                                NeedToStore = false,
                                                TargetAgent = agent,
                                                StorageGuid = best.StorageGuid ?? ""
                                            };
                                        }
                                    }

                                    // L1 match found, but also collect all K candidates for
                                    // potential mosaic fallback if bloat guard rejects the L1 diff
                                    if (res.Results.Count > 1)
                                    {
                                        var list = mosaicCandidates.GetOrAdd(idx, _ => new List<SearchVectorObject>());
                                        lock (list)
                                        {
                                            foreach (var cand in res.Results)
                                            {
                                                if (cand.Chunk != null && cand.Chunk.Length > 0)
                                                    list.Add(cand);
                                            }
                                        }
                                    }
                                }
                                // No L1 match — collect candidates for mosaic-only path
                                else if (res.Save && res.Results.Count > 1)
                                {
                                    var list = mosaicCandidates.GetOrAdd(idx, _ => new List<SearchVectorObject>());
                                    lock (list)
                                    {
                                        foreach (var cand in res.Results)
                                        {
                                            if (cand.Chunk != null && cand.Chunk.Length > 0)
                                                list.Add(cand);
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

        // Fill representatives that NO agent had a match for → need to store on main agent
        for (int i = 0; i < sorted.Length; i++)
        {
            if (!representativeSet.Contains(i)) continue;
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

        // Propagate representative results to non-representative group members.
        // Each non-rep shares the same agent reference as its rep — the diff encoding
        // will compute the byte-level difference against the rep's base chunk.
        for (int i = 0; i < sorted.Length; i++)
        {
            if (representativeSet.Contains(i)) continue;
            int rep = groupRepresentative[i];
            var repResult = sorted[rep]!;
            sorted[i] = new QueryResponseObject
            {
                BucketId = repResult.BucketId,
                BucketKey = repResult.BucketKey,
                Similarity = repResult.Similarity,
                Chunk = repResult.Chunk,
                Index = i,
                Duplicate = true,
                NeedToStore = false,
                TargetAgent = repResult.TargetAgent,
                StorageGuid = repResult.StorageGuid ?? ""
            };
        }

        // Release search-phase allocations early (async state machine keeps locals alive otherwise)
        allBuckets = null!;
        agentChunkBuckets = null!;

        // ── LEVEL 2 MOSAIC ASSEMBLY ──
        // For high-entropy chunks that failed Level 1 (NeedToStore=true + have mosaic candidates),
        // stitch a base chunk from donor sub-regions. The mosaic base replaces ByteString.Empty so
        // error encoding produces a smaller diff.
        var mosaicInfos = new ConcurrentDictionary<int, MosaicChunkInfo>();
        if (Globals.EnableMosaicDedup && mosaicCandidates.Count > 0)
        {
            var swMosaic = Stopwatch.StartNew();
            int subSize = Globals.MosaicSubChunkSize;
            int nComp = Globals.MosaicNComponents;

            Parallel.ForEach(mosaicCandidates, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, kv =>
            {
                int idx = kv.Key;
                var candidateList = kv.Value;

                if (sorted[idx] == null || !sorted[idx]!.NeedToStore) return;
                if (!chunkMap.TryGetValue(idx, out var srcChunk) || srcChunk == null) return;

                // Pre-convert all candidate ByteStrings to byte[] once (avoids 64 × K allocations)
                List<(byte[] bytes, string guid, ulong bucketId, ulong bucketKey)> preConverted;
                lock (candidateList)
                {
                    preConverted = new List<(byte[], string, ulong, ulong)>(candidateList.Count);
                    foreach (var cand in candidateList)
                    {
                        if (cand.Chunk == null || cand.Chunk.Length != srcChunk.Length) continue;
                        preConverted.Add((cand.Chunk.ToByteArray(), cand.StorageGuid ?? "", cand.BucketId, (ulong)cand.BucketKey));
                    }
                }
                if (preConverted.Count == 0) return;

                // Build mosaic: for each source sub-chunk, find the best byte match
                // at ANY position in any candidate (cross-position matching).
                var donors = new List<(ulong BucketId, ulong BucketKey, string StorageGuid, byte[] ChunkBytes)>();
                var donorIndex = new Dictionary<string, int>(); // storageGuid → index in donors
                var info = new MosaicChunkInfo();
                int chunkSize = srcChunk.Length;
                var stitched = new byte[chunkSize];
                int matched = 0;

                for (int e = 0; e < nComp && e * subSize < chunkSize; e++)
                {
                    int srcOffset = e * subSize;
                    int len = Math.Min(subSize, chunkSize - srcOffset);

                    int bestMatchBytes = -1;
                    byte[]? bestDonorChunk = null;
                    string bestGuid = "";
                    ulong bestBucketId = 0;
                    ulong bestBucketKey = 0;
                    int bestDonorPos = e;

                    foreach (var (candBytes, guid, bucketId, bucketKey) in preConverted)
                    {
                        for (int j = 0; j < nComp; j++)
                        {
                            int candOffset = j * subSize;
                            int candLen = Math.Min(subSize, candBytes.Length - candOffset);
                            if (candLen < len) continue;

                            int matchCount = 0;
                            for (int b = 0; b < len; b++)
                            {
                                if (srcChunk[srcOffset + b] == candBytes[candOffset + b])
                                    matchCount++;
                            }

                            if (matchCount > bestMatchBytes)
                            {
                                bestMatchBytes = matchCount;
                                bestDonorChunk = candBytes;
                                bestGuid = guid;
                                bestBucketId = bucketId;
                                bestBucketKey = bucketKey;
                                bestDonorPos = j;
                            }
                        }
                    }

                    if (bestMatchBytes > len / 2 && bestDonorChunk != null && !string.IsNullOrEmpty(bestGuid))
                    {
                        if (!donorIndex.TryGetValue(bestGuid, out int dIdx))
                        {
                            if (donors.Count >= 15) continue;
                            dIdx = donors.Count;
                            donorIndex[bestGuid] = dIdx;
                            donors.Add((bestBucketId, bestBucketKey, bestGuid, bestDonorChunk));
                        }
                        Buffer.BlockCopy(donors[dIdx].ChunkBytes, bestDonorPos * subSize, stitched, srcOffset, len);
                        info.MatchBitmap |= (1UL << e);
                        MosaicChunkInfo.SetSelector(info.Selectors, e, dIdx);
                        info.DonorPositions[e] = (byte)bestDonorPos;
                        matched++;
                    }
                }

                if (matched == 0 || donors.Count == 0) return;

                info.Donors = donors.Select(d => (d.BucketId, d.BucketKey)).ToList();
                info.StitchedBase = stitched;
                mosaicInfos[idx] = info;

                // Replace the empty base chunk with the stitched mosaic
                sorted[idx]!.Chunk = ByteString.CopyFrom(stitched);
            });

            swMosaic.Stop();
            Console.WriteLine($"[Compress] Mosaic L2: {swMosaic.ElapsedMilliseconds}ms, {mosaicInfos.Count}/{mosaicCandidates.Count} chunks assembled");
        }

        // ── GLOBAL LANE SEARCH (Level 2 sub-chunk index) ──
        // For chunks still needing storage after the top-K mosaic pass, query the lane
        // bucket index on ALL agents to find globally similar sub-chunks.
        if (Globals.EnableLaneSearch && Globals.EnableMosaicDedup)
        {
            var swLane = Stopwatch.StartNew();
            int subSize = Globals.MosaicSubChunkSize;
            int nComp = Globals.MosaicNComponents;
            int laneHashBits = Globals.LaneHashBits;
            int laneSaved = 0;

            // Collect chunks that still need storage and have source bytes available
            var laneTargets = new List<int>();
            for (int i = 0; i < sorted.Length; i++)
            {
                if (sorted[i] == null || !sorted[i]!.NeedToStore) continue;
                if (!chunkMap.TryGetValue(i, out var src) || src == null) continue;
                // Skip chunks that already have a full mosaic assembly
                if (mosaicInfos.ContainsKey(i)) continue;
                laneTargets.Add(i);
            }

            if (laneTargets.Count > 0)
            {
                // Compute lane hashes for all target chunks' sub-regions
                var allLaneQueries = new List<LaneQuery>();
                var queryToChunk = new List<(int chunkIdx, int laneIdx)>();

                foreach (int idx in laneTargets)
                {
                    var srcChunk = chunkMap[idx];
                    var laneHashes = Misc.ComputeLaneBitstrings(srcChunk, subSize, nComp, laneHashBits);

                    for (int lane = 0; lane < nComp && lane * subSize < srcChunk.Length; lane++)
                    {
                        int srcOffset = lane * subSize;
                        int len = Math.Min(subSize, srcChunk.Length - srcOffset);
                        var srcLaneBytes = new byte[len];
                        Buffer.BlockCopy(srcChunk, srcOffset, srcLaneBytes, 0, len);

                        allLaneQueries.Add(new LaneQuery
                        {
                            LaneHash = laneHashes[lane],
                            SourceLaneBytes = ByteString.CopyFrom(srcLaneBytes),
                            QueryIndex = allLaneQueries.Count
                        });
                        queryToChunk.Add((idx, lane));
                    }
                }

                if (allLaneQueries.Count > 0)
                {
                    // Send BatchSearchLanes to ALL agents in parallel
                    string[] agentIps;
                    try { agentIps = RendezvousRouter.GetAllAgentIps(); }
                    catch { agentIps = new[] { Globals.AgentsLoadbalancer }; }

                    var allMatches = new ConcurrentBag<(int chunkIdx, int laneIdx, LaneMatch match)>();

                    var laneTasks = agentIps.Select(agentIp => Task.Run(async () =>
                    {
                        try
                        {
                            var client = GrpcChannelFactory.GetClient(
                                target: agentIp,
                                ctor: chan => new SearchLanes.SearchLanesClient(chan),
                                roundRobin: false, port: 5000);

                            var req = new BatchSearchLanesReq();
                            req.Queries.AddRange(allLaneQueries);

                            var res = await client.BatchSearchAsync(req,
                                deadline: DateTime.UtcNow.AddSeconds(30));

                            foreach (var qr in res.Results)
                            {
                                if (qr == null || qr.Matches.Count == 0) continue;
                                int qi = qr.QueryIndex;
                                if (qi < 0 || qi >= queryToChunk.Count) continue;
                                var (chunkIdx, laneIdx) = queryToChunk[qi];

                                foreach (var m in qr.Matches)
                                    allMatches.Add((chunkIdx, laneIdx, m));
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Compress] Lane search to {agentIp} failed: {ex.Message}");
                        }
                    })).ToArray();

                    Task.WaitAll(laneTasks);

                    // Group matches by chunk and assemble mosaic from globally-matched donors
                    var matchesByChunk = allMatches
                        .GroupBy(m => m.chunkIdx)
                        .ToDictionary(g => g.Key, g => g.ToList());

                    Parallel.ForEach(matchesByChunk, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, kv =>
                    {
                        int idx = kv.Key;
                        var chunkMatches = kv.Value;
                        if (!chunkMap.TryGetValue(idx, out var srcChunk) || srcChunk == null) return;

                        var donors = new List<(ulong BucketId, ulong BucketKey, byte[] ChunkBytes)>();
                        var donorIndex = new Dictionary<string, int>();
                        var info = new MosaicChunkInfo();
                        int chunkSize = srcChunk.Length;
                        var stitched = new byte[chunkSize];
                        int matched = 0;

                        // Group matches by lane index, pick best per lane
                        var byLane = chunkMatches.GroupBy(m => m.laneIdx);
                        foreach (var laneGroup in byLane)
                        {
                            int e = laneGroup.Key;
                            int srcOffset = e * subSize;
                            int len = Math.Min(subSize, chunkSize - srcOffset);
                            if (len <= 0) continue;

                            int bestMatchBytes = len / 2; // threshold: >50% match
                            LaneMatch? bestMatch = null;

                            foreach (var (_, _, m) in laneGroup)
                            {
                                if (m.DonorLaneBytes == null || m.DonorLaneBytes.Length < len) continue;
                                var donorBytes = m.DonorLaneBytes.ToByteArray();

                                int matchCount = 0;
                                for (int b = 0; b < len; b++)
                                {
                                    if (srcChunk[srcOffset + b] == donorBytes[b])
                                        matchCount++;
                                }

                                if (matchCount > bestMatchBytes)
                                {
                                    bestMatchBytes = matchCount;
                                    bestMatch = m;
                                }
                            }

                            if (bestMatch != null && !string.IsNullOrEmpty(bestMatch.StorageGuid))
                            {
                                string guid = bestMatch.StorageGuid;
                                if (!donorIndex.TryGetValue(guid, out int dIdx))
                                {
                                    if (donors.Count >= 15) continue;
                                    dIdx = donors.Count;
                                    donorIndex[guid] = dIdx;
                                    donors.Add((bestMatch.BucketId, bestMatch.BucketKey, Array.Empty<byte>()));
                                }

                                var donorLane = bestMatch.DonorLaneBytes.ToByteArray();
                                Buffer.BlockCopy(donorLane, 0, stitched, srcOffset, len);
                                info.MatchBitmap |= (1UL << e);
                                MosaicChunkInfo.SetSelector(info.Selectors, e, dIdx);
                                info.DonorPositions[e] = (byte)bestMatch.LanePosition;
                                matched++;
                            }
                        }

                        if (matched == 0 || donors.Count == 0) return;

                        info.Donors = donors.Select(d => (d.BucketId, d.BucketKey)).ToList();
                        info.StitchedBase = stitched;
                        mosaicInfos[idx] = info;

                        sorted[idx]!.NeedToStore = false;
                        sorted[idx]!.Chunk = ByteString.CopyFrom(stitched);
                        Interlocked.Increment(ref laneSaved);
                    });
                }
            }

            swLane.Stop();
            Console.WriteLine($"[Compress] Lane L2 global: {swLane.ElapsedMilliseconds}ms, {laneSaved}/{laneTargets.Count} chunks rescued via global lane search");
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
                                    deadline: DateTime.UtcNow.AddSeconds(30));

                                int freshlyStored = 0;
                                int dedupedAtStore = 0;
                                for (int j = 0; j < batchItems.Count && j < batchRes.Results.Count; j++)
                                {
                                    var storeRes = batchRes.Results[j];
                                    batchItems[j].response.BucketId = storeRes.Id;
                                    batchItems[j].response.BucketKey = storeRes.Index;
                                    batchItems[j].response.StorageGuid = storeRes.StorageGuid ?? "";

                                    if (storeRes.WasDeduplicated)
                                    {
                                        batchItems[j].response.NeedToStore = false;
                                        batchItems[j].response.Duplicate = true;
                                        batchItems[j].response.Similarity = storeRes.Similarity;
                                        if (storeRes.BaseChunk != null && storeRes.BaseChunk.Length > 0)
                                            batchItems[j].response.Chunk = storeRes.BaseChunk;
                                        dedupedAtStore++;
                                    }
                                    else
                                    {
                                        freshlyStored++;
                                    }
                                }
                                Interlocked.Add(ref totalStored, freshlyStored);
                                if (dedupedAtStore > 0)
                                    Console.WriteLine($"[Compress] Store-time dedup on {agent}: {dedupedAtStore} chunks matched existing entries");
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[Compress] BatchStore to {agent} failed: {ex.Message}, retrying SAME agent...");
                                try
                                {
                                    await Task.Delay(500);
                                    var retryClient = GrpcChannelFactory.GetClient(
                                        target: agent,
                                        ctor: chan => new StoreVector.StoreVectorClient(chan),
                                        roundRobin: false, port: 5000);
                                    var retryRes = await retryClient.BatchStoreAsync(batchReq,
                                        deadline: DateTime.UtcNow.AddSeconds(30));
                                    int freshlyStored = 0;
                                    for (int j = 0; j < batchItems.Count && j < retryRes.Results.Count; j++)
                                    {
                                        var storeRes = retryRes.Results[j];
                                        batchItems[j].response.BucketId = storeRes.Id;
                                        batchItems[j].response.BucketKey = storeRes.Index;
                                        batchItems[j].response.StorageGuid = storeRes.StorageGuid ?? "";

                                        if (storeRes.WasDeduplicated)
                                        {
                                            batchItems[j].response.NeedToStore = false;
                                            batchItems[j].response.Duplicate = true;
                                            batchItems[j].response.Similarity = storeRes.Similarity;
                                            if (storeRes.BaseChunk != null && storeRes.BaseChunk.Length > 0)
                                                batchItems[j].response.Chunk = storeRes.BaseChunk;
                                        }
                                        else
                                        {
                                            freshlyStored++;
                                        }
                                    }
                                    Interlocked.Add(ref totalStored, freshlyStored);
                                    Console.WriteLine($"[Compress] Retry store to {agent} OK for {batchItems.Count} chunks");
                                }
                                catch (Exception retryEx)
                                {
                                    Console.WriteLine($"[Compress] Retry store to {agent} also failed: {retryEx.Message}");
                                    foreach (var item in batchItems)
                                    { item.response.BucketId = 0; item.response.BucketKey = 0; item.response.StorageGuid = ""; item.response.Chunk = ByteString.Empty; }
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

        // ── Post-store: clean up mosaic entries for chunks deduped at store time ──
        // If the agent matched an existing stored chunk, the error encoding base is now
        // the agent's matched chunk (storeRes.BaseChunk), NOT the stitched mosaic.
        // Remove from mosaicInfos so the reference builder writes 0x01 instead of 0x03.
        if (mosaicInfos.Count > 0)
        {
            foreach (var idx in mosaicInfos.Keys.ToList())
            {
                if (sorted[idx] != null && !sorted[idx]!.NeedToStore)
                    mosaicInfos.TryRemove(idx, out _);
            }
        }

        // ── Post-store: propagate updated store results to non-rep group members ──
        // After BatchStore, representatives now have final BucketId/BucketKey/StorageGuid.
        // Non-reps need these values updated so their references point to the stored rep.
        if (Globals.EnableChunkClustering)
        {
            for (int i = 0; i < sorted.Length; i++)
            {
                if (representativeSet.Contains(i)) continue;
                int rep = groupRepresentative[i];
                var repResult = sorted[rep]!;
                sorted[i].BucketId = repResult.BucketId;
                sorted[i].BucketKey = repResult.BucketKey;
                sorted[i].StorageGuid = repResult.StorageGuid ?? "";
                sorted[i].Chunk = repResult.Chunk;
            }
        }

        // NOTE: "referencesFound" is computed AFTER BloatRestore (see below)
        // to reflect actual dedup — chunks that truly reuse an existing base
        // and do NOT need to be re-stored. Initial LSH match count is tracked
        // separately as "initialMatches" for diagnostics.
        int initialMatches = sorted.Count(r => r != null && r.Duplicate && r.Similarity < 1.0f);
        int totalChunks = sorted.Length;

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

                // Only skip diff/fetch for chunks WE stored (NeedToStore) — their storageGuid
                // points to the original bytes. For search matches (even similarity ≈ 1.0),
                // we MUST use the agent's base chunk: vector similarity ≠ byte identity!
                bool skipDiff = sorted[i].NeedToStore && sorted[i].BucketId != 0;

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
                            var storeRes = await client.StoreAsync(storeReq, new CallOptions(deadline: DateTime.UtcNow.AddSeconds(30)));
                            item.resp.BucketId = storeRes.Id;
                            item.resp.BucketKey = storeRes.Index;
                            item.resp.StorageGuid = storeRes.StorageGuid ?? "";
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

            // ── Bloat guard pre-pass ──
            // Detect chunks whose diff would be larger than the raw chunk.
            // Two thresholds: standard for regular search matches, and a more lenient
            // one for clustered non-reps. Ejecting a non-rep adds to datacenter storage
            // (new store), while keeping it only increases client .ccf diff size.
            // Since datacenter growth is the priority metric, we tolerate bigger diffs
            // for non-reps before ejecting.
            int maxAllowedRegular = (int)(Globals.chunkSize * Globals.BloatGuardThreshold);
            int maxAllowedCluster = (int)(Globals.chunkSize * Globals.ClusterBloatGuardThreshold);
            for (int i = 0; i < sorted.Length; i++)
            {
                if (sorted[i].BucketId == 0 && sorted[i].BucketKey == 0)
                {
                    zeroRefCount++;
                    continue;
                }

                if (sorted[i].NeedToStore && sorted[i].BucketId != 0)
                {
                    emptyDiffCount++;
                    continue;
                }

                byte[] baseChunk;
                bool isNonRep = !representativeSet.Contains(i);
                int rep = groupRepresentative[i];

                if (isNonRep && sorted[rep].NeedToStore && sorted[rep].BucketId != 0)
                    baseChunk = fileChunks[rep];
                else if (sorted[i].Chunk != null && sorted[i].Chunk.Length > 0)
                    baseChunk = sorted[i].Chunk.ToByteArray();
                else
                    baseChunk = new byte[Globals.chunkSize];

                int differingByteCount = 0;
                for (int j = 0; j < fileChunks[i].Length; j++)
                {
                    if (fileChunks[i][j] != baseChunk[j])
                        differingByteCount++;
                }

                int threshold = isNonRep ? maxAllowedCluster : maxAllowedRegular;
                if (differingByteCount > threshold)
                {
                    sorted[i].NeedToStore = true;
                    sorted[i].Similarity = 1.0f;
                    sorted[i].TargetAgent = mainAgents[i];
                    bloatedDiffRestore.Add(i);
                    emptyDiffCount++;
                }
                else
                {
                    if (differingByteCount == 0) emptyDiffCount++;
                    else diffCount++;
                }
            }
            phaseSw.Stop();
            Console.WriteLine($"[Compress] DiffEncode: {phaseSw.ElapsedMilliseconds}ms, empty={emptyDiffCount}, diffs={diffCount}, zeroRef={zeroRefCount}, bloated={bloatedDiffRestore.Count}");
            Observability.RecordStage("DiffEncode", phaseSw.Elapsed.TotalMilliseconds,
                ("chunk_count", sorted.Length), ("empty_diff", emptyDiffCount), ("non_empty_diff", diffCount),
                ("zero_ref", zeroRefCount), ("bloated_restore", bloatedDiffRestore.Count));
        }

        // ── MOSAIC FALLBACK for bloat-rejected chunks ──
        // Chunks that had an L1 match but failed the bloat guard. If they have mosaic candidates,
        // try sub-chunk assembly. If the mosaic base produces a diff under the bloat threshold,
        // use it instead of re-storing (saves datacenter storage).
        if (Globals.EnableMosaicDedup && bloatedDiffRestore.Count > 0 && mosaicCandidates.Count > 0)
        {
            var swMosaicFallback = Stopwatch.StartNew();
            int subSize = Globals.MosaicSubChunkSize;
            int nComp = Globals.MosaicNComponents;
            int mosaicSaved = 0;
            var rescued = new ConcurrentBag<int>();

            Parallel.ForEach(bloatedDiffRestore, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, i =>
            {
                if (!mosaicCandidates.TryGetValue(i, out var candidateList)) return;
                if (!chunkMap.TryGetValue(i, out var srcChunk) || srcChunk == null) return;

                List<(byte[] bytes, string guid, ulong bucketId, ulong bucketKey)> preConverted;
                lock (candidateList)
                {
                    preConverted = new List<(byte[], string, ulong, ulong)>(candidateList.Count);
                    foreach (var cand in candidateList)
                    {
                        if (cand.Chunk == null || cand.Chunk.Length != srcChunk.Length) continue;
                        preConverted.Add((cand.Chunk.ToByteArray(), cand.StorageGuid ?? "", cand.BucketId, (ulong)cand.BucketKey));
                    }
                }
                if (preConverted.Count == 0) return;

                var donors = new List<(ulong BucketId, ulong BucketKey, string StorageGuid, byte[] ChunkBytes)>();
                var donorIndex = new Dictionary<string, int>();
                var info = new MosaicChunkInfo();
                int chunkSize = srcChunk.Length;
                var stitched = new byte[chunkSize];
                int matched = 0;

                for (int e = 0; e < nComp && e * subSize < chunkSize; e++)
                {
                    int srcOffset = e * subSize;
                    int len = Math.Min(subSize, chunkSize - srcOffset);

                    int bestMatchBytes = -1;
                    byte[]? bestDonorChunk = null;
                    string bestGuid = "";
                    ulong bestBucketId = 0;
                    ulong bestBucketKey = 0;
                    int bestDonorPos = e;

                    foreach (var (candBytes, guid, bucketId, bucketKey) in preConverted)
                    {
                        for (int cj = 0; cj < nComp; cj++)
                        {
                            int candOffset = cj * subSize;
                            int candLen = Math.Min(subSize, candBytes.Length - candOffset);
                            if (candLen < len) continue;

                            int matchCount = 0;
                            for (int b = 0; b < len; b++)
                            {
                                if (srcChunk[srcOffset + b] == candBytes[candOffset + b])
                                    matchCount++;
                            }
                            if (matchCount > bestMatchBytes)
                            {
                                bestMatchBytes = matchCount;
                                bestDonorChunk = candBytes;
                                bestGuid = guid;
                                bestBucketId = bucketId;
                                bestBucketKey = bucketKey;
                                bestDonorPos = cj;
                            }
                        }
                    }

                    if (bestMatchBytes > len / 2 && bestDonorChunk != null && !string.IsNullOrEmpty(bestGuid))
                    {
                        if (!donorIndex.TryGetValue(bestGuid, out int dIdx))
                        {
                            if (donors.Count >= 15) continue;
                            dIdx = donors.Count;
                            donorIndex[bestGuid] = dIdx;
                            donors.Add((bestBucketId, bestBucketKey, bestGuid, bestDonorChunk));
                        }
                        Buffer.BlockCopy(donors[dIdx].ChunkBytes, bestDonorPos * subSize, stitched, srcOffset, len);
                        info.MatchBitmap |= (1UL << e);
                        MosaicChunkInfo.SetSelector(info.Selectors, e, dIdx);
                        info.DonorPositions[e] = (byte)bestDonorPos;
                        matched++;
                    }
                }

                if (matched == 0 || donors.Count == 0) return;

                // Check if the mosaic base actually produces a diff under the bloat threshold
                int differingBytes = 0;
                for (int j = 0; j < srcChunk.Length; j++)
                {
                    if (srcChunk[j] != stitched[j])
                        differingBytes++;
                }

                int maxAllowed = (int)(Globals.chunkSize * Globals.BloatGuardThreshold);
                if (differingBytes >= maxAllowed) return; // mosaic didn't help enough

                info.Donors = donors.Select(d => (d.BucketId, d.BucketKey)).ToList();
                info.StitchedBase = stitched;
                mosaicInfos[i] = info;

                // Undo the bloat: restore to dedup state with mosaic base
                sorted[i]!.NeedToStore = false;
                sorted[i]!.Chunk = ByteString.CopyFrom(stitched);
                rescued.Add(i);
                Interlocked.Increment(ref mosaicSaved);
            });

            // Remove rescued chunks from bloatedDiffRestore
            if (rescued.Count > 0)
            {
                var rescuedSet = new HashSet<int>(rescued);
                bloatedDiffRestore.RemoveAll(idx => rescuedSet.Contains(idx));
                diffCount += rescued.Count;
                emptyDiffCount -= rescued.Count;
            }

            swMosaicFallback.Stop();
            Console.WriteLine($"[Compress] Mosaic L2 fallback: {swMosaicFallback.ElapsedMilliseconds}ms, {mosaicSaved}/{bloatedDiffRestore.Count + mosaicSaved} bloated chunks rescued");
        }

        // ── LANE SEARCH FALLBACK for remaining bloated chunks ──
        if (Globals.EnableLaneSearch && bloatedDiffRestore.Count > 0)
        {
            var swLaneFallback = Stopwatch.StartNew();
            int subSize = Globals.MosaicSubChunkSize;
            int nComp = Globals.MosaicNComponents;
            int laneHashBits = Globals.LaneHashBits;
            int laneFallbackSaved = 0;

            // Collect bloated chunks that still need re-storing
            var fallbackTargets = new List<int>();
            foreach (var i in bloatedDiffRestore)
            {
                if (!chunkMap.TryGetValue(i, out var src) || src == null) continue;
                if (mosaicInfos.ContainsKey(i)) continue; // already rescued by top-K mosaic fallback
                fallbackTargets.Add(i);
            }

            if (fallbackTargets.Count > 0)
            {
                var allLaneQueries = new List<LaneQuery>();
                var queryToChunk = new List<(int chunkIdx, int laneIdx)>();

                foreach (int idx in fallbackTargets)
                {
                    var srcChunk = chunkMap[idx];
                    var laneHashes = Misc.ComputeLaneBitstrings(srcChunk, subSize, nComp, laneHashBits);

                    for (int lane = 0; lane < nComp && lane * subSize < srcChunk.Length; lane++)
                    {
                        int srcOffset = lane * subSize;
                        int len = Math.Min(subSize, srcChunk.Length - srcOffset);
                        var srcLaneBytes = new byte[len];
                        Buffer.BlockCopy(srcChunk, srcOffset, srcLaneBytes, 0, len);

                        allLaneQueries.Add(new LaneQuery
                        {
                            LaneHash = laneHashes[lane],
                            SourceLaneBytes = ByteString.CopyFrom(srcLaneBytes),
                            QueryIndex = allLaneQueries.Count
                        });
                        queryToChunk.Add((idx, lane));
                    }
                }

                if (allLaneQueries.Count > 0)
                {
                    string[] agentIps;
                    try { agentIps = RendezvousRouter.GetAllAgentIps(); }
                    catch { agentIps = new[] { Globals.AgentsLoadbalancer }; }

                    var allMatches = new ConcurrentBag<(int chunkIdx, int laneIdx, LaneMatch match)>();
                    var laneTasks = agentIps.Select(agentIp => Task.Run(async () =>
                    {
                        try
                        {
                            var client = GrpcChannelFactory.GetClient(
                                target: agentIp,
                                ctor: chan => new SearchLanes.SearchLanesClient(chan),
                                roundRobin: false, port: 5000);

                            var req = new BatchSearchLanesReq();
                            req.Queries.AddRange(allLaneQueries);

                            var res = await client.BatchSearchAsync(req,
                                deadline: DateTime.UtcNow.AddSeconds(30));

                            foreach (var qr in res.Results)
                            {
                                if (qr == null || qr.Matches.Count == 0) continue;
                                int qi = qr.QueryIndex;
                                if (qi < 0 || qi >= queryToChunk.Count) continue;
                                var (chunkIdx, laneIdx) = queryToChunk[qi];
                                foreach (var m in qr.Matches)
                                    allMatches.Add((chunkIdx, laneIdx, m));
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Compress] Lane fallback search to {agentIp} failed: {ex.Message}");
                        }
                    })).ToArray();

                    Task.WaitAll(laneTasks);

                    var matchesByChunk = allMatches
                        .GroupBy(m => m.chunkIdx)
                        .ToDictionary(g => g.Key, g => g.ToList());

                    var rescued = new ConcurrentBag<int>();

                    Parallel.ForEach(matchesByChunk, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, kv =>
                    {
                        int idx = kv.Key;
                        var chunkMatches = kv.Value;
                        if (!chunkMap.TryGetValue(idx, out var srcChunk) || srcChunk == null) return;

                        var donors = new List<(ulong BucketId, ulong BucketKey, byte[] ChunkBytes)>();
                        var donorIndex = new Dictionary<string, int>();
                        var info = new MosaicChunkInfo();
                        int chunkSize = srcChunk.Length;
                        var stitched = new byte[chunkSize];
                        int matched = 0;

                        var byLane = chunkMatches.GroupBy(m => m.laneIdx);
                        foreach (var laneGroup in byLane)
                        {
                            int e = laneGroup.Key;
                            int srcOffset = e * subSize;
                            int len = Math.Min(subSize, chunkSize - srcOffset);
                            if (len <= 0) continue;

                            int bestMatchBytes = len / 2;
                            LaneMatch? bestMatch = null;

                            foreach (var (_, _, m) in laneGroup)
                            {
                                if (m.DonorLaneBytes == null || m.DonorLaneBytes.Length < len) continue;
                                var donorBytes = m.DonorLaneBytes.ToByteArray();

                                int matchCount = 0;
                                for (int b = 0; b < len; b++)
                                {
                                    if (srcChunk[srcOffset + b] == donorBytes[b])
                                        matchCount++;
                                }

                                if (matchCount > bestMatchBytes)
                                {
                                    bestMatchBytes = matchCount;
                                    bestMatch = m;
                                }
                            }

                            if (bestMatch != null && !string.IsNullOrEmpty(bestMatch.StorageGuid))
                            {
                                string guid = bestMatch.StorageGuid;
                                if (!donorIndex.TryGetValue(guid, out int dIdx))
                                {
                                    if (donors.Count >= 15) continue;
                                    dIdx = donors.Count;
                                    donorIndex[guid] = dIdx;
                                    donors.Add((bestMatch.BucketId, bestMatch.BucketKey, Array.Empty<byte>()));
                                }

                                var donorLane = bestMatch.DonorLaneBytes.ToByteArray();
                                Buffer.BlockCopy(donorLane, 0, stitched, srcOffset, len);
                                info.MatchBitmap |= (1UL << e);
                                MosaicChunkInfo.SetSelector(info.Selectors, e, dIdx);
                                info.DonorPositions[e] = (byte)bestMatch.LanePosition;
                                matched++;
                            }
                        }

                        if (matched == 0 || donors.Count == 0) return;

                        // Verify the mosaic base actually improves the diff
                        int differingBytes = 0;
                        for (int j = 0; j < srcChunk.Length; j++)
                        {
                            if (srcChunk[j] != stitched[j]) differingBytes++;
                        }
                        int maxAllowed = (int)(Globals.chunkSize * Globals.BloatGuardThreshold);
                        if (differingBytes >= maxAllowed) return;

                        info.Donors = donors.Select(d => (d.BucketId, d.BucketKey)).ToList();
                        info.StitchedBase = stitched;
                        mosaicInfos[idx] = info;

                        sorted[idx]!.NeedToStore = false;
                        sorted[idx]!.Chunk = ByteString.CopyFrom(stitched);
                        rescued.Add(idx);
                        Interlocked.Increment(ref laneFallbackSaved);
                    });

                    // Remove rescued chunks from bloatedDiffRestore
                    if (rescued.Count > 0)
                    {
                        var rescuedSet = new HashSet<int>(rescued);
                        bloatedDiffRestore.RemoveAll(i => rescuedSet.Contains(i));
                        emptyDiffCount -= rescued.Count;
                    }
                }
            }

            swLaneFallback.Stop();
            Console.WriteLine($"[Compress] Lane L2 fallback: {swLaneFallback.ElapsedMilliseconds}ms, {laneFallbackSaved}/{fallbackTargets.Count} bloated chunks rescued via global lane search");
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
                                deadline: DateTime.UtcNow.AddSeconds(30));

                            for (int j = 0; j < batchItems.Count && j < batchRes.Results.Count; j++)
                            {
                                var storeRes = batchRes.Results[j];
                                batchItems[j].response.BucketId = storeRes.Id;
                                batchItems[j].response.BucketKey = storeRes.Index;
                                batchItems[j].response.StorageGuid = storeRes.StorageGuid ?? "";

                                if (storeRes.WasDeduplicated)
                                {
                                    batchItems[j].response.NeedToStore = false;
                                    batchItems[j].response.Duplicate = true;
                                    batchItems[j].response.Similarity = storeRes.Similarity;
                                    if (storeRes.BaseChunk != null && storeRes.BaseChunk.Length > 0)
                                        batchItems[j].response.Chunk = storeRes.BaseChunk;
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Compress] Re-store to {agent} failed: {ex.Message}, retrying SAME agent...");
                            try
                            {
                                await Task.Delay(500);
                                var retryClient = GrpcChannelFactory.GetClient(
                                    target: agent,
                                    ctor: chan => new StoreVector.StoreVectorClient(chan),
                                    roundRobin: false, port: 5000);
                                var retryRes = await retryClient.BatchStoreAsync(batchReq,
                                    deadline: DateTime.UtcNow.AddSeconds(30));
                                for (int j = 0; j < batchItems.Count && j < retryRes.Results.Count; j++)
                                {
                                    var storeRes = retryRes.Results[j];
                                    batchItems[j].response.BucketId = storeRes.Id;
                                    batchItems[j].response.BucketKey = storeRes.Index;
                                    batchItems[j].response.StorageGuid = storeRes.StorageGuid ?? "";

                                    if (storeRes.WasDeduplicated)
                                    {
                                        batchItems[j].response.NeedToStore = false;
                                        batchItems[j].response.Duplicate = true;
                                        batchItems[j].response.Similarity = storeRes.Similarity;
                                        if (storeRes.BaseChunk != null && storeRes.BaseChunk.Length > 0)
                                            batchItems[j].response.Chunk = storeRes.BaseChunk;
                                    }
                                }
                                Console.WriteLine($"[Compress] Retry re-store to {agent} OK for {batchItems.Count} chunks");
                            }
                            catch (Exception retryEx)
                            {
                                Console.WriteLine(
                                    $"[Compress] WARN: Bloat re-store FAILED for {batchItems.Count} chunks on agent {agent} " +
                                    $"after 2 attempts: {retryEx.Message}. " +
                                    $"Chunks will use zero-ref (full diff encoding) — output file will be larger than optimal.");
                                foreach (var item in batchItems)
                                { item.response.BucketId = 0; item.response.BucketKey = 0; item.response.StorageGuid = ""; item.response.Chunk = ByteString.Empty; }
                            }
                        }
                    }));
                }
            }

            await Task.WhenAll(reStoreTasks);

            // ── CRITICAL: Sweep bloated chunks for any that ended up with BucketId=0 ──
            // This catches individual store failures within a successful batch (agent returns Id=0
            // for one item but succeeds for others). Without this, sorted[i].Chunk still has the old
            // matched bytes but the zero BucketId makes it a zero-ref → base mismatch → corruption.
            foreach (var i in bloatedDiffRestore)
            {
                if (sorted[i].BucketId == 0)
                {
                    sorted[i].Chunk = ByteString.Empty;
                    sorted[i].StorageGuid = "";
                }
            }

            phaseSw.Stop();
            Console.WriteLine($"[Compress] BloatRestore: {phaseSw.ElapsedMilliseconds}ms, {bloatedDiffRestore.Count} chunks re-stored");
        }

        // ── Post-BloatRestore: re-propagate rep results to non-reps ──
        // If a representative was ejected by the bloat guard and re-stored, its
        // BucketId/BucketKey/StorageGuid changed. Non-reps that reference that rep
        // need the updated values.
        if (Globals.EnableChunkClustering && bloatedDiffRestore.Count > 0)
        {
            var ejectedReps = new HashSet<int>();
            foreach (var idx in bloatedDiffRestore)
            {
                if (representativeSet.Contains(idx))
                    ejectedReps.Add(idx);
            }
            if (ejectedReps.Count > 0)
            {
                for (int i = 0; i < sorted.Length; i++)
                {
                    if (representativeSet.Contains(i)) continue;
                    int rep = groupRepresentative[i];
                    if (!ejectedReps.Contains(rep)) continue;
                    sorted[i].BucketId = sorted[rep].BucketId;
                    sorted[i].BucketKey = sorted[rep].BucketKey;
                    sorted[i].StorageGuid = sorted[rep].StorageGuid ?? "";
                    sorted[i].Chunk = sorted[rep].Chunk;
                }
            }
        }

        // ── ACTUAL DEDUP STAT: computed AFTER BloatRestore so it reflects real savings ──
        // "referencesFound" = chunks that truly reuse an existing base (not stored fresh).
        // With clustering, non-reps that weren't ejected count as references (they diff
        // against the rep's base chunk and share its agent reference).
        int referencesFound = sorted.Count(r => r != null && !r.NeedToStore && r.BucketId != 0);
        int storedChunks = sorted.Count(r => r != null && r.NeedToStore && r.BucketId != 0);
        int clusteredNonReps = Globals.EnableChunkClustering ? fileChunks.Count - representativeSet.Count : 0;
        int ejectedByBloat = bloatedDiffRestore.Count(i => !representativeSet.Contains(i));

        // Compute actual datacenter storage cost: collect the exact bytes of every
        // newly stored chunk and compress with zstd (matching agent RocksDB config).
        // No estimation — these are the real bytes that landed on agents.
        long datacenterBytesStored = 0;
        if (storedChunks > 0)
        {
            byte[] storedData = new byte[storedChunks * Globals.chunkSize];
            int pos = 0;
            for (int i = 0; i < sorted.Length; i++)
            {
                if (sorted[i] != null && sorted[i].NeedToStore && sorted[i].BucketId != 0)
                {
                    Buffer.BlockCopy(fileChunks[i], 0, storedData, pos, Globals.chunkSize);
                    pos += Globals.chunkSize;
                }
            }
            using var zstdComp = new Compressor(3);
            datacenterBytesStored = zstdComp.Wrap(storedData.AsSpan(0, pos)).Length;
        }

        long rawStoredBytes = (long)storedChunks * Globals.chunkSize;
        Console.WriteLine($"[Compress] Stats: initialLshMatches={initialMatches}, actualDedup={referencesFound}, " +
            $"stored={storedChunks}, totalChunks={totalChunks}, " +
            $"clusteredNonReps={clusteredNonReps}, ejectedByBloat={ejectedByBloat}, " +
            $"rawStoredBytes={rawStoredBytes}, datacenterBytesZstd={datacenterBytesStored}");

        // v5.0.0: references are built by BuildV5References (ref table + compact indices)

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
                // Mosaic chunks: base == stitched mosaic from donors (what decompressor reconstructs via 0x03 ref)
                // Representatives that were stored: base == rep's own bytes (zero diff for rep itself).
                // Non-reps whose rep was stored: base == rep's bytes (diff encodes distance to rep).
                // Search matches: base == matched chunk from agent.
                if (mosaicInfos.TryGetValue(i, out var mosaicForDiff))
                {
                    Buffer.BlockCopy(mosaicForDiff.StitchedBase, 0, baseBuffer, off,
                        Math.Min(mosaicForDiff.StitchedBase.Length, Globals.chunkSize));
                }
                else if (sorted[i].NeedToStore && sorted[i].BucketId != 0)
                {
                    Buffer.BlockCopy(fileChunks[i], 0, baseBuffer, off, Globals.chunkSize);
                }
                else if (!representativeSet.Contains(i) && sorted[groupRepresentative[i]].NeedToStore
                         && sorted[groupRepresentative[i]].BucketId != 0)
                {
                    // Non-rep whose rep was stored — base is the rep's actual bytes
                    Buffer.BlockCopy(fileChunks[groupRepresentative[i]], 0, baseBuffer, off, Globals.chunkSize);
                }
                else if (sorted[i].Chunk != null && sorted[i].Chunk.Length > 0)
                {
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
                foreach (var (startPos, runLength, diffValue) in flatDiff)
                {
                    cursor += startPos; // Move to start of run
                    if (cursor < 0 || cursor >= verifyBuf.Length)
                        throw new InvalidDataException(
                            $"Compression integrity: patch cursor {cursor} out of range (buffer {verifyBuf.Length}).");

                    // Apply the entire run
                    for (int j = 0; j < runLength; j++)
                    {
                        if (cursor + j >= verifyBuf.Length)
                            throw new InvalidDataException(
                                $"Compression integrity: run extends beyond buffer (cursor={cursor}, runLength={runLength}, buffer={verifyBuf.Length}).");

                        int patched = verifyBuf[cursor + j] + diffValue;
                        if (patched < 0 || patched > 255)
                        {
                            Console.WriteLine($"[Compress] INTEGRITY FAIL: base[{cursor + j}]={verifyBuf[cursor + j]} + delta={diffValue} = {patched} (chunk {(cursor + j) / Globals.chunkSize}, BucketId={sorted[(cursor + j) / Globals.chunkSize].BucketId})");
                            throw new InvalidDataException(
                                $"Compression integrity check failed: patch at {cursor + j} produces out-of-range byte {patched}.");
                        }
                        verifyBuf[cursor + j] = (byte)patched;
                    }
                    cursor += runLength; // Move past the run
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

            // Serialize RLE: <int totalRunCount> + <int startPos><ushort runLength><short diffValue> × totalRunCount
            // Each run: 4 + 2 + 2 = 8 bytes (vs old 6 bytes per byte, but runs compress consecutive identical diffs)
            // Runs longer than 65535 are split into consecutive sub-runs (ushort max).
            // Count total serialized runs first (may be more than flatDiff.Count due to splits).
            int serializedRunCount = 0;
            foreach (var (_, runLength, _) in flatDiff)
                serializedRunCount += (runLength + 65534) / 65535; // ceiling division

            using (var errorMs = new MemoryStream(4 + serializedRunCount * 8))
            using (var errorWriter = new BinaryWriter(errorMs, Encoding.UTF8, leaveOpen: true))
            {
                errorWriter.Write(serializedRunCount);
                foreach (var (startPos, runLength, diffValue) in flatDiff)
                {
                    int remaining = runLength;
                    int relStart = startPos;
                    while (remaining > 0)
                    {
                        int chunk = Math.Min(remaining, 65535);
                        errorWriter.Write(relStart); // 4 bytes: relative start position
                        errorWriter.Write((ushort)chunk); // 2 bytes: run length (max 65,535)
                        errorWriter.Write((short)diffValue); // 2 bytes: diff value
                        remaining -= chunk;
                        relStart = 0; // continuation sub-runs start immediately after previous
                    }
                }
                errorWriter.Flush();
                errorDictionaryBytes = errorMs.ToArray();
            }

            // ── INTEGRITY CHECK 2: serialization round-trip ──
            // Deserialize what we just wrote, apply patches, and verify byte-for-byte
            // match against originalBuffer. Catches any serialization bug including
            // run-splitting errors.
            {
                using var verifyMs = new MemoryStream(errorDictionaryBytes, writable: false);
                using var verifyReader = new BinaryReader(verifyMs, Encoding.UTF8, leaveOpen: true);
                int verifyCount = verifyReader.ReadInt32();
                if (verifyCount != serializedRunCount)
                    throw new InvalidDataException(
                        $"Serialization integrity: run count mismatch ({verifyCount} vs {serializedRunCount}).");

                byte[] verifyBuf2 = new byte[baseBuffer.Length];
                Buffer.BlockCopy(baseBuffer, 0, verifyBuf2, 0, baseBuffer.Length);
                int cursor2 = 0;
                for (int i = 0; i < verifyCount; i++)
                {
                    int rStartPos = verifyReader.ReadInt32();
                    ushort rRunLength = verifyReader.ReadUInt16();
                    short rDiffValue = verifyReader.ReadInt16();
                    cursor2 += rStartPos;
                    for (int j = 0; j < rRunLength; j++)
                        verifyBuf2[cursor2 + j] = (byte)(verifyBuf2[cursor2 + j] + rDiffValue);
                    cursor2 += rRunLength;
                }
                if (!verifyBuf2.AsSpan().SequenceEqual(originalBuffer.AsSpan()))
                    throw new InvalidDataException(
                        "Serialization integrity: deserialized patches do not reconstruct original file.");
            }

            phaseSw.Stop();
            Console.WriteLine($"[Compress] FlatEncode: {phaseSw.ElapsedMilliseconds}ms, runs={flatDiff.Count} (serialized={serializedRunCount}), bytes={errorDictionaryBytes.Length}");
            Observability.RecordStage("FlatEncode", phaseSw.Elapsed.TotalMilliseconds,
                ("pairs", serializedRunCount), ("bytes", errorDictionaryBytes.Length));
        }

        byte[] toReturn;
        {
            using var stage = Observability.StartStage("Serialize");
            var swStage = Stopwatch.StartNew();
            byte[] refBytes = BuildV5References(sorted, mosaicInfos);
            int storedForLog = sorted.Count(r => r != null && r.NeedToStore);

            // Release all heavy collections now that refs/errors are computed
            fileChunks = null!;
            vectors = null!;
            bitStrings = null!;
            mainAgents = null!;
            chunkMap = null!;
            mosaicCandidates = null!;
            mosaicInfos = null!;
            sorted = null!;
            representativeSet = null!;
            groupRepresentative = null!;

            byte[] fileHash = SHA256.HashData(_file);
            toReturn = BuildCompressedPayload(
                EncodingVersion,
                refBytes,
                errorDictionaryBytes,
                trimmedChunk.ToArray(),
                fileHash);
            swStage.Stop();
            Observability.RecordStage("Serialize", swStage.Elapsed.TotalMilliseconds, ("output_bytes", toReturn.Length));

            totalSw.Stop();
            Console.WriteLine($"[Compress] DONE: {totalSw.ElapsedMilliseconds}ms total, in={_file.Length} out={toReturn.Length} ratio={toReturn.Length/(double)_file.Length:F3}, dedup={referencesFound}, lshMatches={initialMatches}, stored={storedForLog}");
        }

        return (toReturn, referencesFound, totalChunks, datacenterBytesStored);
    }

    public async Task<byte[]> CompressFile(byte[] _file)
    {
        var res = await CompressFileWithStats(_file);
        return res.CompressedBytes;
    }

    // ═══════════════════════════════════════════════════════════════════
    //  v4.0.0 WINDOWED COMPRESSION — O(windowSize) RAM, unlimited file size
    // ═══════════════════════════════════════════════════════════════════
    // Format:
    //   [4 bytes: magic "CV4\0"]
    //   [8 bytes: originalFileSize (int64)]
    //   [32 bytes: SHA256 of entire original file]
    //   [4 bytes: blockCount (int32)]
    //   ── per block ──
    //   [8 bytes: blockCompressedLen (int64)]
    //   [8 bytes: blockOriginalLen (int64)]
    //   [blockCompressedLen bytes: self-contained v3.0.0 .ccf data]
    //   ── end blocks ──
    // Each block is an independent v3.0.0 .ccf file for a window of the
    // original file.  This means the existing CompressFileWithStats and
    // DecompressFile work per-block with zero changes.

    private static readonly byte[] V4Magic = "CV4\0"u8.ToArray();
    private static readonly byte[] V5Magic = "CV5\0"u8.ToArray();

    /// <summary>Default window size for windowed compression (64 MiB).</summary>
    public static int WindowSize
    {
        get
        {
            var raw = Environment.GetEnvironmentVariable("CROSS_WINDOW_SIZE_MB");
            if (int.TryParse(raw, out var mb) && mb > 0) return mb * 1024 * 1024;
            return 64 * 1024 * 1024; // 64 MiB
        }
    }

    /// <summary>
    /// Compress a large file using windowed v4.0.0 format.
    /// Reads the input file in windows, compresses each independently,
    /// writes blocks to the output file.  RAM usage ≈ O(windowSize).
    /// Returns (totalCompressedSize, totalRefsFound, totalChunks).
    /// </summary>
    public async Task<(long CompressedSize, int TotalRefs, int TotalChunks)> CompressFileWindowedAsync(
        string inputPath, string outputPath, CancellationToken ct = default)
    {
        long originalFileSize = new FileInfo(inputPath).Length;
        int windowSize = WindowSize;

        // ── 1. Compute SHA256 of entire original file (streaming — no full load) ──
        byte[] originalHash;
        using (var sha = System.Security.Cryptography.SHA256.Create())
        await using (var hashStream = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
                         FileShare.Read, bufferSize: 1024 * 1024, useAsync: true))
        {
            byte[] buf = new byte[1024 * 1024];
            int read;
            while ((read = await hashStream.ReadAsync(buf, 0, buf.Length, ct)) > 0)
                sha.TransformBlock(buf, 0, read, null, 0);
            sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            originalHash = sha.Hash!;
        }

        // ── 2. Determine block count ──
        int blockCount = (int)((originalFileSize + windowSize - 1) / windowSize);
        if (blockCount == 0) blockCount = 1; // empty file edge case

        Console.WriteLine($"[CompressWindowed] file={originalFileSize} bytes, windowSize={windowSize}, blocks={blockCount}");

        // ── 3. Write header + compress each window ──
        long totalCompressedSize = 0;
        int totalRefs = 0;
        int totalChunks = 0;

        await using var inputFs = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
                             FileShare.Read, bufferSize: 1024 * 1024, useAsync: true);
        await using var outputFs = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
                             FileShare.None, bufferSize: 1024 * 1024, useAsync: true);
        using var bw = new BinaryWriter(outputFs, Encoding.UTF8, leaveOpen: true);

        // Header: magic(4) + fileSize(8) + blockCount(4) = 16 bytes
        bw.Write(V5Magic);
        bw.Write(originalFileSize);
        bw.Write(blockCount);
        await outputFs.FlushAsync(ct);

        long headerBytes = 4 + 8 + 4;
        totalCompressedSize += headerBytes;

        byte[] windowBuffer = new byte[windowSize];

        for (int block = 0; block < blockCount; block++)
        {
            ct.ThrowIfCancellationRequested();

            // Read one window
            int remaining = (int)Math.Min(windowSize, originalFileSize - inputFs.Position);
            int totalRead = 0;
            while (totalRead < remaining)
            {
                int r = await inputFs.ReadAsync(windowBuffer.AsMemory(totalRead, remaining - totalRead), ct);
                if (r == 0) break;
                totalRead += r;
            }

            // Exact-size slice (last window may be smaller)
            byte[] windowData = totalRead == windowSize
                ? windowBuffer
                : windowBuffer[..totalRead];

            Console.WriteLine($"[CompressWindowed] Block {block + 1}/{blockCount}: {totalRead} bytes");

            var (compressedBlock, refs, chunks, _) = await CompressFileWithStats(windowData);
            totalRefs += refs;
            totalChunks += chunks;

            // v5: int32 block headers (blocks can't exceed window size)
            bw.Write((int)compressedBlock.Length);
            bw.Write((int)totalRead);
            await outputFs.WriteAsync(compressedBlock, 0, compressedBlock.Length, ct);
            await outputFs.FlushAsync(ct);

            totalCompressedSize += 4 + 4 + compressedBlock.Length;

            Console.WriteLine($"[CompressWindowed] Block {block + 1}/{blockCount}: compressed {totalRead} → {compressedBlock.Length} (ratio {(double)compressedBlock.Length / Math.Max(1, totalRead):F3})");
        }

        // Write SHA256 hash as trailer at end of file
        await outputFs.WriteAsync(originalHash, 0, originalHash.Length, ct);
        await outputFs.FlushAsync(ct);
        totalCompressedSize += 32;

        Console.WriteLine($"[CompressWindowed] DONE: {originalFileSize} → {totalCompressedSize} bytes, {blockCount} blocks, refs={totalRefs}");
        return (totalCompressedSize, totalRefs, totalChunks);
    }

    /// <summary>
    /// TRUE STREAMING: Compress a file that's still being written, processing windows in order as they arrive.
    /// Reads windows from file as they become available, compresses in order, writes to output in order.
    /// RAM usage ≈ O(windowSize). No need to wait for entire file.
    /// Format: Header(magic+fileSize+blockCount) → Blocks → Trailer(sha256)
    /// Hash is written as a TRAILER (not in header) because the output stream is forward-only (gRPC).
    /// </summary>
    public async Task<(long CompressedSize, int TotalRefs, int TotalChunks)> CompressFileWindowedStreamAsync(
        string inputPath, Stream outputStream, long declaredFileSize, CancellationToken ct = default)
    {
        long originalFileSize = declaredFileSize;
        int windowSize = WindowSize;

        // ── 1. Determine block count from declared size ──
        int blockCount = (int)((originalFileSize + windowSize - 1) / windowSize);
        if (blockCount == 0) blockCount = 1; // empty file edge case

        Console.WriteLine($"[CompressWindowedStream] file={originalFileSize} bytes, windowSize={windowSize}, blocks={blockCount}");

        // ── 2. Write header (NO hash — hash goes at end as trailer) ──
        long totalCompressedSize = 0;
        int totalRefs = 0;
        int totalChunks = 0;

        await using var inputFs = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
                             FileShare.Read, bufferSize: 1024 * 1024, useAsync: true);
        using var bw = new BinaryWriter(outputStream, Encoding.UTF8, leaveOpen: true);
        using var sha = System.Security.Cryptography.SHA256.Create();

        // v5 header: magic(4) + fileSize(8) + blockCount(4) = 16 bytes
        bw.Write(V5Magic);
        bw.Write(originalFileSize);
        bw.Write(blockCount);
        await outputStream.FlushAsync(ct);

        long headerBytes = 4 + 8 + 4;
        totalCompressedSize += headerBytes;

        byte[] windowBuffer = new byte[windowSize];
        long totalReadSoFar = 0;

        // ── 3. Process windows in order as they become available ──
        Console.WriteLine($"[CompressWindowedStream] 🚀 STARTING compression (file={originalFileSize} bytes, {blockCount} blocks, file may still be growing)");
        for (int block = 0; block < blockCount; block++)
        {
            ct.ThrowIfCancellationRequested();

            // Read one window (with retries if file is still being written)
            int remaining = (int)Math.Min(windowSize, originalFileSize - totalReadSoFar);
            int totalRead = 0;
            int retries = 0;
            const int maxRetries = 1000; // More retries for large files
            
            Console.WriteLine($"[CompressWindowedStream] Block {block + 1}/{blockCount}: Starting read (need {remaining} bytes, file size={inputFs.Length}, pos={inputFs.Position})");
            
            while (totalRead < remaining && retries < maxRetries)
            {
                long filePos = inputFs.Position;
                long fileLength = inputFs.Length;
                long availableBytes = fileLength - filePos;
                
                if (availableBytes < remaining - totalRead)
                {
                    // File not ready yet, wait a bit
                    if (retries % 50 == 0) // Log every 5 seconds
                        Console.WriteLine($"[CompressWindowedStream] Block {block + 1}: waiting for data (have {availableBytes}, need {remaining - totalRead}, file size={fileLength}/{originalFileSize})");
                    await Task.Delay(100, ct);
                    retries++;
                    continue;
                }

                // remaining - totalRead is at most windowSize (64 MB), and we already verified
                // availableBytes >= remaining - totalRead, so this is safe (no int overflow)
                int toRead = remaining - totalRead;
                int r = await inputFs.ReadAsync(windowBuffer.AsMemory(totalRead, toRead), ct);
                if (r == 0)
                {
                    if (retries < maxRetries)
                    {
                        await Task.Delay(100, ct);
                        retries++;
                        continue;
                    }
                    break;
                }
                totalRead += r;
            }

            if (totalRead == 0 && block < blockCount - 1)
            {
                Console.WriteLine($"[CompressWindowedStream] WARNING: Block {block + 1} read 0 bytes, file may not be complete yet. Waiting...");
                int extraRetries = 0;
                while (totalRead == 0 && extraRetries < 500)
                {
                    await Task.Delay(200, ct);
                    long fileLength = inputFs.Length;
                    long available = fileLength - inputFs.Position;
                    if (available > 0)
                    {
                        int r = await inputFs.ReadAsync(windowBuffer.AsMemory(0, remaining), ct);
                        if (r > 0) totalRead = r;
                    }
                    extraRetries++;
                }
                if (totalRead == 0)
                    throw new InvalidDataException($"Failed to read window {block + 1}: file may not be complete");
            }

            // Exact-size slice (last window may be smaller)
            byte[] windowData = totalRead == windowSize
                ? windowBuffer
                : windowBuffer[..totalRead];

            // Update hash as we read
            sha.TransformBlock(windowData, 0, windowData.Length, null, 0);
            totalReadSoFar += totalRead;

            Console.WriteLine($"[CompressWindowedStream] Block {block + 1}/{blockCount}: {totalRead} bytes (total read: {totalReadSoFar}/{originalFileSize})");

            // Compress this window using the existing v3.0.0 pipeline (unchanged!)
            var (compressedBlock, refs, chunks, _) = await CompressFileWithStats(windowData);
            totalRefs += refs;
            totalChunks += chunks;

            // v5: int32 block headers
            bw.Write((int)compressedBlock.Length);
            bw.Write((int)totalRead);
            await outputStream.WriteAsync(compressedBlock, 0, compressedBlock.Length, ct);
            await outputStream.FlushAsync(ct);

            totalCompressedSize += 4 + 4 + compressedBlock.Length;

            Console.WriteLine($"[CompressWindowedStream] Block {block + 1}/{blockCount}: compressed {totalRead} → {compressedBlock.Length} (ratio {(double)compressedBlock.Length / Math.Max(1, totalRead):F3}) → STREAMED");
        }

        // ── 4. Finalize hash and write as TRAILER (no seeking needed!) ──
        sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        byte[] finalHash = sha.Hash!;

        // Write hash as trailer at end of stream
        await outputStream.WriteAsync(finalHash, 0, 32, ct);
        await outputStream.FlushAsync(ct);
        totalCompressedSize += 32;

        Console.WriteLine($"[CompressWindowedStream] DONE: {originalFileSize} → {totalCompressedSize} bytes, {blockCount} blocks, refs={totalRefs}, hash={Convert.ToHexString(finalHash)}");
        return (totalCompressedSize, totalRefs, totalChunks);
    }

    /// <summary>
    /// Decompress a v4.0.0 windowed file.  Reads blocks from the input file,
    /// decompresses each independently, writes to the output file.
    /// RAM usage ≈ O(windowSize).
    /// Returns the total decompressed size.
    /// </summary>
    public async Task<long> DecompressFileWindowedAsync(
        string inputPath, string outputPath, CancellationToken ct = default)
    {
        await using var inputFs = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
                             FileShare.Read, bufferSize: 1024 * 1024, useAsync: true);
        await using var outputFs = new FileStream(outputPath, FileMode.Create, FileAccess.Write,
                             FileShare.None, bufferSize: 1024 * 1024, useAsync: true);
        using var br = new BinaryReader(inputFs, Encoding.UTF8, leaveOpen: true);

        byte[] magic = br.ReadBytes(4);
        bool isV5Container = magic.AsSpan().SequenceEqual(V5Magic);
        bool isV4Container = magic.AsSpan().SequenceEqual(V4Magic);
        if (!isV5Container && !isV4Container)
            throw new InvalidDataException("Not a v4/v5 windowed compressed file (bad magic).");

        long originalFileSize = br.ReadInt64();
        int blockCount = br.ReadInt32();

        Console.WriteLine($"[DecompressWindowed] format={( isV5Container ? "CV5" : "CV4" )}, originalSize={originalFileSize}, blocks={blockCount}");

        long totalDecompressed = 0;
        using var sha = System.Security.Cryptography.SHA256.Create();

        for (int block = 0; block < blockCount; block++)
        {
            ct.ThrowIfCancellationRequested();

            long compressedLen, originalLen;
            if (isV5Container)
            {
                compressedLen = br.ReadInt32();
                originalLen = br.ReadInt32();
            }
            else
            {
                compressedLen = br.ReadInt64();
                originalLen = br.ReadInt64();
            }

            byte[] compressedBlock = new byte[compressedLen];
            int totalRead = 0;
            while (totalRead < compressedLen)
            {
                int r = await inputFs.ReadAsync(compressedBlock.AsMemory(totalRead, (int)(compressedLen - totalRead)), ct);
                if (r == 0) throw new InvalidDataException($"Unexpected EOF in block {block}.");
                totalRead += r;
            }

            Console.WriteLine($"[DecompressWindowed] Block {block + 1}/{blockCount}: {compressedLen} compressed → decompressing...");

            byte[] decompressedBlock = await DecompressFile(compressedBlock);

            if (decompressedBlock.Length != originalLen)
                throw new InvalidDataException(
                    $"Block {block}: decompressed size {decompressedBlock.Length} != expected {originalLen}.");

            // Write to output and update rolling hash
            await outputFs.WriteAsync(decompressedBlock, 0, decompressedBlock.Length, ct);
            sha.TransformBlock(decompressedBlock, 0, decompressedBlock.Length, null, 0);
            totalDecompressed += decompressedBlock.Length;

            Console.WriteLine($"[DecompressWindowed] Block {block + 1}/{blockCount}: OK ({decompressedBlock.Length} bytes)");
        }

        // ── 3. Read SHA256 hash from trailer (after all blocks) ──
        byte[] expectedHash = br.ReadBytes(32);
        if (expectedHash.Length != 32)
            throw new InvalidDataException("Missing SHA256 trailer in compressed file.");

        // ── 4. Verify whole-file SHA256 ──
        sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        byte[] actualHash = sha.Hash!;

        if (!actualHash.AsSpan().SequenceEqual(expectedHash))
        {
            Console.WriteLine($"[DecompressWindowed] ❌ INTEGRITY FAILURE: whole-file SHA256 mismatch");
            Console.WriteLine($"[DecompressWindowed]    Expected: {Convert.ToHexString(expectedHash)}");
            Console.WriteLine($"[DecompressWindowed]    Got:      {Convert.ToHexString(actualHash)}");
            throw new InvalidDataException(
                "Windowed decompression integrity check failed: SHA256 of reconstructed file does not match original.");
        }

        Console.WriteLine($"[DecompressWindowed] ✅ SHA256 verified, total={totalDecompressed} bytes");
        return totalDecompressed;
    }

    /// <summary>
    /// Streaming version: Decompress a v4.0.0 windowed file, writing directly to an output stream.
    /// Reads blocks from the input file, decompresses each independently, streams to output.
    /// RAM usage ≈ O(windowSize).
    /// Returns (totalDecompressedSize, computedHash).
    /// </summary>
    public async Task<(long DecompressedSize, byte[] Hash)> DecompressFileWindowedStreamAsync(
        string inputPath, Stream outputStream, CancellationToken ct = default)
    {
        await using var inputFs = new FileStream(inputPath, FileMode.Open, FileAccess.Read,
                             FileShare.Read, bufferSize: 1024 * 1024, useAsync: true);
        using var br = new BinaryReader(inputFs, Encoding.UTF8, leaveOpen: true);

        byte[] magic = br.ReadBytes(4);
        bool isV5Container = magic.AsSpan().SequenceEqual(V5Magic);
        bool isV4Container = magic.AsSpan().SequenceEqual(V4Magic);
        if (!isV5Container && !isV4Container)
            throw new InvalidDataException("Not a v4/v5 windowed compressed file (bad magic).");

        long originalFileSize = br.ReadInt64();
        int blockCount = br.ReadInt32();

        Console.WriteLine($"[DecompressWindowedStream] format={( isV5Container ? "CV5" : "CV4" )}, originalSize={originalFileSize}, blocks={blockCount}");

        long totalDecompressed = 0;
        using var sha = System.Security.Cryptography.SHA256.Create();

        for (int block = 0; block < blockCount; block++)
        {
            ct.ThrowIfCancellationRequested();

            long compressedLen, originalLen;
            if (isV5Container)
            {
                compressedLen = br.ReadInt32();
                originalLen = br.ReadInt32();
            }
            else
            {
                compressedLen = br.ReadInt64();
                originalLen = br.ReadInt64();
            }

            byte[] compressedBlock = new byte[compressedLen];
            int totalRead = 0;
            while (totalRead < compressedLen)
            {
                int r = await inputFs.ReadAsync(compressedBlock.AsMemory(totalRead, (int)(compressedLen - totalRead)), ct);
                if (r == 0) throw new InvalidDataException($"Unexpected EOF in block {block}.");
                totalRead += r;
            }

            Console.WriteLine($"[DecompressWindowedStream] Block {block + 1}/{blockCount}: {compressedLen} compressed → decompressing...");

            byte[] decompressedBlock = await DecompressFile(compressedBlock);

            if (decompressedBlock.Length != originalLen)
                throw new InvalidDataException(
                    $"Block {block}: decompressed size {decompressedBlock.Length} != expected {originalLen}.");

            // Stream directly to output and update rolling hash
            await outputStream.WriteAsync(decompressedBlock, 0, decompressedBlock.Length, ct);
            await outputStream.FlushAsync(ct);
            sha.TransformBlock(decompressedBlock, 0, decompressedBlock.Length, null, 0);
            totalDecompressed += decompressedBlock.Length;

            Console.WriteLine($"[DecompressWindowedStream] Block {block + 1}/{blockCount}: OK ({decompressedBlock.Length} bytes) → streamed");
        }

        // ── 3. Read SHA256 hash from trailer (after all blocks) ──
        byte[] expectedHash = br.ReadBytes(32);
        if (expectedHash.Length != 32)
            throw new InvalidDataException("Missing SHA256 trailer in compressed file.");

        // ── 4. Verify whole-file SHA256 ──
        sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        byte[] actualHash = sha.Hash!;

        if (!actualHash.AsSpan().SequenceEqual(expectedHash))
        {
            Console.WriteLine($"[DecompressWindowedStream] ❌ INTEGRITY FAILURE: whole-file SHA256 mismatch");
            Console.WriteLine($"[DecompressWindowedStream]    Expected: {Convert.ToHexString(expectedHash)}");
            Console.WriteLine($"[DecompressWindowedStream]    Got:      {Convert.ToHexString(actualHash)}");
            throw new InvalidDataException(
                "Windowed decompression integrity check failed: SHA256 of reconstructed file does not match original.");
        }

        Console.WriteLine($"[DecompressWindowedStream] ✅ SHA256 verified, total={totalDecompressed} bytes");
        return (totalDecompressed, actualHash);
    }

    /// <summary>
    /// Detect whether a compressed file is v4.0.0 windowed format by checking the magic bytes.
    /// </summary>
    public static bool IsV4Format(byte[] headerBytes)
    {
        return headerBytes.Length >= 4 && headerBytes.AsSpan(0, 4).SequenceEqual(V4Magic);
    }

    public static bool IsV5Format(byte[] headerBytes)
    {
        return headerBytes.Length >= 4 && headerBytes.AsSpan(0, 4).SequenceEqual(V5Magic);
    }

    public static bool IsWindowedFormat(byte[] headerBytes)
    {
        return IsV4Format(headerBytes) || IsV5Format(headerBytes);
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

        // ── Parse references based on version ──
        bool isV3 = header.Version == "v3.0.0" || header.Version == "v3.1.0";
        ulong[] refBucketIds;
        ulong[] refBucketIndices;
        string[] refStorageGuids;
        int chunkCount;

        // Mosaic metadata for 0x03/0x04 refs (populated during parsing, used during fetch)
        // BucketIndex is used by v5 (fetched via GetChunkByReferenceAsync); v3.x sets it to 0 and uses StorageGuid instead.
        var mosaicRefs = new Dictionary<int, (List<(ulong BucketId, string StorageGuid, ulong BucketIndex)> Donors, ulong MatchBitmap, byte[] Selectors, byte[]? DonorPositions)>();

        if (header.Version == "v5.0.0")
        {
            // v5.0.0: compact ref table + ushort indices
            // [4 bytes: chunkCount] [2 bytes: refTableSize]
            // [refTableSize × (8 bytes bucketId + 8 bytes bucketIndex)]
            // then per-chunk: flag byte + ushort table index (or mosaic data)
            int off = referencesOffset;
            chunkCount = BitConverter.ToInt32(file, off);
            off += sizeof(int);
            ushort refTableSize = BitConverter.ToUInt16(file, off);
            off += sizeof(ushort);

            var refTable = new (ulong BucketId, ulong BucketIndex)[refTableSize];
            for (int i = 0; i < refTableSize; i++)
            {
                ulong bucketId = BitConverter.ToUInt64(file, off);
                off += sizeof(ulong);
                ulong bucketIndex = BitConverter.ToUInt64(file, off);
                off += sizeof(ulong);
                refTable[i] = (bucketId, bucketIndex);
            }

            refBucketIds = new ulong[chunkCount];
            refBucketIndices = new ulong[chunkCount];
            refStorageGuids = new string[chunkCount];

            for (int i = 0; i < chunkCount; i++)
            {
                byte flag = file[off++];
                switch (flag)
                {
                    case 0x00:
                        refBucketIds[i] = 0;
                        refBucketIndices[i] = 0;
                        refStorageGuids[i] = "";
                        break;
                    case 0x01:
                    case 0x02:
                    {
                        ushort tableIdx = BitConverter.ToUInt16(file, off);
                        off += sizeof(ushort);
                        if (tableIdx >= refTableSize)
                            throw new InvalidDataException($"v5 ref table index {tableIdx} out of range ({refTableSize} entries)");
                        refBucketIds[i] = refTable[tableIdx].BucketId;
                        refBucketIndices[i] = refTable[tableIdx].BucketIndex;
                        refStorageGuids[i] = "";
                        break;
                    }
                    case 0x04:
                    {
                        int donorCount = file[off++];
                        var donors = new List<(ulong BucketId, string StorageGuid, ulong BucketIndex)>(donorCount);
                        for (int d = 0; d < donorCount; d++)
                        {
                            ushort tableIdx = BitConverter.ToUInt16(file, off);
                            off += sizeof(ushort);
                            if (tableIdx >= refTableSize)
                                throw new InvalidDataException($"v5 mosaic donor table index {tableIdx} out of range");
                            donors.Add((refTable[tableIdx].BucketId, "", refTable[tableIdx].BucketIndex));
                        }
                        ulong matchBitmap = BitConverter.ToUInt64(file, off);
                        off += sizeof(ulong);
                        var selectors = new byte[32];
                        Buffer.BlockCopy(file, off, selectors, 0, 32);
                        off += 32;
                        var donorPositions = new byte[64];
                        Buffer.BlockCopy(file, off, donorPositions, 0, 64);
                        off += 64;

                        mosaicRefs[i] = (donors, matchBitmap, selectors, donorPositions);
                        refBucketIds[i] = ulong.MaxValue;
                        refStorageGuids[i] = "";
                        break;
                    }
                    default:
                        throw new InvalidDataException($"v5: unknown reference flag 0x{flag:X2} at chunk {i}");
                }
            }
        }
        else if (header.Version == "v3.1.0")
        {
            // v3.1.0: flag-based variable-size refs
            // [4 bytes: chunkCount] then per-chunk:
            //   flag=0x00: zero-ref (1 byte)
            //   flag=0x01: full ref (1 + 8 bucketId + 32 storageGuid = 41 bytes)
            //   flag=0x02: compact ref (1 + 8 bucketId + 8 bucketIndex = 17 bytes)
            //   flag=0x03: mosaic ref (donors + bitmap + selectors) — legacy positional
            //   flag=0x04: mosaic ref (donors + bitmap + selectors + donorPositions) — cross-position
            int off = referencesOffset;
            chunkCount = BitConverter.ToInt32(file, off);
            off += sizeof(int);

            refBucketIds = new ulong[chunkCount];
            refBucketIndices = new ulong[chunkCount];
            refStorageGuids = new string[chunkCount];

            for (int i = 0; i < chunkCount; i++)
            {
                byte flag = file[off++];
                switch (flag)
                {
                    case 0x00: // zero-ref
                        refBucketIds[i] = 0;
                        refStorageGuids[i] = "";
                        break;
                    case 0x01: // full ref: bucketId + storageGuid
                        refBucketIds[i] = BitConverter.ToUInt64(file, off);
                        off += sizeof(ulong);
                        var guidRaw = new byte[32];
                        Buffer.BlockCopy(file, off, guidRaw, 0, 32);
                        off += 32;
                        bool allZero = true;
                        for (int b = 0; b < 32; b++) { if (guidRaw[b] != 0) { allZero = false; break; } }
                        refStorageGuids[i] = allZero ? "" : Convert.ToHexString(guidRaw).ToLowerInvariant();
                        break;
                    case 0x02: // compact ref: bucketId + bucketIndex
                        refBucketIds[i] = BitConverter.ToUInt64(file, off);
                        off += sizeof(ulong);
                        refBucketIndices[i] = BitConverter.ToUInt64(file, off);
                        off += sizeof(ulong);
                        refStorageGuids[i] = ""; // decompressor uses bucketIndex path
                        break;
                    case 0x03: // legacy positional mosaic ref: donors + bitmap + selectors
                    {
                        int donorCount = file[off++];
                        var donors = new List<(ulong BucketId, string StorageGuid, ulong BucketIndex)>(donorCount);
                        for (int d = 0; d < donorCount; d++)
                        {
                            ulong dBucketId = BitConverter.ToUInt64(file, off);
                            off += sizeof(ulong);
                            var dGuidRaw = new byte[32];
                            Buffer.BlockCopy(file, off, dGuidRaw, 0, 32);
                            off += 32;
                            bool dAllZero = true;
                            for (int b = 0; b < 32; b++) { if (dGuidRaw[b] != 0) { dAllZero = false; break; } }
                            string dGuid = dAllZero ? "" : Convert.ToHexString(dGuidRaw).ToLowerInvariant();
                            donors.Add((dBucketId, dGuid, 0));
                        }
                        ulong matchBitmap = BitConverter.ToUInt64(file, off);
                        off += sizeof(ulong);
                        var selectors = new byte[32];
                        Buffer.BlockCopy(file, off, selectors, 0, 32);
                        off += 32;

                        mosaicRefs[i] = (donors, matchBitmap, selectors, null);
                        refBucketIds[i] = ulong.MaxValue;
                        refStorageGuids[i] = "";
                        break;
                    }
                    case 0x04: // cross-position mosaic ref: donors + bitmap + selectors + donorPositions
                    {
                        int donorCount = file[off++];
                        var donors = new List<(ulong BucketId, string StorageGuid, ulong BucketIndex)>(donorCount);
                        for (int d = 0; d < donorCount; d++)
                        {
                            ulong dBucketId = BitConverter.ToUInt64(file, off);
                            off += sizeof(ulong);
                            var dGuidRaw = new byte[32];
                            Buffer.BlockCopy(file, off, dGuidRaw, 0, 32);
                            off += 32;
                            bool dAllZero = true;
                            for (int b = 0; b < 32; b++) { if (dGuidRaw[b] != 0) { dAllZero = false; break; } }
                            string dGuid = dAllZero ? "" : Convert.ToHexString(dGuidRaw).ToLowerInvariant();
                            donors.Add((dBucketId, dGuid, 0));
                        }
                        ulong matchBitmap = BitConverter.ToUInt64(file, off);
                        off += sizeof(ulong);
                        var selectors = new byte[32];
                        Buffer.BlockCopy(file, off, selectors, 0, 32);
                        off += 32;
                        var donorPositions = new byte[64];
                        Buffer.BlockCopy(file, off, donorPositions, 0, 64);
                        off += 64;

                        mosaicRefs[i] = (donors, matchBitmap, selectors, donorPositions);
                        refBucketIds[i] = ulong.MaxValue;
                        refStorageGuids[i] = "";
                        break;
                    }
                    default:
                        throw new InvalidDataException($"Unknown reference flag 0x{flag:X2} at chunk {i}");
                }
            }
        }
        else if (isV3)
        {
            // v3.0.0: fixed 40 bytes per ref (bucketId + storageGuid)
            int refBytesPerChunk = sizeof(ulong) + 32; // 40
            if (header.ReferencesLength % refBytesPerChunk != 0)
                throw new InvalidDataException($"Reference section length {header.ReferencesLength} is not divisible by {refBytesPerChunk}.");
            chunkCount = header.ReferencesLength / refBytesPerChunk;
            refBucketIds = new ulong[chunkCount];
            refBucketIndices = new ulong[chunkCount];
            refStorageGuids = new string[chunkCount];
            for (int i = 0; i < chunkCount; i++)
            {
                int off = referencesOffset + (i * refBytesPerChunk);
                refBucketIds[i] = BitConverter.ToUInt64(file, off);
                var guidRaw = new byte[32];
                Buffer.BlockCopy(file, off + sizeof(ulong), guidRaw, 0, 32);
                bool allZero = true;
                for (int b = 0; b < 32; b++) { if (guidRaw[b] != 0) { allZero = false; break; } }
                refStorageGuids[i] = allZero ? "" : Convert.ToHexString(guidRaw).ToLowerInvariant();
            }
        }
        else
        {
            // v2.x: fixed 16 bytes per ref (bucketId + bucketIndex)
            int refBytesPerChunk = sizeof(ulong) * 2; // 16
            if (header.ReferencesLength % refBytesPerChunk != 0)
                throw new InvalidDataException($"Reference section length {header.ReferencesLength} is not divisible by {refBytesPerChunk}.");
            chunkCount = header.ReferencesLength / refBytesPerChunk;
            refBucketIds = new ulong[chunkCount];
            refBucketIndices = new ulong[chunkCount];
            refStorageGuids = new string[chunkCount];
            for (int i = 0; i < chunkCount; i++)
            {
                int off = referencesOffset + (i * refBytesPerChunk);
                refBucketIds[i] = BitConverter.ToUInt64(file, off);
                refBucketIndices[i] = BitConverter.ToUInt64(file, off + sizeof(ulong));
            }
        }

        // ── Parse flat error encoding (RLE format) ──
        // v3.1.0: error section is zstd-compressed; decompress before parsing
        // <int totalRunCount> then <int startPos><ushort runLength><short diffValue> × totalRunCount
        (int startPos, int runLength, short diffValue)[] patches;
        byte[] errorBytes;
        if (header.Version == "v3.1.0" || header.Version == "v5.0.0")
        {
            using var decompressor = new Decompressor();
            errorBytes = decompressor.Unwrap(
                file.AsSpan(errorOffset, header.ErrorLength)).ToArray();
        }
        else
        {
            errorBytes = new byte[header.ErrorLength];
            Buffer.BlockCopy(file, errorOffset, errorBytes, 0, header.ErrorLength);
        }
        using (var errorMs = new MemoryStream(errorBytes, writable: false))
        using (var reader = new BinaryReader(errorMs, Encoding.UTF8, leaveOpen: true))
        {
            int totalRunCount = reader.ReadInt32();
            if (totalRunCount < 0)
                throw new InvalidDataException($"Invalid negative total run count: {totalRunCount}");

            patches = new (int, int, short)[totalRunCount];
            for (int i = 0; i < totalRunCount; i++)
            {
                int startPos = reader.ReadInt32();
                ushort runLength = reader.ReadUInt16();
                short diffValue = reader.ReadInt16();
                patches[i] = (startPos, runLength, diffValue);
            }
        }

        // ── Fetch all base chunks in parallel ──
        var fetchSw = Stopwatch.StartNew();
        var baseChunks = new byte[chunkCount][];
        var fetchTasks = new Task[chunkCount];
        int primaryHits = 0;
        int fallbackHits = 0;

        // Get all agent IPs once for v3.0.0 safe fallback (content-addressable → any agent is correct)
        string[]? allAgentIps = null;
        if (isV3)
        {
            try { allAgentIps = RendezvousRouter.GetAllAgentIps(); }
            catch { allAgentIps = null; }
        }

        for (int i = 0; i < chunkCount; i++)
        {
            ulong bucketId = refBucketIds[i];
            bool isZeroRef = isV3
                ? (bucketId == 0 && string.IsNullOrEmpty(refStorageGuids[i]))
                : (bucketId == 0 && refBucketIndices[i] == 0);
            bool isMosaicRef = mosaicRefs.ContainsKey(i);

            if (isZeroRef)
            {
                baseChunks[i] = new byte[Globals.chunkSize];
                fetchTasks[i] = Task.CompletedTask;
            }
            else if (isMosaicRef)
            {
                int idx = i;
                var (donors, matchBitmap, selectors, donorPositions) = mosaicRefs[idx];
                fetchTasks[i] = Task.Run(async () =>
                {
                    int subSize = Globals.MosaicSubChunkSize;
                    int nComp = Globals.MosaicNComponents;
                    int chSize = Globals.chunkSize;

                    // Fetch all donor chunks in parallel
                    var donorChunks = new byte[donors.Count][];
                    var donorFetches = new Task[donors.Count];
                    for (int d = 0; d < donors.Count; d++)
                    {
                        int dIdx = d;
                        var (dBucketId, dGuid, dBucketIdx) = donors[dIdx];
                        donorFetches[dIdx] = Task.Run(async () =>
                        {
                            string bitstring = UlongToBitstring(dBucketId);
                            string targetAgent = RendezvousRouter.PickAgent(bitstring);
                            byte[]? chunk = null;
                            if (!string.IsNullOrEmpty(dGuid))
                            {
                                chunk = await _chunkReferenceClient.GetChunkByStorageGuidAsync(dGuid, targetAgent);
                                if (chunk == null && allAgentIps != null)
                                {
                                    foreach (var agent in allAgentIps.Where(ip => ip != targetAgent))
                                    {
                                        try { chunk = await _chunkReferenceClient.GetChunkByStorageGuidAsync(dGuid, agent); }
                                        catch { /* try next */ }
                                        if (chunk != null) break;
                                    }
                                }
                            }
                            else
                            {
                                chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(dBucketId, dBucketIdx, targetAgent);
                                if (chunk == null)
                                {
                                    for (int retry = 0; retry < 3; retry++)
                                    {
                                        await Task.Delay(100 * (retry + 1));
                                        chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(dBucketId, dBucketIdx, targetAgent);
                                        if (chunk != null) break;
                                    }
                                }
                            }
                            donorChunks[dIdx] = chunk ?? new byte[chSize];
                        });
                    }
                    await Task.WhenAll(donorFetches);

                    // Stitch the mosaic base from donors based on bitmap, selectors, and donor positions.
                    // donorPositions != null → cross-position (0x04): copy from donor position j.
                    // donorPositions == null → legacy positional (0x03): copy from same position e.
                    var stitched = new byte[chSize];
                    for (int e = 0; e < nComp && e * subSize < chSize; e++)
                    {
                        if ((matchBitmap & (1UL << e)) == 0) continue;
                        int donorIdx = MosaicChunkInfo.GetSelector(selectors, e);
                        if (donorIdx >= donorChunks.Length) continue;
                        int dstOffset = e * subSize;
                        int srcPos = donorPositions != null ? donorPositions[e] : e;
                        int srcOffset = srcPos * subSize;
                        int len = Math.Min(subSize, chSize - dstOffset);
                        if (srcOffset + len <= donorChunks[donorIdx].Length)
                            Buffer.BlockCopy(donorChunks[donorIdx], srcOffset, stitched, dstOffset, len);
                    }
                    baseChunks[idx] = stitched;
                    Interlocked.Increment(ref primaryHits);
                });
            }
            else
            {
                int idx = i;
                ulong bId = bucketId;
                fetchTasks[i] = Task.Run(async () =>
                {
                    // Route to the primary agent via RendezvousRouter
                    string bitstring = UlongToBitstring(bId);
                    string targetAgent = RendezvousRouter.PickAgent(bitstring);
                    byte[]? chunk = null;

                    string guid = refStorageGuids[idx];
                    bool useStorageGuid = isV3 && !string.IsNullOrEmpty(guid);

                    if (useStorageGuid)
                    {
                        // ── Content-addressable retrieval by storageGuid ──
                        chunk = await _chunkReferenceClient.GetChunkByStorageGuidAsync(guid, targetAgent);
                        if (chunk != null)
                        {
                            Interlocked.Increment(ref primaryHits);
                        }
                        else
                        {
                            if (allAgentIps != null && allAgentIps.Length > 0)
                            {
                                var otherAgents = allAgentIps.Where(ip => ip != targetAgent).ToArray();
                                if (otherAgents.Length > 0)
                                {
                                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                                    var fallbackTasks = otherAgents.Select(async agent =>
                                    {
                                        try
                                        {
                                            return await _chunkReferenceClient.GetChunkByStorageGuidAsync(
                                                guid, agent, cts.Token);
                                        }
                                        catch { return null; }
                                    }).ToList();

                                    while (fallbackTasks.Count > 0)
                                    {
                                        var completed = await Task.WhenAny(fallbackTasks);
                                        fallbackTasks.Remove(completed);
                                        var result = await completed;
                                        if (result != null && result.Length > 0)
                                        {
                                            chunk = result;
                                            Interlocked.Increment(ref fallbackHits);
                                            break;
                                        }
                                    }
                                }
                            }

                            if (chunk == null)
                            {
                                for (int retry = 0; retry < 3; retry++)
                                {
                                    await Task.Delay(100 * (retry + 1));
                                    chunk = await _chunkReferenceClient.GetChunkByStorageGuidAsync(guid, targetAgent);
                                    if (chunk != null) { Interlocked.Increment(ref fallbackHits); break; }
                                }
                            }
                        }

                        if (chunk == null)
                            throw new InvalidDataException(
                                $"Missing base chunk for storageGuid={guid}. " +
                                $"PrimaryAgent={targetAgent}, Bitstring={bitstring}");
                    }
                    else
                    {
                        // ── Retrieval by (bucketId, bucketIndex) ──
                        // Used by v2.x and v3.1.0 compact refs (self-stored chunks)
                        ulong bIdx = refBucketIndices[idx];
                        chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(bId, bIdx, targetAgent);
                        if (chunk != null)
                        {
                            Interlocked.Increment(ref primaryHits);
                        }
                        else
                        {
                            // Retry same agent with backoff (unsafe to query others for v2.x)
                            for (int retry = 0; retry < 3; retry++)
                            {
                                await Task.Delay(100 * (retry + 1));
                                chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(bId, bIdx, targetAgent);
                                if (chunk != null) { Interlocked.Increment(ref fallbackHits); break; }
                            }
                        }

                        if (chunk == null)
                            throw new InvalidDataException(
                                $"Missing base chunk for reference ({bId}, {bIdx}). " +
                                $"Agent={targetAgent}, Bitstring={bitstring}");
                    }

                    baseChunks[idx] = chunk;
                });
            }
        }
        await Task.WhenAll(fetchTasks);
        fetchSw.Stop();
        int zeroRefChunks = isV3
            ? Enumerable.Range(0, chunkCount).Count(i => refBucketIds[i] == 0 && string.IsNullOrEmpty(refStorageGuids[i]))
            : Enumerable.Range(0, chunkCount).Count(i => refBucketIds[i] == 0 && refBucketIndices[i] == 0);
        Console.WriteLine($"[Decompress] FetchBaseChunks: {fetchSw.ElapsedMilliseconds}ms, {chunkCount} chunks, version={header.Version}, primary={primaryHits}, fallback={fallbackHits}, zeroRef={zeroRefChunks}");
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

        // ── Apply flat error encoding (RLE) across the entire buffer ──
        int cursor = 0;
        foreach (var (startPos, runLength, diffValue) in patches)
        {
            cursor += startPos; // Move to start of run (relative position)
            if (cursor < 0 || cursor >= baseBuffer.Length)
                throw new InvalidDataException(
                    $"Patch cursor {cursor} out of range (buffer size {baseBuffer.Length}).");

            // Apply the entire run
            for (int j = 0; j < runLength; j++)
            {
                if (cursor + j >= baseBuffer.Length)
                    throw new InvalidDataException(
                        $"Run extends beyond buffer (cursor={cursor}, runLength={runLength}, buffer={baseBuffer.Length}).");

                int patched = baseBuffer[cursor + j] + diffValue;
                if (patched < 0 || patched > 255)
                    throw new InvalidDataException($"Patched byte {patched} out of range at cursor {cursor + j}.");
                baseBuffer[cursor + j] = (byte)patched;
            }
            cursor += runLength; // Move past the run
        }

        // ── Assemble output: reconstructed buffer + trim chunk ──
        int trimLength;
        byte[]? expectedHash = null;

        if (header.TrimLength >= 0)
        {
            // v2.1.0+/v3.0.0: trimLength is explicit in header; hash follows trim chunk
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

                    // 2) First pass: find best by error COUNT only (RLE run count)
                    int bestErrorCount = Globals.chunkSize;
                    int bestIndex = -1;
                    for (int j = 0; j < chunkBytesArray.Length; j++)
                    {
                        int runCount = Misc.GetErrorEncodingCount(
                            fileChunks[i],
                            chunkBytesArray[j]
                        );

                        if (runCount < bestErrorCount)
                        {
                            bestErrorCount = runCount;
                            bestIndex = j;
                        }
                    }

                    // 3) Second pass: full encoding for the best candidate (RLE format)
                    var bestEncoding = Misc.GetErrorEncoding(
                        fileChunks[i],
                        chunkBytesArray[bestIndex]
                    );

                    final_results[i] = candidates[bestIndex];
                    // Convert RLE runs to legacy (key, value) pairs for old code path compatibility
                    // This is dead code (_CompressFile is never called), but keep it compiling
                    final_error_results[i] = new List<(int, int)>();
                    int legacyLastAppend = 0;
                    foreach (var (startPos, runLength, diffValue) in bestEncoding)
                    {
                        int absStart = legacyLastAppend + startPos;
                        for (int k = 0; k < runLength; k++)
                        {
                            int key = (k == 0) ? (absStart - legacyLastAppend) : 1;
                            final_error_results[i].Add((key, diffValue));
                        }
                        legacyLastAppend = absStart + runLength;
                    }
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
            (byte[], int) _errors = Misc.GetErrorEncodingBytesLegacy(encoded_objects[i].error_encoding, error_bytes_offset);
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
