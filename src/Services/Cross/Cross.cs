
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;
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
    // v1.0.0: Diff-only format. Chunks live on the server, compressed file has only references + patches.
    // When base == original (newly stored chunk), patch list is empty → ~20 bytes per chunk.
    private const string EncodingVersion = "v1.0.0";
    string headID = "";
    // OPTIMIZATION: Increased cache size from 10k to 100k for better hit rate
    private static readonly SearchCacheService _searchCache = new SearchCacheService(maxCacheSize: 100000);
    private readonly global::Cross.Services.Grpc.Agent.ChunkReferenceServiceClient _chunkReferenceClient = new();

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
        byte[] trimChunk)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        var versionBytes = Encoding.UTF8.GetBytes(version);
        bw.Write(versionBytes.Length);
        bw.Write(versionBytes);
        bw.Write(references.Length);
        bw.Write(errorDictionary.Length);
        bw.Write(references);
        bw.Write(errorDictionary);
        bw.Write(trimChunk);
        bw.Flush();

        return ms.ToArray();
    }

    private static (string Version, int ReferencesLength, int ErrorLength, int PayloadStart) ParseHeader(ReadOnlySpan<byte> payload)
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

        return (version, referencesLength, errorLength, offset);
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
        List<byte[]> fileChunks;
        List<byte> trimmedChunk;
        {
            using var stage = Observability.StartStage("SplitChunks");
            var swStage = Stopwatch.StartNew();
            (fileChunks, trimmedChunk) = SplitChunks(_file, Globals.chunkSize);
            swStage.Stop();
            Observability.RecordStage("SplitChunks", swStage.Elapsed.TotalMilliseconds, ("chunk_count", fileChunks.Count));
        }

        // Vectorize
        List<float[]> vectors;
        {
            using var stage = Observability.StartStage("Vectorize");
            var swStage = Stopwatch.StartNew();
            await addEvent("Preprocessing", "Vectorizing chunks");
            vectors = Misc.Compute64ElementLSHVectors(fileChunks);
            swStage.Stop();
            Observability.RecordStage("Vectorize", swStage.Elapsed.TotalMilliseconds, ("chunk_count", vectors.Count));
        }

        // Extract bitstring
        List<string> bitStrings;
        {
            using var stage = Observability.StartStage("ExtractBucketKeys");
            var swStage = Stopwatch.StartNew();
            await addEvent("Preprocessing", "Extracting (bucket id) bitstring");
            bitStrings = Misc.ComputeBitStringFromVectors(vectors);
            swStage.Stop();
            Observability.RecordStage("ExtractBucketKeys", swStage.Elapsed.TotalMilliseconds, ("bucket_count", bitStrings.Count));
        }

        // Search using streaming for better performance with caching
        Stopwatch sw = Stopwatch.StartNew();
        // - Prepare queries (without chunk bytes for network efficiency)
        List<QueryObject> queries = GetQueryObjects(fileChunks, vectors, bitStrings);
        
        // Keep chunk mapping for background storage after returning file
        Dictionary<int, byte[]> chunkMap = new Dictionary<int, byte[]>();
        for (int i = 0; i < fileChunks.Count; i++)
        {
            // CRITICAL: All chunks in fileChunks should be full-size (5120 bytes) - trim chunk was removed
            if (fileChunks[i] == null || fileChunks[i].Length == 0)
            {
                throw new InvalidOperationException($"FATAL: Empty chunk at index {i} in fileChunks. This should NEVER happen - all chunks should be full-size (5120 bytes). Trim chunks are handled separately.");
            }
            if (fileChunks[i].Length != Globals.chunkSize)
            {
                throw new InvalidOperationException($"FATAL: Chunk at index {i} has size {fileChunks[i].Length}, expected {Globals.chunkSize}. Trim chunks should have been removed by SplitChunks.");
            }
            chunkMap[i] = fileChunks[i];
        }

        // - Check cache and prepare queries to send
        Dictionary<int, QueryResponseObject> queryResults = new Dictionary<int, QueryResponseObject>();
        List<QueryObject> queriesToSend = new List<QueryObject>();
        List<int> queryIndices = new List<int>();
        
        // Check cache for each query
        for (int i = 0; i < queries.Count; i++)
        {
            var query = queries[i];
            var cached = _searchCache.GetCachedResult(query.Vector.ToArray(), query.BucketString);
            if (cached != null)
            {
                // Use cached result
                queryResults[i] = cached;
                // Removed Console.WriteLine for performance (hot path)
            }
            else
            {
                // Need to search
                queriesToSend.Add(query);
                queryIndices.Add(i);
            }
        }

        // Create async enumerable of queries that need searching
        async IAsyncEnumerable<QueryObject> StreamQueries()
        {
            for (int i = 0; i < queriesToSend.Count; i++)
            {
                yield return queriesToSend[i];
            }
        }

        // Stream queries to Gateway and receive results (only for non-cached)
        if (queriesToSend.Count > 0)
        {
            using var stage = Observability.StartStage("SearchBuckets");
            var swStage = Stopwatch.StartNew();
            // Removed Console.WriteLine for performance (hot path)
            await foreach (var response in Globals.searchAllServiceClient.SearchAllStreamAsync(StreamQueries()))
            {
                // IMPORTANT: Gateway streams results as they complete (out of order).
                // Always use response.Index to map back to the original chunk index.
                int originalIndex = response.Index;
                queryResults[originalIndex] = response;

                // Cache only valid responses. Fallback/error responses (0,0) can poison cache and
                // repeatedly force bad bases on subsequent chunks/files.
                if (!(response.BucketId == 0 && response.BucketKey == 0))
                {
                    var query = queries[originalIndex];
                    _searchCache.CacheResult(query.Vector.ToArray(), query.BucketString, response);
                }

                // Removed Console.WriteLine for performance (hot path - logs every query result)
            }
            // Removed Console.WriteLine for performance
            swStage.Stop();
            Observability.RecordStage("SearchBuckets", swStage.Elapsed.TotalMilliseconds, ("query_count", queriesToSend.Count));
        }

        // - Sort by index (ensure all indices are present)
        List<QueryResponseObject?> sorted = new List<QueryResponseObject?>(queries.Count);
        for (int i = 0; i < queries.Count; i++)
        {
            if (queryResults.TryGetValue(i, out var result))
            {
                sorted.Add(result);
            }
            else
            {
                sorted.Add(null); // Will be handled in error check below
            }
        }

        // ── BATCH STORE: one gRPC call per agent instead of one per chunk ──
        // Rendezvous hash already told us which agent owns each chunk.
        // Group by agent, send ONE BatchStore per agent → ~5 calls instead of ~8,000.
        {
            using var stage = Observability.StartStage("StoreChunks");
            var swStage = Stopwatch.StartNew();

            // Collect all chunks that need storing, grouped by target agent
            var agentGroups = new Dictionary<string, List<(int index, QueryResponseObject response, byte[] chunk, float[] vector, string bucketString)>>();
            for (int i = 0; i < sorted.Count; i++)
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
                if (!agentGroups.TryGetValue(agent, out var list))
                {
                    list = new List<(int, QueryResponseObject, byte[], float[], string)>();
                    agentGroups[agent] = list;
                }
                list.Add((i, sorted[i], chunkBytes, queries[i].Vector.ToArray(), queries[i].BucketString));
            }

            // Fire ONE BatchStore per agent — in parallel
            int totalStored = 0;
            if (agentGroups.Count > 0)
            {
                var batchTasks = agentGroups.Select(async kv =>
                {
                    var agent = kv.Key;
                    var items = kv.Value;
                    try
                    {
                        var client = GrpcChannelFactory.GetClient(
                            target: agent,
                            ctor: chan => new StoreVector.StoreVectorClient(chan),
                            roundRobin: false, port: 5000);

                        var batchReq = new BatchStoreVector_Req();
                        foreach (var item in items)
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

                        // Generous deadline: batch may have thousands of chunks
                        var batchRes = await client.BatchStoreAsync(batchReq,
                            deadline: DateTime.UtcNow.AddSeconds(60));

                        // Map results back to sorted[] responses
                        for (int j = 0; j < items.Count && j < batchRes.Results.Count; j++)
                        {
                            var res = batchRes.Results[j];
                            items[j].response.BucketId = res.Id;
                            items[j].response.BucketKey = res.Index;
                        }
                        Interlocked.Add(ref totalStored, items.Count);
                    }
                    catch
                    {
                        // Batch failed — IDs stay 0, safety net in diff encoding handles it
                    }
                }).ToList();

                await Task.WhenAll(batchTasks);
            }

            swStage.Stop();
            Observability.RecordStage("StoreChunks", swStage.Elapsed.TotalMilliseconds,
                ("agents", agentGroups.Count), ("chunks_stored", totalStored));
        }

        // Stats: "references found" = chunks that re-used an existing base (not newly stored chunks).
        // Count only when Duplicate=true AND similarity < 1.0 (excludes newly stored chunks where similarity=1.0).
        int referencesFound = sorted.Count(r => r != null && r.Duplicate && r.Similarity < 1.0f);
        int totalChunks = sorted.Count;
        int nullCount = sorted.Count(r => r == null);
        int zeroIdCount = sorted.Count(r => r != null && r.BucketId == 0 && r.BucketKey == 0);
        int validRefCount = sorted.Count(r => r != null && r.BucketId > 0);
        int emptyChunkRefs = sorted.Count(r => r != null && r.BucketId > 0 && (r.Chunk == null || r.Chunk.Length == 0));
        // Removed Console.WriteLine for performance (stats available via observability)

        // Encode references and per-chunk error dictionary.
        // Diff-only format: chunks live on the server, compressed file has only references + diff patches.
        // When base == original (newly stored chunk), diff is EMPTY → ~20 bytes per chunk.
        var references = new List<byte>(sorted.Count * (sizeof(ulong) * 2));
        var perChunkPatches = new List<List<(int key, int value)>>(sorted.Count);
        int diffCount = 0;
        int emptyDiffCount = 0;
        int zeroRefCount = 0;
        {
            using var stage = Observability.StartStage("DiffEncode");
            var swStage = Stopwatch.StartNew();

            // ── BASE CHUNK FETCH (fallback only) ──
            // Agent search now returns base chunk bytes from its MRU cache (O(1)).
            // This fetch is only needed for the RARE case where the agent's MRU cache
            // evicted the chunk (>512MB of chunks on one agent). Most files will have
            // zero entries in baseChunkFetchIndices.
            var baseChunkFetchIndices = new List<int>();
            for (int i = 0; i < sorted.Count; i++)
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
                var fetchedChunks = new byte[sorted.Count][];
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
                for (int i = 0; i < sorted.Count; i++)
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
                            failedFetchStore.Add((idx, sorted[idx], chunkBytes, agent, queries[idx].Vector.ToArray(), queries[idx].BucketString));
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

            // ── Now encode references + diffs ──
            for (int i = 0; i < sorted.Count; i++)
            {
                // Encode reference (bucketId, bucketKey) — always 16 bytes
                references.AddRange(BitConverter.GetBytes(sorted[i].BucketId));
                references.AddRange(BitConverter.GetBytes(sorted[i].BucketKey));

                if (sorted[i].BucketId == 0 && sorted[i].BucketKey == 0)
                    zeroRefCount++;

                // Fast path: base == original → empty diff
                bool isExactMatch = sorted[i].Similarity >= 0.999999f;
                if ((sorted[i].NeedToStore || isExactMatch) && sorted[i].BucketId != 0)
                {
                    perChunkPatches.Add(new List<(int key, int value)>());
                    emptyDiffCount++;
                    continue;
                }

                // Compute diff: original chunk vs base chunk
                byte[] baseChunk;
                if (sorted[i].Chunk != null && sorted[i].Chunk.Length > 0)
                    baseChunk = sorted[i].Chunk.ToByteArray();
                else
                    baseChunk = new byte[Globals.chunkSize]; // zeros fallback

                var diff = Misc.GetErrorEncoding(fileChunks[i], baseChunk);
                perChunkPatches.Add(diff);

                if (diff.Count == 0)
                    emptyDiffCount++;
                else
                    diffCount++;
            }
            swStage.Stop();
            Observability.RecordStage("DiffEncode", swStage.Elapsed.TotalMilliseconds,
                ("chunk_count", sorted.Count), ("empty_diff", emptyDiffCount), ("non_empty_diff", diffCount),
                ("zero_ref", zeroRefCount));
        }

        // Error dictionary layout (v1.0.0 — diff only):
        // <int chunkCount>
        // Per chunk: <int pairCount> then <int deltaIndex><short delta> × pairCount
        byte[] errorDictionaryBytes;
        using (var errorMs = new MemoryStream())
        using (var errorWriter = new BinaryWriter(errorMs, Encoding.UTF8, leaveOpen: true))
        {
            errorWriter.Write(sorted.Count);
            for (int i = 0; i < sorted.Count; i++)
            {
                var patch = perChunkPatches[i];
                errorWriter.Write(patch.Count); // 0 for empty diff (base == original)
                foreach (var pair in patch)
                {
                    errorWriter.Write(pair.key);
                    errorWriter.Write((short)pair.value);
                }
            }
            errorWriter.Flush();
            errorDictionaryBytes = errorMs.ToArray();
        }

        byte[] toReturn;
        {
            using var stage = Observability.StartStage("Serialize");
            var swStage = Stopwatch.StartNew();
            toReturn = BuildCompressedPayload(
                EncodingVersion,
                references.ToArray(),
                errorDictionaryBytes,
                trimmedChunk.ToArray());
            swStage.Stop();
            Observability.RecordStage("Serialize", swStage.Elapsed.TotalMilliseconds, ("output_bytes", toReturn.Length));
        }

        // Return
        sw.Stop();
        // Removed Console.WriteLine for performance (stats available via observability)
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

        if (!string.Equals(header.Version, "v1.0.0", StringComparison.Ordinal))
            throw new InvalidDataException($"Unsupported encoding version '{header.Version}'. Expected v1.0.0.");

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

        // Parse error dictionary (v1.0.0 — diff only).
        // Per chunk: <int pairCount> then <int deltaIndex><short delta> × pairCount
        var chunkPatches = new List<(int key, short delta)>[chunkCount];
        using (var errorMs = new MemoryStream(file, errorOffset, header.ErrorLength, writable: false))
        using (var reader = new BinaryReader(errorMs, Encoding.UTF8, leaveOpen: true))
        {
            int encodedChunkCount = reader.ReadInt32();
            if (encodedChunkCount != chunkCount)
                throw new InvalidDataException($"Error dictionary chunk count mismatch. refs={chunkCount}, errors={encodedChunkCount}");

            for (int i = 0; i < encodedChunkCount; i++)
            {
                int pairCount = reader.ReadInt32();
                if (pairCount < 0)
                    throw new InvalidDataException($"Invalid negative patch pair count ({pairCount}) at chunk {i}.");

                var list = new List<(int key, short delta)>(pairCount);
                for (int j = 0; j < pairCount; j++)
                {
                    int key = reader.ReadInt32();
                    short delta = reader.ReadInt16();
                    list.Add((key, delta));
                }
                chunkPatches[i] = list;
            }
        }

        using var output = new MemoryStream(chunkCount * Globals.chunkSize + (file.Length - trimOffset));
        var applySw = Stopwatch.StartNew();
        for (int i = 0; i < chunkCount; i++)
        {
            // Fetch base chunk from server using the reference.
            byte[] baseChunk;
            var reference = references[i];
            if (reference.BucketId == 0 && reference.BucketIndex == 0)
            {
                // Store failed during compression — zero-base fallback (diff reconstructs from zeros).
                baseChunk = new byte[Globals.chunkSize];
            }
            else
            {
                // OPTIMIZATION: Retry logic for chunks that might not be stored yet (background storage)
                // This handles the race condition where decompression happens immediately after compression
                baseChunk = await _chunkReferenceClient.GetChunkByReferenceAsync(reference.BucketId, reference.BucketIndex);
                
                if (baseChunk == null)
                {
                    // Retry with exponential backoff (chunk might still be storing in background)
                    for (int retry = 0; retry < 3; retry++)
                    {
                        await Task.Delay(100 * (int)Math.Pow(2, retry)); // 100ms, 200ms, 400ms
                        baseChunk = await _chunkReferenceClient.GetChunkByReferenceAsync(reference.BucketId, reference.BucketIndex);
                        if (baseChunk != null)
                            break;
                    }
                    
                    if (baseChunk == null)
                        throw new InvalidDataException($"Missing base chunk for reference ({reference.BucketId}, {reference.BucketIndex}).");
                }
            }

            if (baseChunk.Length != Globals.chunkSize)
                throw new InvalidDataException($"Base chunk length {baseChunk.Length} differs from chunk size {Globals.chunkSize} for chunk {i}.");

            // Apply diff patches to reconstruct original chunk.
            // Empty patch list (pairCount == 0) means base == original → just write the base.
            int cursor = 0;
            foreach (var (deltaIndex, deltaValue) in chunkPatches[i])
            {
                cursor += deltaIndex;
                if (cursor < 0 || cursor >= baseChunk.Length)
                    throw new InvalidDataException($"Patch index out of range at chunk {i}, cursor {cursor}.");

                int patched = baseChunk[cursor] + deltaValue;
                if (patched < 0 || patched > 255)
                    throw new InvalidDataException($"Patched byte out of range at chunk {i}, index {cursor}.");
                baseChunk[cursor] = (byte)patched;
            }

            await output.WriteAsync(baseChunk);
        }

        // Final trim chunk (if any) is appended verbatim.
        if (trimOffset < file.Length)
        {
            await output.WriteAsync(file.AsMemory(trimOffset, file.Length - trimOffset));
        }
        applySw.Stop();
        Observability.RecordStage("ApplyPatchAndEgress", applySw.Elapsed.TotalMilliseconds, ("chunk_count", chunkCount));

        return output.ToArray();
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
