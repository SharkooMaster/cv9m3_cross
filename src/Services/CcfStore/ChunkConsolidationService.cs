using System.Text;
using Cross.Utilities;
using Cross.Services.Grpc.Agent;

namespace Cross.Services.CcfStore;

/// <summary>
/// Phase A: Background service that scans stored CCFs, builds a reverse chunk index,
/// identifies groups of similar chunks, computes centroids, and logs potential savings.
/// Only active when EnableCcfStore is true (cluster-store mode).
/// Read-only — does not modify any CCFs or agent chunk data.
/// </summary>
public class ChunkConsolidationService : BackgroundService
{
    private static readonly TimeSpan ScanInterval = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan InitialDelay = TimeSpan.FromMinutes(5);
    private const int MinRefsForAnalysis = 3;
    private const int MaxChunkFetchesPerCycle = 500;
    private const double SimilarityThreshold = 0.70;

    public static volatile bool IsRunning;
    public static DateTime? LastScanUtc;
    public static int LastTotalChunksScanned;
    public static int LastGroupsFound;
    public static long LastEstimatedSavings;

    private readonly ChunkReferenceServiceClient _chunkClient = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!Globals.EnableCcfStore)
        {
            Console.WriteLine("[ChunkConsolidation] Disabled (ENABLE_CCF_STORE != true)");
            return;
        }

        Console.WriteLine("[ChunkConsolidation] Service started, waiting for initial data accumulation...");
        await Task.Delay(InitialDelay, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                IsRunning = true;
                await RunConsolidationAnalysis(stoppingToken);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[ChunkConsolidation] Scan error: {ex.Message}");
            }
            finally
            {
                IsRunning = false;
                LastScanUtc = DateTime.UtcNow;
            }

            await Task.Delay(ScanInterval, stoppingToken);
        }
    }

    private async Task RunConsolidationAnalysis(CancellationToken ct)
    {
        var store = CcfStoreService.Instance;
        var sw = System.Diagnostics.Stopwatch.StartNew();

        // Step 1: Collect all CCF file IDs (unpacked + packed)
        var allCcfIds = new List<string>();
        foreach (var id in store.ListUnpackedCcfs())
            allCcfIds.Add(id);
        foreach (var id in store.ListPackedCcfIds())
            allCcfIds.Add(id);

        if (allCcfIds.Count < 10)
        {
            Console.WriteLine($"[ChunkConsolidation] Only {allCcfIds.Count} CCFs in store, skipping analysis");
            return;
        }

        Console.WriteLine($"[ChunkConsolidation] Scanning {allCcfIds.Count} CCFs for chunk reference analysis...");

        // Step 2: Parse each CCF's ref table, build reverse index
        // Key: (bucketId, bucketIndex), Value: list of CCF fileIds referencing it
        var reverseIndex = new Dictionary<(ulong BucketId, ulong BucketIndex), List<string>>();
        int parsedCount = 0;
        int parseErrors = 0;

        foreach (var fileId in allCcfIds)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                byte[]? ccfData = await store.GetCcfAsync(fileId, ct);
                if (ccfData == null) continue;

                var refs = ExtractChunkRefs(ccfData);
                foreach (var r in refs)
                {
                    if (r.BucketId == 0 && r.BucketIndex == 0) continue;
                    var key = (r.BucketId, r.BucketIndex);
                    if (!reverseIndex.TryGetValue(key, out var list))
                    {
                        list = new List<string>();
                        reverseIndex[key] = list;
                    }
                    if (!list.Contains(fileId))
                        list.Add(fileId);
                }
                parsedCount++;
            }
            catch (Exception ex)
            {
                parseErrors++;
                if (parseErrors <= 5)
                    Console.WriteLine($"[ChunkConsolidation] Parse error on {fileId}: {ex.Message}");
            }
        }

        int totalUniqueChunks = reverseIndex.Count;
        int multiRefChunks = reverseIndex.Count(kv => kv.Value.Count >= MinRefsForAnalysis);

        Console.WriteLine($"[ChunkConsolidation] Parsed {parsedCount} CCFs ({parseErrors} errors), " +
            $"{totalUniqueChunks} unique chunk refs, {multiRefChunks} chunks referenced by {MinRefsForAnalysis}+ CCFs");

        LastTotalChunksScanned = totalUniqueChunks;

        if (multiRefChunks == 0)
        {
            Console.WriteLine("[ChunkConsolidation] No chunks with enough cross-CCF references for grouping");
            LastGroupsFound = 0;
            LastEstimatedSavings = 0;
            return;
        }

        // Step 3: Fetch chunk bytes for highly-referenced chunks
        var candidateChunks = reverseIndex
            .Where(kv => kv.Value.Count >= MinRefsForAnalysis)
            .OrderByDescending(kv => kv.Value.Count)
            .Take(MaxChunkFetchesPerCycle)
            .ToList();

        Console.WriteLine($"[ChunkConsolidation] Fetching {candidateChunks.Count} candidate chunks from agents...");

        var chunkData = new Dictionary<(ulong, ulong), byte[]>();
        int fetchSuccess = 0, fetchFail = 0;

        foreach (var (key, ccfIds) in candidateChunks)
        {
            ct.ThrowIfCancellationRequested();
            try
            {
                string bitstring = UlongToBitstring(key.BucketId);
                string targetAgent = RendezvousRouter.PickAgent(bitstring);
                byte[]? data = await _chunkClient.GetChunkByReferenceAsync(
                    key.BucketId, key.BucketIndex, targetAgent, ct);

                if (data != null && data.Length > 0)
                {
                    chunkData[key] = data;
                    fetchSuccess++;
                }
                else
                {
                    fetchFail++;
                }

                if (fetchSuccess % 50 == 0 && fetchSuccess > 0)
                    await Task.Delay(10, ct);
            }
            catch (Exception ex)
            {
                fetchFail++;
                if (fetchFail <= 3)
                    Console.WriteLine($"[ChunkConsolidation] Fetch error for ({key.BucketId},{key.BucketIndex}): {ex.Message}");
            }
        }

        Console.WriteLine($"[ChunkConsolidation] Fetched {fetchSuccess} chunks ({fetchFail} failures)");

        if (fetchSuccess < 2)
        {
            LastGroupsFound = 0;
            LastEstimatedSavings = 0;
            return;
        }

        // Step 4: Group similar chunks using byte-level similarity
        var chunkList = chunkData.ToList();
        var groups = GroupSimilarChunks(chunkList, ct);

        Console.WriteLine($"[ChunkConsolidation] Found {groups.Count} groups of similar chunks");
        LastGroupsFound = groups.Count;

        // Step 5: For each group, compute centroid and estimate savings
        long totalEstimatedSavings = 0;
        int groupIdx = 0;

        foreach (var group in groups)
        {
            ct.ThrowIfCancellationRequested();
            if (group.Count < 2) continue;

            byte[] centroid = ComputeCentroid(group.Select(g => g.Data).ToList());
            int centroidSize = centroid.Length;

            int totalOriginalErrors = 0;
            int totalCentroidErrors = 0;

            foreach (var member in group)
            {
                int originalErrors = CountDifferences(member.Data, new byte[member.Data.Length]);
                int centroidErrors = CountDifferences(member.Data, centroid);

                int ccfRefsCount = reverseIndex.TryGetValue(member.Key, out var refs) ? refs.Count : 0;
                totalOriginalErrors += originalErrors * ccfRefsCount;
                totalCentroidErrors += centroidErrors * ccfRefsCount;
            }

            long savedBytes = totalOriginalErrors - totalCentroidErrors;
            if (savedBytes > 0)
                totalEstimatedSavings += savedBytes;

            if (groupIdx < 5)
            {
                Console.WriteLine($"[ChunkConsolidation]   Group {groupIdx + 1}: {group.Count} chunks, " +
                    $"avg errors before={totalOriginalErrors / Math.Max(1, group.Count)}, " +
                    $"avg errors with centroid={totalCentroidErrors / Math.Max(1, group.Count)}, " +
                    $"estimated byte savings={savedBytes}");
            }
            groupIdx++;
        }

        LastEstimatedSavings = totalEstimatedSavings;
        sw.Stop();

        Console.WriteLine($"[ChunkConsolidation] Analysis complete in {sw.ElapsedMilliseconds}ms: " +
            $"{groups.Count} groups, estimated savings={totalEstimatedSavings / 1024.0:F1}KB " +
            $"across {parsedCount} CCFs");
    }

    /// <summary>
    /// Extract (bucketId, bucketIndex) pairs from a CCF's reference section.
    /// Supports both v5.x compact ref table and v3.x legacy formats.
    /// </summary>
    private static List<(ulong BucketId, ulong BucketIndex)> ExtractChunkRefs(byte[] file)
    {
        var refs = new List<(ulong, ulong)>();
        try
        {
            if (file.Length < 12) return refs;

            int pos = 0;
            int versionLen = BitConverter.ToInt32(file, pos); pos += 4;
            if (versionLen <= 0 || versionLen > 20 || pos + versionLen > file.Length) return refs;

            string version = Encoding.UTF8.GetString(file, pos, versionLen); pos += versionLen;
            int refsLen = BitConverter.ToInt32(file, pos); pos += 4;

            bool hasOrigLen = version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0";
            pos += 4; // errorCompLen
            if (hasOrigLen) pos += 4;

            bool hasTrim = version is "v2.1.0" or "v3.0.0" or "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0";
            if (hasTrim) pos += 4;

            int refsOffset = pos;
            if (refsOffset + refsLen > file.Length) return refs;

            if (version is "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0")
            {
                int off = refsOffset;
                if (off + 6 > file.Length) return refs;

                int chunkCount = BitConverter.ToInt32(file, off); off += 4;
                ushort refTableSize = BitConverter.ToUInt16(file, off); off += 2;

                if (off + refTableSize * 16 > file.Length) return refs;

                var refTable = new (ulong BucketId, ulong BucketIndex)[refTableSize];
                for (int i = 0; i < refTableSize; i++)
                {
                    ulong bucketId = BitConverter.ToUInt64(file, off); off += 8;
                    ulong bucketIndex = BitConverter.ToUInt64(file, off); off += 8;
                    refTable[i] = (bucketId, bucketIndex);
                }

                for (int i = 0; i < chunkCount && off < file.Length; i++)
                {
                    byte flag = file[off++];
                    switch (flag)
                    {
                        case 0x00:
                            break;
                        case 0x01:
                        case 0x02:
                        {
                            if (off + 2 > file.Length) return refs;
                            ushort tableIdx = BitConverter.ToUInt16(file, off); off += 2;
                            if (tableIdx < refTableSize)
                                refs.Add(refTable[tableIdx]);
                            break;
                        }
                        case 0x04:
                        {
                            if (off >= file.Length) return refs;
                            int donorCount = file[off++];
                            for (int d = 0; d < donorCount; d++)
                            {
                                if (off + 2 > file.Length) return refs;
                                ushort tableIdx = BitConverter.ToUInt16(file, off); off += 2;
                                if (tableIdx < refTableSize)
                                    refs.Add(refTable[tableIdx]);
                            }
                            off += 8 + 32 + 64; // matchBitmap + selectors + donorPositions
                            break;
                        }
                        case 0x05:
                        {
                            int bitmaskLen = (Globals.chunkSize + 7) / 8;
                            for (int d = 0; d < 2; d++)
                            {
                                if (off + 2 > file.Length) return refs;
                                ushort tableIdx = BitConverter.ToUInt16(file, off); off += 2;
                                if (tableIdx < refTableSize)
                                    refs.Add(refTable[tableIdx]);
                            }
                            off += bitmaskLen;
                            break;
                        }
                        default:
                            return refs;
                    }
                }
            }
        }
        catch { }
        return refs;
    }

    /// <summary>
    /// Group chunks by byte-level similarity using single-linkage clustering.
    /// </summary>
    private static List<List<((ulong BucketId, ulong BucketIndex) Key, byte[] Data)>> GroupSimilarChunks(
        List<KeyValuePair<(ulong, ulong), byte[]>> chunks, CancellationToken ct)
    {
        int n = chunks.Count;
        int[] parent = new int[n];
        for (int i = 0; i < n; i++) parent[i] = i;

        int Find(int x)
        {
            while (parent[x] != x) { parent[x] = parent[parent[x]]; x = parent[x]; }
            return x;
        }

        void Union(int a, int b)
        {
            int ra = Find(a), rb = Find(b);
            if (ra != rb) parent[ra] = rb;
        }

        for (int i = 0; i < n && !ct.IsCancellationRequested; i++)
        {
            for (int j = i + 1; j < n; j++)
            {
                if (Find(i) == Find(j)) continue;

                double sim = ByteSimilarity(chunks[i].Value, chunks[j].Value);
                if (sim >= SimilarityThreshold)
                    Union(i, j);
            }
        }

        var groupMap = new Dictionary<int, List<int>>();
        for (int i = 0; i < n; i++)
        {
            int root = Find(i);
            if (!groupMap.TryGetValue(root, out var list))
            {
                list = new List<int>();
                groupMap[root] = list;
            }
            list.Add(i);
        }

        var result = new List<List<((ulong, ulong) Key, byte[] Data)>>();
        foreach (var (_, members) in groupMap)
        {
            if (members.Count < 2) continue;
            var group = members.Select(i => (chunks[i].Key, chunks[i].Value)).ToList();
            result.Add(group);
        }

        return result;
    }

    private static double ByteSimilarity(byte[] a, byte[] b)
    {
        int len = Math.Min(a.Length, b.Length);
        if (len == 0) return 0;

        int matching = 0;
        for (int i = 0; i < len; i++)
        {
            if (a[i] == b[i]) matching++;
        }
        return (double)matching / Math.Max(a.Length, b.Length);
    }

    /// <summary>
    /// Compute a per-byte majority-vote centroid from a set of chunks.
    /// For each byte position, picks the value that appears most often.
    /// </summary>
    private static byte[] ComputeCentroid(List<byte[]> chunks)
    {
        if (chunks.Count == 0) return Array.Empty<byte>();
        int len = chunks[0].Length;

        byte[] centroid = new byte[len];
        int[] counts = new int[256];

        for (int pos = 0; pos < len; pos++)
        {
            Array.Clear(counts, 0, 256);
            foreach (var chunk in chunks)
            {
                if (pos < chunk.Length)
                    counts[chunk[pos]]++;
            }

            int bestVal = 0, bestCount = 0;
            for (int v = 0; v < 256; v++)
            {
                if (counts[v] > bestCount)
                {
                    bestCount = counts[v];
                    bestVal = v;
                }
            }
            centroid[pos] = (byte)bestVal;
        }
        return centroid;
    }

    private static int CountDifferences(byte[] a, byte[] b)
    {
        int len = Math.Max(a.Length, b.Length);
        int diffs = 0;
        for (int i = 0; i < len; i++)
        {
            byte va = i < a.Length ? a[i] : (byte)0;
            byte vb = i < b.Length ? b[i] : (byte)0;
            if (va != vb) diffs++;
        }
        return diffs;
    }

    private static string UlongToBitstring(ulong packed)
    {
        char[] chars = new char[64];
        for (int i = 0; i < 64; i++)
            chars[i] = (packed & (1UL << i)) != 0 ? '1' : '0';
        return new string(chars);
    }
}
