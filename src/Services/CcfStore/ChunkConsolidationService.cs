using System.Security.Cryptography;
using System.Text;
using Cross.Utilities;

namespace Cross.Services.CcfStore;

/// <summary>
/// Background service that identifies suboptimally packed CCFs and triggers targeted
/// repacking for better P-frame delta compression. Scans packs to find families
/// (same RefsFingerprint) scattered across multiple packs, then consolidates them
/// into optimized packs where families are co-located.
/// Coordinates with CcfPackOptimizerService via a shared lock.
/// </summary>
public class ChunkConsolidationService : BackgroundService
{
    private static readonly TimeSpan ScanInterval = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan InitialDelay = TimeSpan.FromMinutes(5);
    private const int MaxPacksPerCycle = 8;
    private const int MinFamilySize = 3;

    public static volatile bool IsRunning;
    public static DateTime? LastScanUtc;
    public static int LastScannedPacks;
    public static int LastRepackedEntries;
    public static int LastScatteredFamilies;

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!Globals.EnableCcfStore)
        {
            Console.WriteLine("[ChunkConsolidation] Disabled (ENABLE_CCF_STORE != true)");
            return;
        }

        Console.WriteLine("[ChunkConsolidation] Action service started, waiting for initial data...");
        await Task.Delay(InitialDelay, stoppingToken);

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await RunConsolidationCycle(stoppingToken);
            }
            catch (OperationCanceledException) { break; }
            catch (Exception ex)
            {
                Console.WriteLine($"[ChunkConsolidation] Cycle error: {ex.Message}");
            }
            finally
            {
                LastScanUtc = DateTime.UtcNow;
            }

            await Task.Delay(ScanInterval, stoppingToken);
        }
    }

    private async Task RunConsolidationCycle(CancellationToken ct)
    {
        if (!await Globals.CcfOptimizationLock.WaitAsync(TimeSpan.FromSeconds(10), ct))
        {
            Console.WriteLine("[ChunkConsolidation] Skipping cycle, optimizer holds lock");
            return;
        }

        try
        {
            IsRunning = true;
            await RunConsolidationCycleInner(ct);
        }
        finally
        {
            IsRunning = false;
            Globals.CcfOptimizationLock.Release();
        }
    }

    private async Task RunConsolidationCycleInner(CancellationToken ct)
    {
        var store = CcfStoreService.Instance;
        var sw = System.Diagnostics.Stopwatch.StartNew();
        long memoryBudget = Globals.GetDynamicMemoryBudget(0.30);
        DateTime deadline = DateTime.UtcNow.AddMinutes(3);

        // Phase 1: Lightweight fingerprint scan per pack.
        // Track which RefsFingerprint families live in each pack.
        var packFamilies = new Dictionary<string, Dictionary<string, List<string>>>(StringComparer.OrdinalIgnoreCase);
        var globalFamilies = new Dictionary<string, List<(string FileId, string PackId)>>();
        int scanned = 0;
        long bytesRead = 0;

        foreach (var (packId, packBytes) in store.ListPacks())
        {
            ct.ThrowIfCancellationRequested();
            if (DateTime.UtcNow >= deadline || bytesRead > memoryBudget) break;

            var fpMap = new Dictionary<string, List<string>>();

            foreach (var fileId in store.ListPackEntryIds(packId))
            {
                if (string.IsNullOrWhiteSpace(fileId)) continue;

                byte[]? data = await store.GetCcfAsync(fileId, ct);
                if (data == null) continue;

                bytesRead += data.Length;
                string? fp = ExtractRefsFingerprintLight(data);
                if (fp == null) continue;

                if (!fpMap.TryGetValue(fp, out var fpList))
                {
                    fpList = new List<string>();
                    fpMap[fp] = fpList;
                }
                fpList.Add(fileId);

                if (!globalFamilies.TryGetValue(fp, out var gList))
                {
                    gList = new List<(string, string)>();
                    globalFamilies[fp] = gList;
                }
                gList.Add((fileId, packId));

                scanned++;
            }

            if (fpMap.Count > 0)
                packFamilies[packId] = fpMap;
        }

        LastScannedPacks = packFamilies.Count;

        if (packFamilies.Count < 2)
        {
            Console.WriteLine($"[ChunkConsolidation] Only {packFamilies.Count} packs scanned, nothing to consolidate");
            return;
        }

        // Phase 2: Find families scattered across 2+ different packs
        var scatteredFamilies = globalFamilies
            .Where(kv => kv.Value.Count >= MinFamilySize)
            .Where(kv => kv.Value.Select(v => v.PackId).Distinct(StringComparer.OrdinalIgnoreCase).Count() > 1)
            .OrderByDescending(kv => kv.Value.Count)
            .ToList();

        LastScatteredFamilies = scatteredFamilies.Count;

        if (scatteredFamilies.Count == 0)
        {
            Console.WriteLine($"[ChunkConsolidation] Scanned {scanned} entries in {packFamilies.Count} packs, " +
                $"no scattered families found ({sw.ElapsedMilliseconds}ms)");
            return;
        }

        Console.WriteLine($"[ChunkConsolidation] Found {scatteredFamilies.Count} scattered families " +
            $"across {packFamilies.Count} packs ({scanned} entries, {sw.ElapsedMilliseconds}ms)");

        // Phase 3: Select source packs that contain scattered family members
        var packsToRepack = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        foreach (var (_, members) in scatteredFamilies)
        {
            if (packsToRepack.Count >= MaxPacksPerCycle) break;
            foreach (var (_, packId) in members)
                packsToRepack.Add(packId);
        }

        Console.WriteLine($"[ChunkConsolidation] Targeting {packsToRepack.Count} packs for family consolidation");

        // Phase 4: Load ALL entries from selected packs and repack optimally
        var allEntries = new List<CcfPackOptimizerService.LoadedEntry>();
        var packEntryCounts = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        long loadBytes = 0;

        foreach (var packId in packsToRepack)
        {
            ct.ThrowIfCancellationRequested();
            if (loadBytes > memoryBudget) break;

            var ids = store.ListPackEntryIds(packId)
                .Where(id => !string.IsNullOrWhiteSpace(id))
                .Distinct(StringComparer.OrdinalIgnoreCase)
                .ToList();

            int loaded = 0;
            foreach (var fileId in ids)
            {
                byte[]? data = await store.GetCcfAsync(fileId, ct);
                if (data == null) continue;

                loadBytes += data.Length;
                var comp = CcfPackOptimizerService.ParseCcfComponents(data);
                allEntries.Add(new CcfPackOptimizerService.LoadedEntry
                {
                    FileId = fileId,
                    Ccf = data,
                    Comp = comp,
                    SourcePackId = packId
                });
                loaded++;
            }

            packEntryCounts[packId] = ids.Count;
        }

        if (allEntries.Count < 2)
        {
            Console.WriteLine("[ChunkConsolidation] Not enough entries loaded for repacking");
            return;
        }

        Console.WriteLine($"[ChunkConsolidation] Loaded {allEntries.Count} entries ({loadBytes / 1024 / 1024}MB) " +
            $"from {packsToRepack.Count} packs, building optimized pack...");

        // Phase 5: Build optimized pack (groups by fingerprint for P-frame delta)
        var result = await CcfPackOptimizerService.PackEntriesAsync(store, allEntries, ct);

        // Phase 6: Cleanup source packs where ALL entries were migrated (journaled for crash-recovery)
        var fullyMigratedSourcePacks = new List<string>();
        foreach (var packId in packsToRepack)
        {
            int expected = packEntryCounts.TryGetValue(packId, out var n) ? n : 0;
            int migrated = allEntries.Count(e =>
                string.Equals(e.SourcePackId, packId, StringComparison.OrdinalIgnoreCase));

            if (migrated == expected && expected > 0)
            {
                fullyMigratedSourcePacks.Add(packId);
            }
            else
            {
                Console.WriteLine($"[ChunkConsolidation] Kept pack {packId} (migrated {migrated}/{expected})");
            }
        }

        await CcfPackOptimizerService.ApplyCleanupWithJournalAsync(
            store,
            result.PackId,
            Array.Empty<string>(),
            fullyMigratedSourcePacks,
            ct);

        LastRepackedEntries = result.EntryCount;

        CcfPackOptimizerService.PframeGroupsFound += result.PframeGroups;
        CcfPackOptimizerService.PframeDeltaCount += result.PframeDeltaCount;
        CcfPackOptimizerService.PframeSavedBytes += result.PframeSavedBytes;

        double ratio = result.InnerBytes > 0 ? 1.0 - (double)result.CompressedBytes / result.InnerBytes : 0;
        Console.WriteLine($"[ChunkConsolidation] Consolidated {result.EntryCount} entries → {result.PackId}: " +
            $"inner={result.InnerBytes / 1024.0:F1}KB → zstd={result.CompressedBytes / 1024.0:F1}KB ({ratio * 100:F1}%), " +
            $"P-frame: {result.PframeDeltaCount} deltas in {result.PframeGroups} families, " +
            $"cleanup scheduled for {fullyMigratedSourcePacks.Count}/{packsToRepack.Count} source packs ({sw.ElapsedMilliseconds}ms)");
    }

    /// <summary>
    /// Lightweight fingerprint: parse CCF header to extract refs section and hash it.
    /// Does NOT decompress error streams — only reads version + refs.
    /// </summary>
    internal static string? ExtractRefsFingerprintLight(byte[] file)
    {
        try
        {
            if (file.Length < 12) return null;
            int pos = 0;
            int versionLen = BitConverter.ToInt32(file, pos); pos += 4;
            if (versionLen <= 0 || versionLen > 20 || pos + versionLen > file.Length) return null;

            string version = Encoding.UTF8.GetString(file, pos, versionLen); pos += versionLen;
            int refsLen = BitConverter.ToInt32(file, pos); pos += 4;

            bool hasOrigLen = version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0";
            pos += 4;
            if (hasOrigLen) pos += 4;

            bool hasTrim = version is "v2.1.0" or "v3.0.0" or "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0";
            if (hasTrim) pos += 4;

            int refsOffset = pos;
            if (refsLen <= 0 || refsOffset + refsLen > file.Length) return null;

            return Convert.ToHexString(
                SHA256.HashData(file.AsSpan(refsOffset, refsLen))
            ).ToLowerInvariant();
        }
        catch { return null; }
    }
}
