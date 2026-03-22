using Cross.Utilities;
using ZstdSharp;

namespace Cross.Services.CcfStore;

/// <summary>
/// Filesystem-backed CCF storage for cluster-stored mode.
/// Stores individual CCFs and packs under a PVC-mounted directory.
///
/// Layout:
///   {BasePath}/ccf/{fileId}.ccf                 — individual CCFs
///   {BasePath}/packs/{packId}.pack              — packed CCFs (error streams concatenated)
///   {BasePath}/packs/{packId}.idx               — pack index (fileId → offset, length)
///   {BasePath}/dict/latest.dict                 — trained zstd dictionary
/// </summary>
public class CcfStoreService
{
    private static readonly Lazy<CcfStoreService> _instance = new(() => new CcfStoreService());
    public static CcfStoreService Instance => _instance.Value;

    private readonly string _basePath;
    private readonly string _ccfDir;
    private readonly string _packsDir;
    private readonly string _dictDir;

    private CcfStoreService()
    {
        _basePath = Globals.CcfStorePath;
        _ccfDir = Path.Combine(_basePath, "ccf");
        _packsDir = Path.Combine(_basePath, "packs");
        _dictDir = Path.Combine(_basePath, "dict");

        Directory.CreateDirectory(_ccfDir);
        Directory.CreateDirectory(_packsDir);
        Directory.CreateDirectory(_dictDir);
    }

    public string CcfDirectory => _ccfDir;
    public string PacksDirectory => _packsDir;
    public string DictDirectory => _dictDir;

    public async Task StoreCcfAsync(string fileId, byte[] ccfBytes, CancellationToken ct = default)
    {
        string path = GetCcfPath(fileId);
        await WriteFileAtomicAsync(path, ccfBytes, ct);
        Console.WriteLine($"[CcfStore] Stored {fileId}.ccf ({ccfBytes.Length} bytes)");
    }

    public async Task<byte[]?> GetCcfAsync(string fileId, CancellationToken ct = default)
    {
        string ccfPath = GetCcfPath(fileId);
        if (File.Exists(ccfPath))
            return await File.ReadAllBytesAsync(ccfPath, ct);

        byte[]? fromPack = await GetCcfFromPackAsync(fileId, ct);
        if (fromPack != null) return fromPack;

        return null;
    }

    public bool CcfExists(string fileId)
    {
        if (File.Exists(GetCcfPath(fileId))) return true;
        return PackIndexContains(fileId);
    }

    public void DeleteCcf(string fileId)
    {
        string path = GetCcfPath(fileId);
        if (File.Exists(path))
        {
            File.Delete(path);
            Console.WriteLine($"[CcfStore] Deleted individual {fileId}.ccf");
        }
    }

    public IEnumerable<string> ListUnpackedCcfs()
    {
        if (!Directory.Exists(_ccfDir)) yield break;
        foreach (var file in Directory.EnumerateFiles(_ccfDir, "*.ccf"))
            yield return Path.GetFileNameWithoutExtension(file);
    }

    public IEnumerable<string> ListPackedCcfIds()
    {
        if (!Directory.Exists(_packsDir)) yield break;
        foreach (var idxFile in Directory.EnumerateFiles(_packsDir, "*.idx"))
        {
            FileStream? fs = null;
            BinaryReader? br = null;
            try
            {
                fs = new FileStream(idxFile, FileMode.Open, FileAccess.Read, FileShare.Read);
                br = new BinaryReader(fs);
                int entryCount = br.ReadInt32();
                byte[] idBytes = new byte[64];
                for (int i = 0; i < entryCount; i++)
                {
                    int bytesRead = br.Read(idBytes, 0, 64);
                    if (bytesRead < 64) break;
                    br.ReadInt64(); // offset
                    br.ReadInt32(); // length
                    string entryId = System.Text.Encoding.UTF8.GetString(idBytes).TrimEnd('\0');
                    if (!string.IsNullOrEmpty(entryId))
                        yield return entryId;
                }
            }
            finally
            {
                br?.Dispose();
                fs?.Dispose();
            }
        }
    }

    public IEnumerable<(string PackId, long PackBytes)> ListPacks()
    {
        if (!Directory.Exists(_packsDir)) yield break;
        foreach (var packFile in Directory.EnumerateFiles(_packsDir, "*.pack"))
        {
            string packId = Path.GetFileNameWithoutExtension(packFile);
            long bytes = 0;
            try { bytes = new FileInfo(packFile).Length; } catch { }
            yield return (packId, bytes);
        }
    }

    public IEnumerable<string> ListPackEntryIds(string packId)
    {
        string idxFile = Path.Combine(_packsDir, $"{packId}.idx");
        if (!File.Exists(idxFile)) yield break;

        FileStream? fs = null;
        BinaryReader? br = null;
        try
        {
            fs = new FileStream(idxFile, FileMode.Open, FileAccess.Read, FileShare.Read);
            br = new BinaryReader(fs);
            int entryCount = br.ReadInt32();
            byte[] idBytes = new byte[64];
            for (int i = 0; i < entryCount; i++)
            {
                int bytesRead = br.Read(idBytes, 0, 64);
                if (bytesRead < 64) break;
                br.ReadInt64(); // offset
                br.ReadInt32(); // length
                string entryId = System.Text.Encoding.UTF8.GetString(idBytes).TrimEnd('\0');
                if (!string.IsNullOrEmpty(entryId))
                    yield return entryId;
            }
        }
        finally
        {
            br?.Dispose();
            fs?.Dispose();
        }
    }

    public int UnpackedCount()
    {
        if (!Directory.Exists(_ccfDir)) return 0;
        return Directory.GetFiles(_ccfDir, "*.ccf").Length;
    }

    public async Task StorePackAsync(string packId, byte[] packData, byte[] indexData, CancellationToken ct = default)
    {
        await WriteFileAtomicAsync(Path.Combine(_packsDir, $"{packId}.pack"), packData, ct);
        await WriteFileAtomicAsync(Path.Combine(_packsDir, $"{packId}.idx"), indexData, ct);
        Console.WriteLine($"[CcfStore] Stored pack {packId} ({packData.Length} bytes, {indexData.Length} byte index)");
    }

    public bool DeletePack(string packId)
    {
        string packFile = Path.Combine(_packsDir, $"{packId}.pack");
        string idxFile = Path.Combine(_packsDir, $"{packId}.idx");
        bool deleted = false;

        try
        {
            if (File.Exists(packFile))
            {
                File.Delete(packFile);
                deleted = true;
            }
            if (File.Exists(idxFile))
            {
                File.Delete(idxFile);
                deleted = true;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CcfStore] Failed deleting pack {packId}: {ex.Message}");
            return false;
        }

        if (deleted)
        {
            lock (_cacheLock)
            {
                if (_packCache.ContainsKey(packFile))
                    _packCache.Remove(packFile);
            }

            lock (_pframeCacheLock)
            {
                var keysToRemove = _pframeCache.Keys
                    .Where(k => k.StartsWith(packFile + ":", StringComparison.Ordinal))
                    .ToList();
                foreach (var key in keysToRemove)
                    _pframeCache.Remove(key);
            }

            Console.WriteLine($"[CcfStore] Deleted pack {packId}");
        }

        return true;
    }

    public async Task StoreDictAsync(byte[] dictData, CancellationToken ct = default)
    {
        string path = Path.Combine(_dictDir, "latest.dict");
        await WriteFileAtomicAsync(path, dictData, ct);
        Console.WriteLine($"[CcfStore] Stored dictionary ({dictData.Length} bytes)");
    }

    public byte[]? LoadDict()
    {
        string path = Path.Combine(_dictDir, "latest.dict");
        return File.Exists(path) ? File.ReadAllBytes(path) : null;
    }

    public (long BytesFreed, int FilesDeleted) ClearAll()
    {
        long bytesFreed = 0;
        int filesDeleted = 0;

        void ClearDir(string dir)
        {
            if (!Directory.Exists(dir)) return;
            foreach (var f in Directory.EnumerateFiles(dir))
            {
                try
                {
                    bytesFreed += new FileInfo(f).Length;
                    File.Delete(f);
                    filesDeleted++;
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[CcfStore] Failed to delete {f}: {ex.Message}");
                }
            }
        }

        ClearDir(_ccfDir);
        ClearDir(_packsDir);
        ClearDir(_dictDir);

        lock (_cacheLock) { _packCache.Clear(); }
        lock (_pframeCacheLock) { _pframeCache.Clear(); }

        Console.WriteLine($"[CcfStore] Cleared all: {filesDeleted} files, {bytesFreed / 1024.0 / 1024.0:F1} MB freed");
        return (bytesFreed, filesDeleted);
    }

    public CcfStoreStats GetStoreStats()
    {
        var stats = new CcfStoreStats();

        if (Directory.Exists(_ccfDir))
        {
            foreach (var f in Directory.EnumerateFiles(_ccfDir, "*.ccf"))
            {
                stats.UnpackedCount++;
                try { stats.UnpackedBytes += new FileInfo(f).Length; } catch { }
            }
        }

        if (Directory.Exists(_packsDir))
        {
            foreach (var f in Directory.EnumerateFiles(_packsDir, "*.pack"))
            {
                stats.PackCount++;
                try { stats.PackBytes += new FileInfo(f).Length; } catch { }
            }
        }

        string dictPath = Path.Combine(_dictDir, "latest.dict");
        if (File.Exists(dictPath))
        {
            stats.HasDictionary = true;
            try { stats.DictBytes = new FileInfo(dictPath).Length; } catch { }
        }

        return stats;
    }

    private string GetCcfPath(string fileId) => Path.Combine(_ccfDir, $"{fileId}.ccf");

    private static async Task WriteFileAtomicAsync(string finalPath, byte[] bytes, CancellationToken ct)
    {
        string? dir = Path.GetDirectoryName(finalPath);
        if (string.IsNullOrWhiteSpace(dir))
            throw new InvalidOperationException($"Invalid target path: {finalPath}");

        Directory.CreateDirectory(dir);
        string tempPath = Path.Combine(dir, $".{Path.GetFileName(finalPath)}.{Guid.NewGuid():N}.tmp");

        try
        {
            await using (var fs = new FileStream(
                             tempPath,
                             FileMode.CreateNew,
                             FileAccess.Write,
                             FileShare.None,
                             bufferSize: 1024 * 1024,
                             options: FileOptions.WriteThrough))
            {
                await fs.WriteAsync(bytes, ct);
                await fs.FlushAsync(ct);
            }

            File.Move(tempPath, finalPath, overwrite: true);
        }
        finally
        {
            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }

    private static readonly byte[] PackMagic = "CCP\0"u8.ToArray();

    // Cache: decompressed inner payload + pack version + ref catalog + neural models
    private readonly Dictionary<string, (byte[] Data, int Version, List<(ulong, ulong)>? Catalog, Dictionary<int, NeuralErrorCompressor.CompressResult>? NeuralModels, DateTime LastAccess)> _packCache = new();
    private readonly object _cacheLock = new();
    private const int MaxCachedPacks = 8;

    // Cache: reconstructed P-frame CCF bytes (key = packFile + ":" + fileId)
    private readonly Dictionary<string, (byte[] Ccf, DateTime LastAccess)> _pframeCache = new();
    private readonly object _pframeCacheLock = new();
    private const int MaxPframeCached = 32;

    private async Task<byte[]?> GetCcfFromPackAsync(string fileId, CancellationToken ct)
    {
        if (!Directory.Exists(_packsDir)) return null;

        foreach (var idxFile in Directory.EnumerateFiles(_packsDir, "*.idx"))
        {
            var entry = FindInPackIndex(idxFile, fileId);
            if (entry == null) continue;

            string packFile = Path.ChangeExtension(idxFile, ".pack");
            if (!File.Exists(packFile)) continue;

            var (innerPayload, packVersion, catalog, neuralModels) = await GetInnerPayload(packFile, ct);
            if (innerPayload.Length == 0) continue;

            long offset = entry.Value.Offset;
            int length = entry.Value.Length;

            if (packVersion <= 2)
            {
                // v2 index: offset points to int32 dataLength field, length = ccfDataLength
                long dataStart = offset + 4;
                if (dataStart + length > innerPayload.Length) continue;
                byte[] data = new byte[length];
                Buffer.BlockCopy(innerPayload, (int)dataStart, data, 0, length);
                return data;
            }

            // v3+: offset points to frameType byte, length = 9 + dataLength
            if (offset + 9 > innerPayload.Length) continue;
            byte frameType = innerPayload[offset];
            int baseIndex = BitConverter.ToInt32(innerPayload, (int)offset + 1);
            int dataLength = BitConverter.ToInt32(innerPayload, (int)offset + 5);
            long dataStart3 = offset + 9;

            if (dataStart3 + dataLength > innerPayload.Length) continue;

            if (frameType == 0) // I-frame
            {
                byte[] data = new byte[dataLength];
                Buffer.BlockCopy(innerPayload, (int)dataStart3, data, 0, dataLength);
                if (neuralModels != null && entry.Value.Index >= 0 &&
                    neuralModels.TryGetValue(entry.Value.Index, out var neuralModel))
                {
                    try { data = NeuralErrorCompressor.RestoreCcf(data, neuralModel, CcfPackOptimizerService.LiveDictionary); }
                    catch (Exception ex) { Console.WriteLine($"[CcfStore] Neural restore failed for {fileId}: {ex.Message}"); }
                }
                if (catalog != null)
                    data = CcfPackOptimizerService.ExpandCcfRefs(data, catalog);
                return data;
            }

            // P-frame: check cache first
            string pframeCacheKey = $"{packFile}:{fileId}";
            lock (_pframeCacheLock)
            {
                if (_pframeCache.TryGetValue(pframeCacheKey, out var cached))
                {
                    _pframeCache[pframeCacheKey] = (cached.Ccf, DateTime.UtcNow);
                    return cached.Ccf;
                }
            }

            // Need to reconstruct from base
            byte[] pframePayload = new byte[dataLength];
            Buffer.BlockCopy(innerPayload, (int)dataStart3, pframePayload, 0, dataLength);

            // Find base entry
            var baseEntry = CcfPackOptimizerService.FindEntryByPosition(idxFile, baseIndex);
            if (baseEntry == null)
            {
                Console.WriteLine($"[CcfStore] P-frame base entry {baseIndex} not found in {idxFile}");
                continue;
            }

            long baseOffset = baseEntry.Value.Offset;
            if (baseOffset + 9 > innerPayload.Length) continue;

            byte baseFrameType = innerPayload[baseOffset];
            int baseDataLen = BitConverter.ToInt32(innerPayload, (int)baseOffset + 5);
            long baseDataStart = baseOffset + 9;
            if (baseFrameType != 0 || baseDataStart + baseDataLen > innerPayload.Length)
            {
                Console.WriteLine($"[CcfStore] P-frame base is not an I-frame or out of bounds");
                continue;
            }

            byte[] baseCcf = new byte[baseDataLen];
            Buffer.BlockCopy(innerPayload, (int)baseDataStart, baseCcf, 0, baseDataLen);

            byte[]? reconstructed = CcfPackOptimizerService.ReconstructCcfFromPFrame(pframePayload, baseCcf, CcfPackOptimizerService.LiveDictionary);
            if (reconstructed == null)
            {
                Console.WriteLine($"[CcfStore] P-frame reconstruction failed for {fileId}");
                continue;
            }

            if (neuralModels != null && entry.Value.Index >= 0 &&
                neuralModels.TryGetValue(entry.Value.Index, out var neuralModelPf))
            {
                try { reconstructed = NeuralErrorCompressor.RestoreCcf(reconstructed, neuralModelPf, CcfPackOptimizerService.LiveDictionary); }
                catch (Exception ex) { Console.WriteLine($"[CcfStore] Neural restore (P-frame) failed for {fileId}: {ex.Message}"); }
            }

            if (catalog != null)
                reconstructed = CcfPackOptimizerService.ExpandCcfRefs(reconstructed, catalog);

            // Cache the expanded reconstructed CCF
            lock (_pframeCacheLock)
            {
                _pframeCache[pframeCacheKey] = (reconstructed, DateTime.UtcNow);
                if (_pframeCache.Count > MaxPframeCached)
                {
                    var oldest = _pframeCache.OrderBy(kv => kv.Value.LastAccess).First().Key;
                    _pframeCache.Remove(oldest);
                }
            }

            return reconstructed;
        }
        return null;
    }

    private async Task<(byte[] Inner, int Version, List<(ulong, ulong)>? Catalog, Dictionary<int, NeuralErrorCompressor.CompressResult>? NeuralModels)> GetInnerPayload(string packFile, CancellationToken ct)
    {
        lock (_cacheLock)
        {
            if (_packCache.TryGetValue(packFile, out var cached))
            {
                _packCache[packFile] = (cached.Data, cached.Version, cached.Catalog, cached.NeuralModels, DateTime.UtcNow);
                return (cached.Data, cached.Version, cached.Catalog, cached.NeuralModels);
            }
        }

        byte[] fileData = await File.ReadAllBytesAsync(packFile, ct);
        if (fileData.Length < 12) return (fileData, 1, null, null);

        bool isMagic = fileData[0] == PackMagic[0] && fileData[1] == PackMagic[1]
                    && fileData[2] == PackMagic[2] && fileData[3] == PackMagic[3];

        byte[] inner;
        int version = 1;
        if (isMagic)
        {
            version = BitConverter.ToInt32(fileData, 4);
            if (version >= 2)
            {
                using var decompressor = new Decompressor();
                inner = decompressor.Unwrap(fileData.AsSpan(12)).ToArray();
            }
            else
            {
                inner = fileData;
            }
        }
        else
        {
            inner = fileData;
        }

        List<(ulong, ulong)>? catalog = null;
        Dictionary<int, NeuralErrorCompressor.CompressResult>? neuralModels = null;

        if (version >= 4 && inner.Length >= 8)
        {
            try
            {
                int cPos = 0;
                cPos += 4; // entryCount
                int dictSize = BitConverter.ToInt32(inner, cPos); cPos += 4;
                cPos += dictSize;
                if (cPos + 4 <= inner.Length)
                {
                    int catalogCount = BitConverter.ToInt32(inner, cPos); cPos += 4;
                    catalog = new List<(ulong, ulong)>(catalogCount);
                    for (int i = 0; i < catalogCount && cPos + 16 <= inner.Length; i++)
                    {
                        ulong id = BitConverter.ToUInt64(inner, cPos);
                        ulong key = BitConverter.ToUInt64(inner, cPos + 8);
                        catalog.Add((id, key));
                        cPos += 16;
                    }
                }

                if (version >= 5 && cPos + 4 <= inner.Length)
                {
                    int neuralCount = BitConverter.ToInt32(inner, cPos); cPos += 4;
                    if (neuralCount > 0)
                    {
                        neuralModels = new Dictionary<int, NeuralErrorCompressor.CompressResult>(neuralCount);
                        for (int i = 0; i < neuralCount && cPos + 8 <= inner.Length; i++)
                        {
                            int entryIdx = BitConverter.ToInt32(inner, cPos); cPos += 4;
                            int serializedLen = BitConverter.ToInt32(inner, cPos); cPos += 4;
                            if (cPos + serializedLen > inner.Length) break;
                            byte[] modelBuf = new byte[serializedLen];
                            Buffer.BlockCopy(inner, cPos, modelBuf, 0, serializedLen);
                            cPos += serializedLen;
                            var model = NeuralErrorCompressor.DeserializeModel(modelBuf);
                            if (model != null) neuralModels[entryIdx] = model;
                        }
                    }
                }
            }
            catch { catalog = null; neuralModels = null; }
        }

        lock (_cacheLock)
        {
            _packCache[packFile] = (inner, version, catalog, neuralModels, DateTime.UtcNow);
            if (_packCache.Count > MaxCachedPacks)
            {
                var oldest = _packCache.OrderBy(kv => kv.Value.LastAccess).First().Key;
                _packCache.Remove(oldest);
            }
        }

        return (inner, version, catalog, neuralModels);
    }

    private bool PackIndexContains(string fileId)
    {
        if (!Directory.Exists(_packsDir)) return false;
        foreach (var idxFile in Directory.EnumerateFiles(_packsDir, "*.idx"))
        {
            if (FindInPackIndex(idxFile, fileId) != null) return true;
        }
        return false;
    }

    private static (long Offset, int Length, int Index)? FindInPackIndex(string idxPath, string fileId)
    {
        try
        {
            using var fs = new FileStream(idxPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var br = new BinaryReader(fs);
            int entryCount = br.ReadInt32();
            byte[] idBytes = new byte[64];
            for (int i = 0; i < entryCount; i++)
            {
                int bytesRead = br.Read(idBytes, 0, 64);
                if (bytesRead < 64) return null;
                long offset = br.ReadInt64();
                int length = br.ReadInt32();

                string entryId = System.Text.Encoding.UTF8.GetString(idBytes).TrimEnd('\0');
                if (string.Equals(entryId, fileId, StringComparison.OrdinalIgnoreCase))
                    return (offset, length, i);
            }
        }
        catch { }
        return null;
    }
}

public class CcfStoreStats
{
    public int UnpackedCount { get; set; }
    public long UnpackedBytes { get; set; }
    public int PackCount { get; set; }
    public long PackBytes { get; set; }
    public bool HasDictionary { get; set; }
    public long DictBytes { get; set; }
    public long TotalBytes => UnpackedBytes + PackBytes;
    public long PackRawBytes { get; set; }
    public long PackCompressedBytes { get; set; }
    public double PackCompressionRatio => PackRawBytes > 0 ? 1.0 - (double)PackCompressedBytes / PackRawBytes : 0;
}
