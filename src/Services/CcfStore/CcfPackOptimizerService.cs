using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using Cross.Utilities;
using ZstdSharp;

namespace Cross.Services.CcfStore;

/// <summary>
/// Background service that periodically packs unpacked CCFs into pack files
/// with P-frame cross-file delta compression, trains a shared zstd dictionary,
/// and removes individual CCFs after packing.
///
/// Pack format v3 (P-frame delta compression):
///   [4B] magic "CCP\0"
///   [4B] version = 3
///   [4B] decompressedSize
///   [remainder] zstd-19 compressed inner payload
///
///   Inner payload:
///     [4B] entryCount
///     [4B] dictSize (0 = no dictionary)
///     [dictSize B] dictionary
///     Per entry:
///       [1B] frameType: 0 = I-frame, 1 = P-frame (subtraction delta)
///       [4B] baseIndex: -1 for I-frame, positional index for P-frame
///       [4B] dataLength
///       [dataLength B] data (I-frame: raw CCF, P-frame: PFramePayload)
///
/// Pack format v2 (legacy, zstd-compressed):
///   [4B] magic "CCP\0"  [4B] version = 2  [4B] decompressedSize
///   [remainder] zstd-19 compressed inner payload
///   Inner: [4B] entryCount [4B] dictSize ... per entry: [4B] ccfDataLen [ccf bytes]
///
/// Index (.idx) — same structure for all versions:
///   [4B] entryCount
///   Per entry: [64B] fileId [8B] offset [4B] length
///   v2: offset → int32 dataLength field, length = ccfDataLength
///   v3: offset → frameType byte, length = 9 + dataLength
/// </summary>
public class CcfPackOptimizerService : BackgroundService
{
    private static readonly byte[] PackMagic = "CCP\0"u8.ToArray();
    private const int PackVersion = 3;
    public static long TotalPackRawBytes;
    public static long TotalPackCompressedBytes;

    public static volatile bool IsRunning;
    public static DateTime? LastRunUtc;
    public static int LastPackedCount;
    public static long LastPackSavedBytes;
    public static int TotalPackedAllTime;

    // P-frame stats
    public static int PframeGroupsFound;
    public static int PframeDeltaCount;
    public static long PframeSavedBytes;

    private const int DictTrainThreshold = 50;
    private static volatile byte[]? _liveDict;
    private static readonly object _dictLock = new();
    public static byte[]? LiveDictionary => _liveDict;

    public static void ResetStats()
    {
        TotalPackRawBytes = 0;
        TotalPackCompressedBytes = 0;
        LastPackedCount = 0;
        LastPackSavedBytes = 0;
        TotalPackedAllTime = 0;
        PframeGroupsFound = 0;
        PframeDeltaCount = 0;
        PframeSavedBytes = 0;
        LastRunUtc = null;
        lock (_dictLock) { _liveDict = null; }
        Console.WriteLine("[CcfPackOptimizer] Stats and dictionary reset");
    }

    private const double PframeSavingsThreshold = 0.85;

    internal sealed class LoadedEntry
    {
        public string FileId { get; init; } = string.Empty;
        public byte[] Ccf { get; init; } = Array.Empty<byte>();
        public CcfComponents Comp { get; init; } = new();
        public string? SourcePackId { get; init; }
    }

    private sealed class ProcessedFamily
    {
        public required LoadedEntry Base;
        public List<ProcessedMember> Members = new();
    }

    private sealed class ProcessedMember
    {
        public required LoadedEntry Entry;
        public byte[]? PframePayload;
    }

    // ───────────────────────────────────────────────────────────────────
    //  CCF component extraction (shared with CcfStoreService for reconstruction)
    // ───────────────────────────────────────────────────────────────────

    internal sealed class CcfComponents
    {
        public string Version = "";
        public byte[] Refs = Array.Empty<byte>();
        public byte[] Trim = Array.Empty<byte>();
        public byte[] Hash = Array.Empty<byte>();
        public int PatchCount;
        public int OriginalSize;
        public byte[] RawSkip = Array.Empty<byte>();
        public byte[] RawValues = Array.Empty<byte>();
        public byte[] RawError = Array.Empty<byte>();
        public bool IsSplitStream;
        public bool IsFourStream;
        public bool IsDirectPatch;
        public byte[] ModeBitfield = Array.Empty<byte>();
        public byte[] RawBitmask = Array.Empty<byte>();
        public int FourStreamChunkCount;
        public bool IsValid;
        public string RefsFingerprint = "";
        public int OriginalCcfLength;
        public byte[] DeltaSource => (IsFourStream || IsDirectPatch) ? RawValues : IsSplitStream ? RawValues : RawError;
    }

    internal static CcfComponents ParseCcfComponents(byte[] file)
    {
        var comp = new CcfComponents { OriginalCcfLength = file.Length };
        try
        {
            if (file.Length < 12) return comp;

            int pos = 0;
            int versionLen = BitConverter.ToInt32(file, pos); pos += 4;
            if (versionLen <= 0 || versionLen > 20 || pos + versionLen > file.Length) return comp;

            comp.Version = Encoding.UTF8.GetString(file, pos, versionLen); pos += versionLen;
            int refsLen = BitConverter.ToInt32(file, pos); pos += 4;

            int errorCompLen;
            bool hasOrigLen = comp.Version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0";
            errorCompLen = BitConverter.ToInt32(file, pos); pos += 4;
            if (hasOrigLen) pos += 4; // skip errorOrigLen

            int trimLen = 0;
            bool hasTrim = comp.Version is "v2.1.0" or "v3.0.0" or "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0";
            if (hasTrim) { trimLen = BitConverter.ToInt32(file, pos); pos += 4; }

            int refsOffset = pos;
            int errorOffset = refsOffset + refsLen;
            int trimOffset = errorOffset + errorCompLen;
            int hashOffset = trimOffset + (hasTrim ? trimLen : 0);

            if (errorOffset + errorCompLen > file.Length) return comp;

            comp.Refs = new byte[refsLen];
            if (refsLen > 0) Buffer.BlockCopy(file, refsOffset, comp.Refs, 0, refsLen);

            if (hasTrim && trimLen > 0 && trimOffset + trimLen <= file.Length)
            {
                comp.Trim = new byte[trimLen];
                Buffer.BlockCopy(file, trimOffset, comp.Trim, 0, trimLen);
            }

            if (hashOffset + 4 <= file.Length)
            {
                int hashContentLen = BitConverter.ToInt32(file, hashOffset);
                if (hashContentLen > 0 && hashOffset + 4 + hashContentLen <= file.Length)
                {
                    comp.Hash = new byte[hashContentLen];
                    Buffer.BlockCopy(file, hashOffset + 4, comp.Hash, 0, hashContentLen);
                }
            }

            byte[] errorPayload = new byte[errorCompLen];
            if (errorCompLen > 0) Buffer.BlockCopy(file, errorOffset, errorPayload, 0, errorCompLen);

            if (comp.Version == "v5.5.0" && errorCompLen >= 28)
            {
                comp.IsDirectPatch = true;
                comp.PatchCount = BitConverter.ToInt32(errorPayload, 0);
                comp.OriginalSize = BitConverter.ToInt32(errorPayload, 4);
                comp.FourStreamChunkCount = BitConverter.ToInt32(errorPayload, 8);
                int compModeLen = BitConverter.ToInt32(errorPayload, 12);
                int compSkipLen = BitConverter.ToInt32(errorPayload, 16);
                int compBitmaskLen = BitConverter.ToInt32(errorPayload, 20);
                int compValLen = BitConverter.ToInt32(errorPayload, 24);

                int headerSize = 28;
                if (headerSize + compModeLen + compSkipLen + compBitmaskLen + compValLen <= errorCompLen)
                {
                    var dict = _liveDict;
                    int off = headerSize;
                    try
                    {
                        using var d = new Decompressor();
                        if (dict != null) d.LoadDictionary(dict);
                        comp.ModeBitfield = d.Unwrap(errorPayload.AsSpan(off, compModeLen)).ToArray();
                        off += compModeLen;
                        comp.RawSkip = d.Unwrap(errorPayload.AsSpan(off, compSkipLen)).ToArray();
                        off += compSkipLen;
                        comp.RawBitmask = d.Unwrap(errorPayload.AsSpan(off, compBitmaskLen)).ToArray();
                        off += compBitmaskLen;
                        comp.RawValues = d.Unwrap(errorPayload.AsSpan(off, compValLen)).ToArray();
                    }
                    catch
                    {
                        using var d2 = new Decompressor();
                        off = headerSize;
                        comp.ModeBitfield = d2.Unwrap(errorPayload.AsSpan(off, compModeLen)).ToArray();
                        off += compModeLen;
                        comp.RawSkip = d2.Unwrap(errorPayload.AsSpan(off, compSkipLen)).ToArray();
                        off += compSkipLen;
                        comp.RawBitmask = d2.Unwrap(errorPayload.AsSpan(off, compBitmaskLen)).ToArray();
                        off += compBitmaskLen;
                        comp.RawValues = d2.Unwrap(errorPayload.AsSpan(off, compValLen)).ToArray();
                    }
                    comp.IsValid = true;
                }
            }
            else if (comp.Version == "v5.4.0" && errorCompLen >= 28)
            {
                comp.IsFourStream = true;
                comp.PatchCount = BitConverter.ToInt32(errorPayload, 0);
                comp.OriginalSize = BitConverter.ToInt32(errorPayload, 4);
                comp.FourStreamChunkCount = BitConverter.ToInt32(errorPayload, 8);
                int compModeLen = BitConverter.ToInt32(errorPayload, 12);
                int compSkipLen = BitConverter.ToInt32(errorPayload, 16);
                int compBitmaskLen = BitConverter.ToInt32(errorPayload, 20);
                int compValLen = BitConverter.ToInt32(errorPayload, 24);

                int headerSize = 28;
                if (headerSize + compModeLen + compSkipLen + compBitmaskLen + compValLen <= errorCompLen)
                {
                    var dict = _liveDict;
                    int off = headerSize;
                    try
                    {
                        using var d = new Decompressor();
                        if (dict != null) d.LoadDictionary(dict);
                        comp.ModeBitfield = d.Unwrap(errorPayload.AsSpan(off, compModeLen)).ToArray();
                        off += compModeLen;
                        comp.RawSkip = d.Unwrap(errorPayload.AsSpan(off, compSkipLen)).ToArray();
                        off += compSkipLen;
                        comp.RawBitmask = d.Unwrap(errorPayload.AsSpan(off, compBitmaskLen)).ToArray();
                        off += compBitmaskLen;
                        comp.RawValues = d.Unwrap(errorPayload.AsSpan(off, compValLen)).ToArray();
                    }
                    catch
                    {
                        using var d2 = new Decompressor();
                        off = headerSize;
                        comp.ModeBitfield = d2.Unwrap(errorPayload.AsSpan(off, compModeLen)).ToArray();
                        off += compModeLen;
                        comp.RawSkip = d2.Unwrap(errorPayload.AsSpan(off, compSkipLen)).ToArray();
                        off += compSkipLen;
                        comp.RawBitmask = d2.Unwrap(errorPayload.AsSpan(off, compBitmaskLen)).ToArray();
                        off += compBitmaskLen;
                        comp.RawValues = d2.Unwrap(errorPayload.AsSpan(off, compValLen)).ToArray();
                    }
                    comp.IsValid = true;
                }
            }
            else if (comp.Version == "v5.3.0" && errorCompLen >= 16)
            {
                comp.IsSplitStream = true;
                comp.PatchCount = BitConverter.ToInt32(errorPayload, 0);
                comp.OriginalSize = BitConverter.ToInt32(errorPayload, 4);
                int compSkipLen = BitConverter.ToInt32(errorPayload, 8);
                int compValLen = BitConverter.ToInt32(errorPayload, 12);

                if (16 + compSkipLen + compValLen <= errorCompLen)
                {
                    var dict = _liveDict;
                    try
                    {
                        using var d = new Decompressor();
                        if (dict != null) d.LoadDictionary(dict);
                        comp.RawSkip = d.Unwrap(errorPayload.AsSpan(16, compSkipLen)).ToArray();
                        comp.RawValues = d.Unwrap(errorPayload.AsSpan(16 + compSkipLen, compValLen)).ToArray();
                    }
                    catch
                    {
                        using var d2 = new Decompressor();
                        comp.RawSkip = d2.Unwrap(errorPayload.AsSpan(16, compSkipLen)).ToArray();
                        comp.RawValues = d2.Unwrap(errorPayload.AsSpan(16 + compSkipLen, compValLen)).ToArray();
                    }
                    comp.IsValid = true;
                }
            }
            else if (comp.Version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0")
            {
                using var d = new Decompressor();
                comp.RawError = d.Unwrap(errorPayload).ToArray();
                comp.OriginalSize = comp.RawError.Length;
                comp.IsValid = true;
            }
            else if (comp.Version is "v2.0.0" or "v2.1.0" or "v3.0.0")
            {
                comp.RawError = errorPayload;
                comp.OriginalSize = errorPayload.Length;
                comp.IsValid = true;
            }

            comp.RefsFingerprint = Convert.ToHexString(SHA256.HashData(comp.Refs)).ToLowerInvariant();
        }
        catch { /* leave IsValid = false */ }
        return comp;
    }

    // ───────────────────────────────────────────────────────────────────
    //  P-frame payload building & reconstruction
    // ───────────────────────────────────────────────────────────────────

    internal static byte[] BuildPFramePayload(CcfComponents comp, byte[] compressedDelta)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        bw.Write(comp.OriginalCcfLength);
        byte errorFormat = comp.IsDirectPatch ? (byte)3 : comp.IsFourStream ? (byte)2 : comp.IsSplitStream ? (byte)0 : (byte)1;
        bw.Write(errorFormat);
        bw.Write(comp.PatchCount);
        bw.Write(comp.OriginalSize);

        if (comp.IsDirectPatch || comp.IsFourStream)
        {
            bw.Write(comp.FourStreamChunkCount);
            using var c = new Compressor(19);
            byte[] portableMode = c.Wrap(comp.ModeBitfield).ToArray();
            byte[] portableSkip = c.Wrap(comp.RawSkip).ToArray();
            byte[] portableBitmask = c.Wrap(comp.RawBitmask).ToArray();
            bw.Write(portableMode.Length);
            bw.Write(portableMode);
            bw.Write(portableSkip.Length);
            bw.Write(portableSkip);
            bw.Write(portableBitmask.Length);
            bw.Write(portableBitmask);
        }
        else if (comp.IsSplitStream)
        {
            using var c = new Compressor(19);
            byte[] portableSkip = c.Wrap(comp.RawSkip).ToArray();
            bw.Write(portableSkip.Length);
            bw.Write(portableSkip);
        }
        else
        {
            bw.Write(0); // no skip stream for single-blob
        }

        bw.Write(compressedDelta.Length);
        bw.Write(compressedDelta);

        var versionBytes = Encoding.UTF8.GetBytes(comp.Version);
        bw.Write(versionBytes.Length);
        bw.Write(versionBytes);
        bw.Write(comp.Refs.Length);
        if (comp.Refs.Length > 0) bw.Write(comp.Refs);
        bw.Write(comp.Trim.Length);
        if (comp.Trim.Length > 0) bw.Write(comp.Trim);
        bw.Write(comp.Hash.Length);
        if (comp.Hash.Length > 0) bw.Write(comp.Hash);

        bw.Flush();
        return ms.ToArray();
    }

    /// <summary>
    /// Reconstruct the original CCF from a P-frame payload and the base I-frame CCF bytes.
    /// Returns null on failure.
    /// </summary>
    internal static byte[]? ReconstructCcfFromPFrame(byte[] pframeData, byte[] baseCcf)
    {
        try
        {
            using var ms = new MemoryStream(pframeData);
            using var br = new BinaryReader(ms, Encoding.UTF8);

            int _origLen = br.ReadInt32();
            byte errorFormat = br.ReadByte();
            int patchCount = br.ReadInt32();
            int originalSize = br.ReadInt32();

            int fourStreamChunkCount = 0;
            byte[] compMode = Array.Empty<byte>();
            byte[] compSkip = Array.Empty<byte>();
            byte[] compBitmask = Array.Empty<byte>();

            if (errorFormat == 3 || errorFormat == 2) // v5.5.0 direct / v5.4.0 four-stream
            {
                fourStreamChunkCount = br.ReadInt32();
                int compModeLen = br.ReadInt32();
                compMode = compModeLen > 0 ? br.ReadBytes(compModeLen) : Array.Empty<byte>();
                int compSkipLen = br.ReadInt32();
                compSkip = compSkipLen > 0 ? br.ReadBytes(compSkipLen) : Array.Empty<byte>();
                int compBitmaskLen = br.ReadInt32();
                compBitmask = compBitmaskLen > 0 ? br.ReadBytes(compBitmaskLen) : Array.Empty<byte>();
            }
            else if (errorFormat == 0) // v5.3.0 split-stream
            {
                int compSkipLen = br.ReadInt32();
                compSkip = compSkipLen > 0 ? br.ReadBytes(compSkipLen) : Array.Empty<byte>();
            }
            else // single-blob
            {
                int compSkipLen = br.ReadInt32();
                compSkip = compSkipLen > 0 ? br.ReadBytes(compSkipLen) : Array.Empty<byte>();
            }

            int compDeltaLen = br.ReadInt32();
            byte[] compDelta = br.ReadBytes(compDeltaLen);

            int versionLen = br.ReadInt32();
            string version = Encoding.UTF8.GetString(br.ReadBytes(versionLen));
            int refsLen = br.ReadInt32();
            byte[] refs = refsLen > 0 ? br.ReadBytes(refsLen) : Array.Empty<byte>();
            int trimLen = br.ReadInt32();
            byte[] trim = trimLen > 0 ? br.ReadBytes(trimLen) : Array.Empty<byte>();
            int hashLen = br.ReadInt32();
            byte[] hash = hashLen > 0 ? br.ReadBytes(hashLen) : Array.Empty<byte>();

            using var decomp = new Decompressor();
            byte[] rawDelta = decomp.Unwrap(compDelta).ToArray();

            var baseComp = ParseCcfComponents(baseCcf);
            if (!baseComp.IsValid) return null;
            byte[] baseData = baseComp.DeltaSource;

            byte[] originalData = new byte[rawDelta.Length];
            for (int i = 0; i < rawDelta.Length; i++)
            {
                byte b = i < baseData.Length ? baseData[i] : (byte)0;
                originalData[i] = (byte)((rawDelta[i] + b) & 0xFF);
            }

            byte[] errorPayload;
            int errorOriginalLength;

            if (errorFormat == 3 || errorFormat == 2) // v5.5.0 direct / v5.4.0 four-stream
            {
                using var dMode = new Decompressor();
                byte[] rawMode = compMode.Length > 0 ? dMode.Unwrap(compMode).ToArray() : Array.Empty<byte>();
                byte[] rawSkip = compSkip.Length > 0 ? dMode.Unwrap(compSkip).ToArray() : Array.Empty<byte>();
                byte[] rawBitmask = compBitmask.Length > 0 ? dMode.Unwrap(compBitmask).ToArray() : Array.Empty<byte>();

                using var c = new Compressor(3);
                byte[] zMode = c.Wrap(rawMode).ToArray();
                byte[] zSkip = c.Wrap(rawSkip).ToArray();
                byte[] zBitmask = c.Wrap(rawBitmask).ToArray();
                byte[] zVals = c.Wrap(originalData).ToArray();

                using var errMs = new MemoryStream();
                using var errBw = new BinaryWriter(errMs, Encoding.UTF8, leaveOpen: true);
                errBw.Write(patchCount);
                errBw.Write(originalSize);
                errBw.Write(fourStreamChunkCount);
                errBw.Write(zMode.Length);
                errBw.Write(zSkip.Length);
                errBw.Write(zBitmask.Length);
                errBw.Write(zVals.Length);
                errBw.Write(zMode);
                errBw.Write(zSkip);
                errBw.Write(zBitmask);
                errBw.Write(zVals);
                errBw.Flush();
                errorPayload = errMs.ToArray();
                errorOriginalLength = errorPayload.Length;
            }
            else if (errorFormat == 0) // v5.3.0 split-stream
            {
                byte[] rawSkip;
                if (compSkip.Length > 0)
                {
                    using var dSkip = new Decompressor();
                    rawSkip = dSkip.Unwrap(compSkip).ToArray();
                }
                else
                {
                    rawSkip = Array.Empty<byte>();
                }

                using var c = new Compressor(3);
                byte[] zSkip = c.Wrap(rawSkip).ToArray();
                byte[] zVals = c.Wrap(originalData).ToArray();

                using var errMs = new MemoryStream();
                using var errBw = new BinaryWriter(errMs, Encoding.UTF8, leaveOpen: true);
                errBw.Write(patchCount);
                errBw.Write(originalSize);
                errBw.Write(zSkip.Length);
                errBw.Write(zVals.Length);
                errBw.Write(zSkip);
                errBw.Write(zVals);
                errBw.Flush();
                errorPayload = errMs.ToArray();
                errorOriginalLength = errorPayload.Length;
            }
            else // single-blob
            {
                errorOriginalLength = originalData.Length;
                bool needsCompression = version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0";
                if (needsCompression)
                {
                    using var c = new Compressor(3);
                    errorPayload = c.Wrap(originalData).ToArray();
                }
                else
                {
                    errorPayload = originalData;
                    errorOriginalLength = errorPayload.Length;
                }
            }

            return ReassembleCcf(version, refs, errorPayload, errorOriginalLength, trim, hash);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CcfPackOptimizer] P-frame reconstruction failed: {ex.Message}");
            return null;
        }
    }

    internal static byte[] ReassembleCcf(
        string version, byte[] refs, byte[] errorPayload, int errorOriginalLength,
        byte[] trim, byte[] hash)
    {
        using var ms = new MemoryStream();
        using var bw = new BinaryWriter(ms, Encoding.UTF8, leaveOpen: true);

        var versionBytes = Encoding.UTF8.GetBytes(version);
        bw.Write(versionBytes.Length);
        bw.Write(versionBytes);
        bw.Write(refs.Length);

        if (version is "v5.3.0" or "v5.4.0" or "v5.5.0")
        {
            bw.Write(errorPayload.Length);
            bw.Write(errorPayload.Length);
        }
        else if (version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0")
        {
            bw.Write(errorPayload.Length);
            bw.Write(errorOriginalLength);
        }
        else
        {
            bw.Write(errorPayload.Length);
        }

        if (version is "v2.1.0" or "v3.0.0" or "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0")
            bw.Write(trim.Length);

        bw.Write(refs);
        bw.Write(errorPayload);
        bw.Write(trim);
        bw.Write(hash.Length);
        bw.Write(hash);
        bw.Flush();

        return ms.ToArray();
    }

    // ───────────────────────────────────────────────────────────────────
    //  Main loop
    // ───────────────────────────────────────────────────────────────────

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!Globals.EnableCcfStore)
        {
            Console.WriteLine("[CcfPackOptimizer] Disabled (ENABLE_CCF_STORE != true)");
            return;
        }

        var existingDict = CcfStoreService.Instance.LoadDict();
        if (existingDict != null)
        {
            lock (_dictLock) { _liveDict = existingDict; }
            Console.WriteLine($"[CcfPackOptimizer] Loaded existing dictionary ({existingDict.Length} bytes)");
        }

        Console.WriteLine($"[CcfPackOptimizer] Started. PackThreshold={Globals.CcfPackThreshold}, DictThreshold={DictTrainThreshold}, Interval={Globals.CcfPackIntervalSec}s, PackVersion={PackVersion}");

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                await Task.Delay(TimeSpan.FromSeconds(Globals.CcfPackIntervalSec), stoppingToken);
                await RunOptimizationCycle(stoppingToken);
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[CcfPackOptimizer] Error in optimization cycle: {ex.Message}");
                await Task.Delay(TimeSpan.FromSeconds(30), stoppingToken);
            }
        }
    }

    private async Task RunOptimizationCycle(CancellationToken ct)
    {
        if (!await Globals.CcfOptimizationLock.WaitAsync(TimeSpan.FromSeconds(5), ct))
        {
            Console.WriteLine("[CcfPackOptimizer] Skipping cycle, consolidation service holds lock");
            return;
        }

        try
        {
            IsRunning = true;
            await RunOptimizationCycleInner(ct);
        }
        finally
        {
            IsRunning = false;
            LastRunUtc = DateTime.UtcNow;
            Globals.CcfOptimizationLock.Release();
        }
    }

    private async Task RunOptimizationCycleInner(CancellationToken ct)
    {
        var store = CcfStoreService.Instance;
        var unpackedIds = store.ListUnpackedCcfs().ToList();

        if (_liveDict == null && unpackedIds.Count >= DictTrainThreshold)
        {
            var trainEntries = new List<(string FileId, byte[] Data)>();
            foreach (var fileId in unpackedIds)
            {
                byte[]? data = await store.GetCcfAsync(fileId, ct);
                if (data != null) trainEntries.Add((fileId, data));
                if (trainEntries.Count >= 500) break;
            }

            byte[]? dict = TrainDictionaryFromCcfs(trainEntries);
            if (dict != null)
            {
                await store.StoreDictAsync(dict, ct);
                lock (_dictLock) { _liveDict = dict; }
                Console.WriteLine($"[CcfPackOptimizer] Dictionary trained: {dict.Length} bytes from {trainEntries.Count} CCFs");
            }
        }

        var candidateIds = new List<(string FileId, string? SourcePackId)>();
        var seen = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        var selectedPacks = new Dictionary<string, List<string>>(StringComparer.OrdinalIgnoreCase);

        foreach (var id in unpackedIds)
        {
            if (seen.Add(id))
                candidateIds.Add((id, null));
        }

        if (Globals.CcfGlobalCompactionEnabled)
        {
            int maxSourcePacks = Math.Max(0, Globals.CcfCompactionMaxSourcePacksPerCycle);
            long readBudget = Math.Max(64L * 1024 * 1024, Globals.CcfCompactionMaxReadBytesPerCycle);
            long selectedPackBytes = 0;

            foreach (var (packId, packBytes) in store.ListPacks().OrderBy(p => p.PackId))
            {
                if (selectedPacks.Count >= maxSourcePacks) break;
                if (selectedPackBytes + packBytes > readBudget / 2 && selectedPacks.Count > 0) break;

                var ids = store.ListPackEntryIds(packId)
                    .Where(id => !string.IsNullOrWhiteSpace(id))
                    .Distinct(StringComparer.OrdinalIgnoreCase)
                    .ToList();

                if (ids.Count == 0) continue;

                selectedPacks[packId] = ids;
                selectedPackBytes += Math.Max(0, packBytes);
                foreach (var id in ids)
                {
                    if (seen.Add(id))
                        candidateIds.Add((id, packId));
                }
            }

            Console.WriteLine($"[CcfPackOptimizer] Global compaction enabled: sourcePacks={selectedPacks.Count}, candidates={candidateIds.Count}, readBudget={readBudget / 1024 / 1024}MB");
        }

        if (candidateIds.Count < Globals.CcfPackThreshold)
        {
            Console.WriteLine($"[CcfPackOptimizer] {candidateIds.Count} candidate CCFs (threshold={Globals.CcfPackThreshold}), dict={(LiveDictionary != null ? $"{LiveDictionary.Length}B" : "none")}");
            return;
        }

        var loadSw = System.Diagnostics.Stopwatch.StartNew();
        var loadedEntries = new List<LoadedEntry>(capacity: Math.Min(candidateIds.Count, 4096));
        long bytesRead = 0;
        long maxReadBytes = Math.Max(64L * 1024 * 1024, Globals.CcfCompactionMaxReadBytesPerCycle);
        long maxWorkingSetBytes = Globals.CcfCompactionMaxWorkingSetMb > 0
            ? (long)Globals.CcfCompactionMaxWorkingSetMb * 1024L * 1024L
            : Globals.GetDynamicMemoryBudget();
        DateTime deadline = DateTime.UtcNow.AddSeconds(Math.Max(5, Globals.CcfCompactionMaxDurationSec));
        var loadedByPack = selectedPacks.Keys.ToDictionary(k => k, _ => 0, StringComparer.OrdinalIgnoreCase);

        foreach (var (fileId, sourcePackId) in candidateIds)
        {
            ct.ThrowIfCancellationRequested();
            if (DateTime.UtcNow >= deadline && loadedEntries.Count >= 2)
                break;

            if (bytesRead >= maxReadBytes && loadedEntries.Count >= 2)
                break;

            if (Environment.WorkingSet > maxWorkingSetBytes && loadedEntries.Count >= 2)
            {
                Console.WriteLine($"[CcfPackOptimizer] Memory guard hit ({Environment.WorkingSet / 1024 / 1024}MB >= {maxWorkingSetBytes / 1024 / 1024}MB), ending load phase");
                break;
            }

            byte[]? data = await store.GetCcfAsync(fileId, ct);
            if (data == null) continue;

            bytesRead += data.Length;
            var comp = ParseCcfComponents(data);
            loadedEntries.Add(new LoadedEntry
            {
                FileId = fileId,
                Ccf = data,
                Comp = comp,
                SourcePackId = sourcePackId
            });

            if (sourcePackId != null && loadedByPack.ContainsKey(sourcePackId))
                loadedByPack[sourcePackId]++;
        }

        loadSw.Stop();
        if (loadedEntries.Count < 2)
        {
            Console.WriteLine($"[CcfPackOptimizer] Not enough readable CCFs to pack (loaded={loadedEntries.Count}, read={bytesRead / 1024}KB)");
            return;
        }

        Console.WriteLine($"[CcfPackOptimizer] Packing loaded set: entries={loadedEntries.Count}, read={bytesRead / 1024 / 1024}MB, memBudget={maxWorkingSetBytes / 1024 / 1024}MB, loadTime={loadSw.ElapsedMilliseconds}ms");

        var packResult = await PackEntriesAsync(store, loadedEntries, ct);

        // Delete source unpacked files that were compacted.
        foreach (var entry in loadedEntries)
        {
            if (entry.SourcePackId == null)
                store.DeleteCcf(entry.FileId);
        }

        // Delete source packs only when all of their indexed entries were compacted this cycle.
        foreach (var (packId, ids) in selectedPacks)
        {
            int loaded = loadedByPack.TryGetValue(packId, out var n) ? n : 0;
            if (loaded == ids.Count && ids.Count > 0)
            {
                if (!store.DeletePack(packId))
                    Console.WriteLine($"[CcfPackOptimizer] Failed deleting migrated source pack {packId}");
            }
            else
            {
                Console.WriteLine($"[CcfPackOptimizer] Kept source pack {packId} (loaded {loaded}/{ids.Count} entries this cycle)");
            }
        }

        LastPackedCount = packResult.EntryCount;
        LastPackSavedBytes = packResult.TotalRawBytes - packResult.PackBytes;
        TotalPackedAllTime += packResult.EntryCount;
        TotalPackRawBytes += packResult.InnerBytes;
        TotalPackCompressedBytes += packResult.CompressedBytes;
        PframeGroupsFound += packResult.PframeGroups;
        PframeDeltaCount += packResult.PframeDeltaCount;
        PframeSavedBytes += packResult.PframeSavedBytes;

        double compressionRatio = packResult.InnerBytes > 0
            ? 1.0 - (double)packResult.CompressedBytes / packResult.InnerBytes
            : 0;

        Console.WriteLine($"[CcfPackOptimizer] Packed {packResult.EntryCount} CCFs into {packResult.PackId}: " +
            $"raw={packResult.TotalRawBytes / 1024.0:F1}KB → inner={packResult.InnerBytes / 1024.0:F1}KB → " +
            $"zstd={packResult.CompressedBytes / 1024.0:F1}KB ({compressionRatio * 100:F1}% pack compression), " +
            $"P-frame: {packResult.PframeDeltaCount} deltas in {packResult.PframeGroups} families (saved {packResult.PframeSavedBytes / 1024.0:F1}KB inner)");
    }

    internal sealed class PackBuildResult
    {
        public string PackId { get; init; } = string.Empty;
        public int EntryCount { get; init; }
        public long TotalRawBytes { get; init; }
        public int PframeGroups { get; init; }
        public int PframeDeltaCount { get; init; }
        public long PframeSavedBytes { get; init; }
        public long InnerBytes { get; init; }
        public long CompressedBytes { get; init; }
        public long PackBytes { get; init; }
    }

    internal static async Task<PackBuildResult> PackEntriesAsync(CcfStoreService store, List<LoadedEntry> entries, CancellationToken ct)
    {
        long totalRawBytes = 0;
        foreach (var entry in entries)
            totalRawBytes += entry.Ccf.Length;

        var groups = entries
            .Where(e => e.Comp.IsValid && e.Comp.RefsFingerprint.Length > 0)
            .GroupBy(e => e.Comp.RefsFingerprint)
            .ToList();

        var ungrouped = entries.Where(e => !e.Comp.IsValid || e.Comp.RefsFingerprint.Length == 0).ToList();

        int pframeGroups = groups.Count(g => g.Count() > 1);

        // ── Phase 1: Parallel P-frame delta computation per family ──
        int parallelism = Globals.CcfOptimizerParallelism > 0
            ? Globals.CcfOptimizerParallelism
            : Math.Max(1, Math.Min(Environment.ProcessorCount / 2, 8));

        var multiGroups = groups.Where(g => g.Count() > 1).ToList();
        var familyResults = new ConcurrentBag<ProcessedFamily>();

        if (multiGroups.Count > 0)
        {
            Console.WriteLine($"[CcfPackOptimizer] Parallel P-frame: {multiGroups.Count} families, parallelism={parallelism}");

            Parallel.ForEach(multiGroups, new ParallelOptions
            {
                MaxDegreeOfParallelism = parallelism,
                CancellationToken = ct
            }, group =>
            {
                var members = group.ToList();
                var family = new ProcessedFamily { Base = members[0] };
                var baseMember = members[0];

                for (int m = 1; m < members.Count; m++)
                {
                    var member = members[m];
                    var pm = new ProcessedMember { Entry = member };

                    if (member.Comp.IsValid && baseMember.Comp.IsValid)
                    {
                        try
                        {
                            byte[] memberSrc = member.Comp.DeltaSource;
                            byte[] baseSrc = baseMember.Comp.DeltaSource;
                            byte[] delta = new byte[memberSrc.Length];
                            int limit = Math.Min(memberSrc.Length, baseSrc.Length);
                            for (int i = 0; i < limit; i++)
                                delta[i] = (byte)((memberSrc[i] - baseSrc[i]) & 0xFF);
                            for (int i = limit; i < memberSrc.Length; i++)
                                delta[i] = memberSrc[i];

                            using var zc = new Compressor(19);
                            byte[] compDelta = zc.Wrap(delta).ToArray();
                            byte[] pframePayload = BuildPFramePayload(member.Comp, compDelta);

                            if (pframePayload.Length < member.Ccf.Length * PframeSavingsThreshold)
                            {
                                byte[]? reconstructed = ReconstructCcfFromPFrame(pframePayload, baseMember.Ccf);
                                if (reconstructed != null)
                                {
                                    var reconComp = ParseCcfComponents(reconstructed);
                                    if (reconComp.IsValid &&
                                        reconComp.DeltaSource.Length == memberSrc.Length &&
                                        reconComp.DeltaSource.AsSpan().SequenceEqual(memberSrc))
                                    {
                                        pm.PframePayload = pframePayload;
                                    }
                                }
                            }
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[CcfPackOptimizer] P-frame failed for {member.FileId}: {ex.Message}");
                        }
                    }

                    family.Members.Add(pm);
                }

                familyResults.Add(family);
            });
        }

        // ── Phase 2: Sequential assembly into pack format ──
        string packId = $"pack-{DateTime.UtcNow:yyyyMMdd-HHmmss}-{Guid.NewGuid().ToString("N")[..8]}";
        using var innerMs = new MemoryStream();
        using var idxMs = new MemoryStream();

        byte[]? currentDict = LiveDictionary;
        WriteInt32(innerMs, entries.Count);
        WriteInt32(innerMs, currentDict?.Length ?? 0);
        if (currentDict != null) innerMs.Write(currentDict);

        WriteInt32(idxMs, entries.Count);
        int entryIndex = 0;
        int cyclePframeDelta = 0;
        long cyclePframeSaved = 0;

        void WriteIFrame(string fileId, byte[] ccfBytes)
        {
            long offset = innerMs.Position;
            innerMs.WriteByte(0);
            WriteInt32(innerMs, -1);
            WriteInt32(innerMs, ccfBytes.Length);
            innerMs.Write(ccfBytes);
            int entrySize = 1 + 4 + 4 + ccfBytes.Length;
            WriteIndexEntry(idxMs, fileId, offset, entrySize);
            entryIndex++;
        }

        void WritePFrame(string fileId, byte[] pframePayload, int baseIdx)
        {
            long offset = innerMs.Position;
            innerMs.WriteByte(1);
            WriteInt32(innerMs, baseIdx);
            WriteInt32(innerMs, pframePayload.Length);
            innerMs.Write(pframePayload);
            int entrySize = 1 + 4 + 4 + pframePayload.Length;
            WriteIndexEntry(idxMs, fileId, offset, entrySize);
            entryIndex++;
        }

        foreach (var group in groups)
        {
            if (group.Count() == 1)
                WriteIFrame(group.First().FileId, group.First().Ccf);
        }

        foreach (var family in familyResults)
        {
            WriteIFrame(family.Base.FileId, family.Base.Ccf);
            int baseEntryIndex = entryIndex - 1;

            foreach (var pm in family.Members)
            {
                if (pm.PframePayload != null)
                {
                    WritePFrame(pm.Entry.FileId, pm.PframePayload, baseEntryIndex);
                    cyclePframeDelta++;
                    cyclePframeSaved += pm.Entry.Ccf.Length - pm.PframePayload.Length;
                }
                else
                {
                    WriteIFrame(pm.Entry.FileId, pm.Entry.Ccf);
                }
            }
        }

        foreach (var entry in ungrouped)
            WriteIFrame(entry.FileId, entry.Ccf);

        byte[] innerPayload = innerMs.ToArray();
        using var compressor = new Compressor(19);
        byte[] compressedPayload = compressor.Wrap(innerPayload).ToArray();

        using var packMs = new MemoryStream();
        packMs.Write(PackMagic);
        WriteInt32(packMs, PackVersion);
        WriteInt32(packMs, innerPayload.Length);
        packMs.Write(compressedPayload);

        byte[] packData = packMs.ToArray();
        byte[] indexData = idxMs.ToArray();
        await store.StorePackAsync(packId, packData, indexData, ct);

        return new PackBuildResult
        {
            PackId = packId,
            EntryCount = entries.Count,
            TotalRawBytes = totalRawBytes,
            PframeGroups = pframeGroups,
            PframeDeltaCount = cyclePframeDelta,
            PframeSavedBytes = cyclePframeSaved,
            InnerBytes = innerPayload.Length,
            CompressedBytes = compressedPayload.Length,
            PackBytes = packData.Length
        };
    }

    private static byte[]? TrainDictionaryFromCcfs(List<(string FileId, byte[] Data)> ccfEntries)
    {
        var samples = new List<byte[]>();
        foreach (var (_, ccfBytes) in ccfEntries)
        {
            byte[]? errorStream = ExtractErrorStream(ccfBytes);
            if (errorStream != null && errorStream.Length > 0)
                samples.Add(errorStream);
        }

        if (samples.Count < 10) return null;

        try
        {
            return DictBuilder.TrainFromBuffer(samples, 64 * 1024);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[CcfPackOptimizer] Dictionary training failed: {ex.Message}");
            return null;
        }
    }

    private static byte[]? ExtractErrorStream(byte[] file)
    {
        try
        {
            if (file.Length < 12) return null;

            int pos = 0;
            int versionLen = BitConverter.ToInt32(file, pos); pos += 4;
            if (versionLen <= 0 || versionLen > 20 || pos + versionLen > file.Length) return null;

            string version = Encoding.UTF8.GetString(file, pos, versionLen); pos += versionLen;
            int refsLen = BitConverter.ToInt32(file, pos); pos += 4;

            int errorCompLen;
            if (version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0")
            {
                errorCompLen = BitConverter.ToInt32(file, pos); pos += 4;
                pos += 4;
            }
            else
            {
                errorCompLen = BitConverter.ToInt32(file, pos); pos += 4;
            }

            if (version is "v2.1.0" or "v3.0.0" or "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0")
                pos += 4;

            int errorOffset = pos + refsLen;
            if (errorCompLen <= 0 || errorOffset + errorCompLen > file.Length) return null;

            byte[] errorPayload = new byte[errorCompLen];
            Buffer.BlockCopy(file, errorOffset, errorPayload, 0, errorCompLen);

            if ((version is "v5.5.0" or "v5.4.0") && errorCompLen >= 28)
            {
                int compModeLen = BitConverter.ToInt32(errorPayload, 12);
                int compSkipLen = BitConverter.ToInt32(errorPayload, 16);
                int compBitmaskLen = BitConverter.ToInt32(errorPayload, 20);
                int compValLen = BitConverter.ToInt32(errorPayload, 24);
                int valOffset = 28 + compModeLen + compSkipLen + compBitmaskLen;
                if (valOffset + compValLen <= errorCompLen)
                {
                    using var decomp = new Decompressor();
                    return decomp.Unwrap(errorPayload.AsSpan(valOffset, compValLen)).ToArray();
                }
            }
            else if (version == "v5.3.0" && errorCompLen >= 16)
            {
                int compSkipLen = BitConverter.ToInt32(errorPayload, 8);
                int compValLen = BitConverter.ToInt32(errorPayload, 12);
                if (16 + compSkipLen + compValLen <= errorCompLen)
                {
                    using var decomp = new Decompressor();
                    return decomp.Unwrap(errorPayload.AsSpan(16 + compSkipLen, compValLen)).ToArray();
                }
            }
            else if (version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0")
            {
                using var decomp = new Decompressor();
                return decomp.Unwrap(errorPayload).ToArray();
            }

            return errorPayload;
        }
        catch
        {
            return null;
        }
    }

    // ───────────────────────────────────────────────────────────────────
    //  Helpers
    // ───────────────────────────────────────────────────────────────────

    private static void WriteIndexEntry(Stream idxMs, string fileId, long offset, int entrySize)
    {
        byte[] idBytes = new byte[64];
        Encoding.UTF8.GetBytes(fileId, 0, Math.Min(fileId.Length, 64), idBytes, 0);
        idxMs.Write(idBytes);
        WriteInt64(idxMs, offset);
        WriteInt32(idxMs, entrySize);
    }

    private static void WriteInt32(Stream s, int value)
    {
        Span<byte> buf = stackalloc byte[4];
        BitConverter.TryWriteBytes(buf, value);
        s.Write(buf);
    }

    private static void WriteInt64(Stream s, long value)
    {
        Span<byte> buf = stackalloc byte[8];
        BitConverter.TryWriteBytes(buf, value);
        s.Write(buf);
    }

    /// <summary>
    /// Read the N-th entry's (offset, length) from an index file.
    /// </summary>
    internal static (long Offset, int Length)? FindEntryByPosition(string idxPath, int position)
    {
        try
        {
            using var fs = new FileStream(idxPath, FileMode.Open, FileAccess.Read, FileShare.Read);
            using var br = new BinaryReader(fs);
            int entryCount = br.ReadInt32();
            if (position < 0 || position >= entryCount) return null;
            fs.Seek(4 + (long)position * (64 + 8 + 4), SeekOrigin.Begin);
            fs.Seek(64, SeekOrigin.Current); // skip fileId
            long offset = br.ReadInt64();
            int length = br.ReadInt32();
            return (offset, length);
        }
        catch { return null; }
    }
}
