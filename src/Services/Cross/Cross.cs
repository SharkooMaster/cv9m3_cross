
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Collections.Generic;
using System.Numerics;
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
using Cross.Services.CcfStore;
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
    private static string GetEncodingVersion(string preferredEncoding = "")
        => preferredEncoding == "v6.0.0" || (string.IsNullOrEmpty(preferredEncoding) && Globals.CcfEncodingV6)
            ? "v6.0.0" : "v5.6.0";
    private static readonly string[] SupportedVersions = { "v2.0.0", "v2.1.0", "v3.0.0", "v3.1.0", "v5.0.0", "v5.1.0", "v5.2.0", "v5.3.0", "v5.4.0", "v5.5.0", "v5.6.0", "v6.0.0" };
    string headID = "";
    private readonly global::Cross.Services.Grpc.Agent.ChunkReferenceServiceClient _chunkReferenceClient = new();

    /// <summary>
    /// When set on the current async flow, <see cref="DecompressFile"/> returns
    /// the (possibly corrupted) reconstructed bytes instead of throwing on a
    /// final-stage SHA256 mismatch. Used exclusively by the in-process compress
    /// → decompress smoketest so it can compare bytes and pinpoint the failing
    /// chunk. Never read by production callers.
    /// </summary>
    private static readonly AsyncLocal<bool> _smoketestSuppressIntegrityThrow = new();

    private static int BitCount(byte b) => System.Numerics.BitOperations.PopCount((uint)b);

    private static int ParseEnvInt(string name, int defaultValue, int min)
    {
        var raw = Environment.GetEnvironmentVariable(name);
        if (int.TryParse(raw, out var v) && v >= min) return v;
        return defaultValue;
    }

    private static string TakeShort(string s, int n) =>
        string.IsNullOrEmpty(s) ? "" : (s.Length <= n ? s : s.Substring(0, n));

    private static string Sha256HexLocal(byte[] data)
    {
        Span<byte> hash = stackalloc byte[32];
        System.Security.Cryptography.SHA256.HashData(data, hash);
        var sb = new StringBuilder(64);
        for (int i = 0; i < hash.Length; i++) sb.Append(hash[i].ToString("x2"));
        return sb.ToString();
    }

    private static void WriteVarint(Stream s, uint value)
    {
        while (value >= 0x80)
        {
            s.WriteByte((byte)(value | 0x80));
            value >>= 7;
        }
        s.WriteByte((byte)value);
    }

    private static uint ReadVarint(Stream s)
    {
        uint result = 0;
        int shift = 0;
        while (true)
        {
            int b = s.ReadByte();
            if (b < 0) throw new InvalidDataException("Unexpected end of stream reading varint.");
            result |= (uint)(b & 0x7F) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift > 28) throw new InvalidDataException("Varint too large.");
        }
    }

    /// <summary>
    /// Lossless same-size nibble rearrangement: packs pairs of high nibbles into the first
    /// half and pairs of low nibbles into the second half. This separates the coarse magnitude
    /// (high nibbles cluster around 0x0/0xF for small deltas) from the fine detail (low nibbles),
    /// dramatically improving entropy coding for Laplacian-distributed error values.
    /// </summary>
    internal static byte[] NibbleSplit(byte[] input)
    {
        int n = input.Length;
        int pairs = n / 2;
        var output = new byte[n];

        for (int i = 0; i < pairs; i++)
        {
            output[i] = (byte)((input[2 * i] & 0xF0) | (input[2 * i + 1] >> 4));
            output[pairs + i] = (byte)(((input[2 * i] & 0x0F) << 4) | (input[2 * i + 1] & 0x0F));
        }

        if (n % 2 != 0)
            output[n - 1] = input[n - 1];

        return output;
    }

    internal static byte[] NibbleUnsplit(byte[] input)
    {
        int n = input.Length;
        int pairs = n / 2;
        var output = new byte[n];

        for (int i = 0; i < pairs; i++)
        {
            byte hiPacked = input[i];
            byte loPacked = input[pairs + i];
            output[2 * i] = (byte)((hiPacked & 0xF0) | (loPacked >> 4));
            output[2 * i + 1] = (byte)(((hiPacked & 0x0F) << 4) | (loPacked & 0x0F));
        }

        if (n % 2 != 0)
            output[n - 1] = input[n - 1];

        return output;
    }

    internal static (byte[] Quantized, byte[] Residual) QuantizeWithResidual(byte[] input, int threshold)
    {
        var quantized = new byte[input.Length];
        var residual = new byte[input.Length];
        for (int i = 0; i < input.Length; i++)
        {
            byte v = input[i];
            int signed = v < 128 ? v : v - 256;
            if (Math.Abs(signed) <= threshold)
            {
                quantized[i] = 0;
                residual[i] = v;
            }
            else
            {
                quantized[i] = v;
                residual[i] = 0;
            }
        }
        return (quantized, residual);
    }

    internal static byte[] MergeQuantizedResidual(byte[] quantized, byte[] residual)
    {
        var output = new byte[quantized.Length];
        for (int i = 0; i < output.Length; i++)
            output[i] = (byte)(quantized[i] | residual[i]);
        return output;
    }

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
        => SplitChunks(_bytes, _bytes?.Length ?? 0, _chunkSize);

    /// <summary>
    /// Overload with explicit logical length so pooled buffers (where
    /// _bytes.Length may exceed the actual data) split correctly.
    /// </summary>
    private (List<byte[]>, List<byte>) SplitChunks(byte[] _bytes, int _bytesLen, int _chunkSize)
    {
        List<byte[]> toRet = Misc.SplitFile(_bytes, _bytesLen, _chunkSize);
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

        byte[] errorPayload;
        if (version is "v5.3.0" or "v5.4.0" or "v5.5.0" or "v5.6.0" or "v6.0.0")
        {
            errorPayload = errorDictionary;
            bw.Write(errorPayload.Length);
            bw.Write(errorPayload.Length);
        }
        else if (version == "v3.1.0" || version == "v5.0.0" || version == "v5.1.0" || version == "v5.2.0")
        {
            int zstdLevel = version == "v5.2.0" ? 19 : version == "v5.1.0" ? 19 : version == "v5.0.0" ? 9 : 3;
            using var compressor = new Compressor(zstdLevel);
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
    ///
    /// Reads <paramref name="encodeBases"/> exclusively — the same source of truth
    /// PatchEncode consults. Encoder and decoder cannot disagree on the diff basis
    /// for any chunk: the EncodeBase variant determines both the basis bytes the
    /// encoder subtracts AND the ref byte (and therefore the decoder's recovery path).
    /// </summary>
    private static byte[] BuildV5References(ChunkEncodeBase[] encodeBases)
    {
        int chunkCount = encodeBases.Length;

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
            switch (encodeBases[i])
            {
                case ChunkEncodeBase.Mosaic m when m.Info.Donors.Count > 0:
                    foreach (var (dBucketId, dBucketKey) in m.Info.Donors)
                        GetOrAddRef(dBucketId, dBucketKey);
                    break;
                case ChunkEncodeBase.SelfFresh sf:
                    GetOrAddRef(sf.BucketId, sf.BucketKey);
                    break;
                case ChunkEncodeBase.RepFresh rf:
                    GetOrAddRef(rf.BucketId, rf.BucketKey);
                    break;
                case ChunkEncodeBase.Ref r:
                    GetOrAddRef(r.BucketId, r.BucketKey);
                    break;
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

        // Per-chunk references — one switch per chunk, exhaustive over the
        // ChunkEncodeBase variants. Any future variant must extend this switch
        // or the compiler will warn (and the runtime throws).
        for (int i = 0; i < chunkCount; i++)
        {
            switch (encodeBases[i])
            {
                case ChunkEncodeBase.Mosaic m when m.Info.Donors.Count > 0:
                    if (m.Info.IsByteMerge && m.Info.Donors.Count == 2)
                    {
                        // 0x05: byte-level pair merge — 2 donors + per-byte bitmask
                        bw.Write((byte)0x05);
                        bw.Write(GetOrAddRef(m.Info.Donors[0].BucketId, m.Info.Donors[0].BucketKey));
                        bw.Write(GetOrAddRef(m.Info.Donors[1].BucketId, m.Info.Donors[1].BucketKey));
                        bw.Write(m.Info.ByteMergeBitmask);
                    }
                    else
                    {
                        bw.Write((byte)0x04);
                        bw.Write((byte)m.Info.Donors.Count);
                        foreach (var (dBucketId, dBucketKey) in m.Info.Donors)
                            bw.Write(GetOrAddRef(dBucketId, dBucketKey));
                        bw.Write(m.Info.MatchBitmap);
                        bw.Write(m.Info.Selectors);
                        bw.Write(m.Info.DonorPositions);
                    }
                    break;

                case ChunkEncodeBase.SelfFresh sf:
                    bw.Write((byte)0x02);
                    bw.Write(GetOrAddRef(sf.BucketId, sf.BucketKey));
                    break;

                case ChunkEncodeBase.RepFresh rf:
                    bw.Write((byte)0x01);
                    bw.Write(GetOrAddRef(rf.BucketId, rf.BucketKey));
                    break;

                case ChunkEncodeBase.Ref r:
                    bw.Write((byte)0x01);
                    bw.Write(GetOrAddRef(r.BucketId, r.BucketKey));
                    break;

                case ChunkEncodeBase.Zeros:
                    bw.Write((byte)0x00);
                    break;

                case ChunkEncodeBase.Mosaic:
                    // Degenerate mosaic (0 donors) — encode as zero-ref fallback so
                    // the decoder doesn't try to consume a malformed donor table.
                    bw.Write((byte)0x00);
                    break;

                default:
                    throw new InvalidOperationException(
                        $"Unknown ChunkEncodeBase variant at chunk {i}: {encodeBases[i].GetType().Name}");
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

        // v3.1.0+: error dictionary is zstd-compressed; header has [compressedLen][originalLen]
        // v5.3.0+: sub-streams are internally compressed; both lengths are identical
        int errorOriginalLength = errorLength; // for uncompressed versions, original == stored
        if (version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0" or "v5.6.0" or "v6.0.0")
        {
            errorOriginalLength = BitConverter.ToInt32(payload.Slice(offset, IntSize));
            offset += IntSize;
        }

        // v2.1.0+: trim chunk length is stored in the header
        int trimLength = -1; // -1 means "not present" (v2.0.0 compat)
        if (version is "v2.1.0" or "v3.0.0" or "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0" or "v5.6.0" or "v6.0.0")
        {
            trimLength = BitConverter.ToInt32(payload.Slice(offset, IntSize));
            offset += IntSize;
        }

        return (version, referencesLength, errorLength, errorOriginalLength, trimLength, offset);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  v6.0.0 TRANSFORM PROGRAM ENCODER
    //  Opcodes: 0x01=CONST_FIELD, 0x02=SEQ_FIELD, 0x03=LITERAL_PATCH
    // ═══════════════════════════════════════════════════════════════════
    private const byte V6_CONST_FIELD = 0x01;
    private const byte V6_SEQ_FIELD = 0x02;
    private const byte V6_LITERAL_PATCH = 0x03;

    private static byte[] EncodeErrorsV6(
        List<(ushort Offset, byte Value)>[] chunkErrors,
        int chunkCount, int chunkSize, int patchCount, int totalBytes)
    {
        // Coverage map: track which (chunk, byte) errors are claimed by instructions
        var covered = new HashSet<long>();
        long CoverKey(int chunk, int bytePos) => ((long)chunk << 16) | (uint)bytePos;

        // Build column index: for each byte position, sorted list of (chunkIdx, value)
        var columns = new Dictionary<int, List<(int Chunk, byte Value)>>();
        for (int i = 0; i < chunkCount; i++)
        {
            foreach (var (off, val) in chunkErrors[i])
            {
                if (!columns.TryGetValue(off, out var list))
                {
                    list = new List<(int, byte)>();
                    columns[off] = list;
                }
                list.Add((i, val));
            }
        }

        // Detect patterns in each column: find maximal contiguous chunk ranges
        // with constant or arithmetic-sequence values
        var candidates = new List<(int ByteOffset, int ChunkStart, int ChunkEnd, bool IsSeq, byte ConstVal, byte Delta, int Savings)>();

        foreach (var (bytePos, entries) in columns)
        {
            if (entries.Count < 3) continue;

            int runStart = 0;
            while (runStart < entries.Count)
            {
                int runEnd = runStart;
                byte startVal = entries[runStart].Value;
                int startChunk = entries[runStart].Chunk;

                // Extend run: consecutive chunk indices with constant or sequential values
                bool isConst = true;
                byte seqDelta = 0;
                if (runStart + 1 < entries.Count && entries[runStart + 1].Chunk == startChunk + 1)
                    seqDelta = (byte)(entries[runStart + 1].Value - startVal);

                while (runEnd + 1 < entries.Count)
                {
                    int nextChunk = entries[runEnd + 1].Chunk;
                    byte nextVal = entries[runEnd + 1].Value;
                    int curChunk = entries[runEnd].Chunk;

                    if (nextChunk != curChunk + 1) break;

                    byte expectedConst = startVal;
                    byte expectedSeq = (byte)(startVal + (byte)((nextChunk - startChunk) * seqDelta));

                    if (nextVal == expectedConst) { runEnd++; continue; }
                    if (nextVal == expectedSeq) { isConst = false; runEnd++; continue; }
                    break;
                }

                int runLen = runEnd - runStart + 1;
                if (runLen >= 3)
                {
                    int chunkStart = entries[runStart].Chunk;
                    int chunkEnd = entries[runEnd].Chunk;
                    // Savings: each covered error avoids a 5-byte residual entry.
                    // Instruction cost: CONST=9, SEQ=10 (for 1-byte field)
                    int instrCost = (isConst || seqDelta == 0) ? 9 : 10;
                    int savings = runLen * 5 - instrCost;
                    if (savings > 0)
                    {
                        candidates.Add((bytePos, chunkStart, chunkEnd,
                            !isConst && seqDelta != 0, startVal, seqDelta, savings));
                    }
                }

                runStart = runEnd + 1;
            }
        }

        // Group adjacent byte positions with matching chunk ranges and compatible patterns
        candidates.Sort((a, b) =>
        {
            int c = a.ChunkStart.CompareTo(b.ChunkStart);
            if (c != 0) return c;
            c = a.ChunkEnd.CompareTo(b.ChunkEnd);
            if (c != 0) return c;
            return a.ByteOffset.CompareTo(b.ByteOffset);
        });

        // Greedy: emit instructions sorted by savings (descending)
        candidates.Sort((a, b) => b.Savings.CompareTo(a.Savings));

        using var progMs = new MemoryStream();
        using var progBw = new BinaryWriter(progMs, Encoding.UTF8, leaveOpen: true);
        int instrCount = 0;

        // Placeholder for instruction count
        long instrCountPos = progMs.Position;
        progBw.Write((ushort)0);

        foreach (var cand in candidates)
        {
            // Check if any error in this range is already covered
            bool conflict = false;
            for (int ci = cand.ChunkStart; ci <= cand.ChunkEnd && !conflict; ci++)
                if (covered.Contains(CoverKey(ci, cand.ByteOffset)))
                    conflict = true;
            if (conflict) continue;

            // Mark covered
            for (int ci = cand.ChunkStart; ci <= cand.ChunkEnd; ci++)
                covered.Add(CoverKey(ci, cand.ByteOffset));

            if (cand.IsSeq)
            {
                progBw.Write(V6_SEQ_FIELD);
                progBw.Write((ushort)cand.ChunkStart);
                progBw.Write((ushort)cand.ChunkEnd);
                progBw.Write((ushort)cand.ByteOffset);
                progBw.Write((byte)1);
                progBw.Write(cand.ConstVal);
                progBw.Write(cand.Delta);
            }
            else
            {
                progBw.Write(V6_CONST_FIELD);
                progBw.Write((ushort)cand.ChunkStart);
                progBw.Write((ushort)cand.ChunkEnd);
                progBw.Write((ushort)cand.ByteOffset);
                progBw.Write((byte)1);
                progBw.Write(cand.ConstVal);
            }
            instrCount++;
        }

        // LITERAL_PATCH: only for truly contiguous error positions (no gaps)
        // Cheaper than per-chunk bitmask when the run is short
        int bitmaskCostPerChunk = chunkSize / 8;
        for (int i = 0; i < chunkCount; i++)
        {
            if (chunkErrors[i].Count == 0) continue;
            var uncovered = chunkErrors[i]
                .Where(e => !covered.Contains(CoverKey(i, e.Offset)))
                .OrderBy(e => e.Offset)
                .ToList();
            if (uncovered.Count == 0) continue;

            int ri = 0;
            while (ri < uncovered.Count)
            {
                int runStart = ri;
                while (ri + 1 < uncovered.Count && uncovered[ri + 1].Offset == uncovered[ri].Offset + 1)
                    ri++;
                int runLen = ri - runStart + 1;
                // LITERAL_PATCH costs 7+runLen. Compare against bitmask cost for this chunk:
                // if ALL uncovered errors in the chunk are captured by LITERAL_PATCHes,
                // we avoid a full bitmask entry (bitmaskCostPerChunk + errorCount).
                // Conservative: emit only when the run itself saves vs residual bitmask overhead.
                if (runLen >= 4 && runLen <= 255 && 7 + runLen < bitmaskCostPerChunk)
                {
                    progBw.Write(V6_LITERAL_PATCH);
                    progBw.Write((ushort)i);
                    progBw.Write((ushort)uncovered[runStart].Offset);
                    progBw.Write((ushort)runLen);
                    for (int r = runStart; r <= ri; r++)
                    {
                        progBw.Write(uncovered[r].Value);
                        covered.Add(CoverKey(i, uncovered[r].Offset));
                    }
                    instrCount++;
                }
                ri++;
            }
        }

        // Write actual instruction count
        progBw.Flush();
        long savedPos = progMs.Position;
        progMs.Position = instrCountPos;
        progBw.Write((ushort)instrCount);
        progMs.Position = savedPos;
        progBw.Flush();

        byte[] rawProgram = progMs.ToArray();

        // Build residual using v5.6.0-style bitmask encoding for uncovered errors
        // Format: [modeBitfield] then per-errored-chunk [bitmask][values]
        int resBitmaskBytes = chunkSize / 8;
        byte[] resModeBitfield = new byte[(chunkCount + 7) / 8];
        using var resBitmaskMs = new MemoryStream();
        using var resValMs = new MemoryStream();
        int residualChunks = 0;
        int residualCount = 0;

        for (int i = 0; i < chunkCount; i++)
        {
            var uncov = chunkErrors[i]
                .Where(e => !covered.Contains(CoverKey(i, e.Offset)))
                .ToList();
            if (uncov.Count == 0) continue;

            resModeBitfield[i >> 3] |= (byte)(1 << (i & 7));
            residualChunks++;

            byte[] mask = new byte[resBitmaskBytes];
            foreach (var (off, _) in uncov)
                mask[off >> 3] |= (byte)(1 << (off & 7));
            resBitmaskMs.Write(mask, 0, resBitmaskBytes);

            foreach (var (_, val) in uncov)
                resValMs.WriteByte(val);

            residualCount += uncov.Count;
        }

        // Concatenate residual streams: modeBitfield + bitmasks + values
        using var resMs = new MemoryStream();
        resMs.Write(resModeBitfield, 0, resModeBitfield.Length);
        byte[] resBitmasks = resBitmaskMs.ToArray();
        byte[] resVals = resValMs.ToArray();
        resMs.Write(resBitmasks, 0, resBitmasks.Length);
        resMs.Write(resVals, 0, resVals.Length);
        byte[] rawResidual = resMs.ToArray();

        // Compress with dictionary if available
        var liveDict = Globals.EnableCcfStore ? CcfPackOptimizerService.LiveDictionary : null;
        byte[] zstdProgram, zstdResidual;
        if (liveDict != null)
        {
            using var c1 = new Compressor(19); c1.LoadDictionary(liveDict);
            zstdProgram = c1.Wrap(rawProgram).ToArray();
            using var c2 = new Compressor(19); c2.LoadDictionary(liveDict);
            zstdResidual = c2.Wrap(rawResidual).ToArray();
        }
        else
        {
            using var zc = new Compressor(19);
            zstdProgram = zc.Wrap(rawProgram).ToArray();
            zstdResidual = zc.Wrap(rawResidual).ToArray();
        }

        // Assemble final payload
        using var outMs = new MemoryStream();
        using var outBw = new BinaryWriter(outMs, Encoding.UTF8, leaveOpen: true);
        outBw.Write(patchCount);
        outBw.Write(totalBytes);
        outBw.Write(chunkCount);
        outBw.Write((ushort)chunkSize);
        outBw.Write(zstdProgram.Length);
        outBw.Write(zstdResidual.Length);
        outBw.Write(zstdProgram);
        outBw.Write(zstdResidual);
        outBw.Flush();

        int coveredErrors = covered.Count;
        Console.WriteLine($"[Compress] PatchEncode(v6.0.0): " +
            $"totalBytes={totalBytes}, patches={patchCount}, " +
            $"instructions={instrCount}, coveredByProgram={coveredErrors} ({100.0 * coveredErrors / Math.Max(1, patchCount):F1}%), " +
            $"residualChunks={residualChunks}, residualErrors={residualCount}, " +
            $"program={rawProgram.Length}→{zstdProgram.Length}, " +
            $"residual={rawResidual.Length}→{zstdResidual.Length}, " +
            $"totalPayload={outMs.Length}");

        return outMs.ToArray();
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
    public Task<(byte[] CompressedBytes, int ReferencesFound, int TotalChunks, long DatacenterBytesStored, float AverageErrorRate, long ErrorPayloadBytes)> CompressFileWithStats(byte[] _file, float maxErrorRate = 0f, string preferredEncoding = "")
        => CompressFileWithStats(_file, _file?.Length ?? 0, maxErrorRate, preferredEncoding);

    /// <summary>
    /// Compress an in-memory buffer. The overload taking an explicit
    /// <paramref name="_fileLen"/> allows callers to pass pooled
    /// <see cref="System.Buffers.ArrayPool{T}"/> buffers (whose
    /// <see cref="System.Array.Length"/> is typically larger than the
    /// requested capacity) without compressing the trailing garbage.
    /// All file-size–derived references inside use <paramref name="_fileLen"/>,
    /// never <c>_file.Length</c>.
    /// </summary>
    public async Task<(byte[] CompressedBytes, int ReferencesFound, int TotalChunks, long DatacenterBytesStored, float AverageErrorRate, long ErrorPayloadBytes)> CompressFileWithStats(byte[] _file, int _fileLen, float maxErrorRate = 0f, string preferredEncoding = "")
    {
        string encodingVersion = GetEncodingVersion(preferredEncoding);
        float bloatThreshold = maxErrorRate > 0f && maxErrorRate <= 1f
            ? maxErrorRate
            : Globals.BloatGuardThreshold;
        using var rootSpan = Observability.StartStage("CompressFileWithStats");
        // Divide into (N) chunks
        var totalSw = Stopwatch.StartNew();
        List<byte[]> fileChunks;
        List<byte> trimmedChunk;
        {
            using var stage = Observability.StartStage("SplitChunks");
            var swStage = Stopwatch.StartNew();
            (fileChunks, trimmedChunk) = SplitChunks(_file, _fileLen, Globals.chunkSize);
            swStage.Stop();
            Console.WriteLine($"[Compress] Split: {swStage.ElapsedMilliseconds}ms, {fileChunks.Count} chunks ({_fileLen} bytes)");
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
        // pipelineState owns the per-chunk encode/decode contract. The legacy
        // QueryResponseObject?[] sorted array is kept as a wire-DTO mirror and
        // is updated whenever encodeBases changes (see ChunkPipelineState.SetEncodeBase).
        // No code below this point may mutate sorted[i].(BucketId|BucketKey|StorageGuid|
        // TargetAgent|Chunk) directly — every encode-contract mutation MUST go through
        // pipelineState.SetEncodeBase to keep the wire mirror and the source of truth
        // in lock-step. That invariant is what eliminates the encoder/decoder-disagree
        // bug class (see ChunkEncodeBase.cs xmldoc).
        var pipelineState = new global::Cross.Services.Cross.ChunkPipelineState(fileChunks.Count);
        var sorted = pipelineState.Sorted;
        var encodeBases = pipelineState.EncodeBases;
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
                                    K = Globals.EnableMosaicDedup && isHighEntropy
                                        ? Math.Max(Globals.SearchTopK, Globals.MosaicTopK)
                                        : Globals.SearchTopK
                                };
                                req.Vector.AddRange(vectors[chunkIdx]);
                                req.Bitstrings.AddRange(buckets);
                                batchReq.Queries.Add(req);
                                indexMap.Add(chunkIdx);
                            }

                            // No application-level deadline: a heavily loaded but alive
                            // agent can take as long as it needs to finish its LSH search
                            // work; cross→agent fan-out is throttled per-agent (see
                            // AgentRpcThrottle) so we don't pile on more pressure while
                            // we wait. Channel keepalive (~40 s) catches truly-dead TCP.
                            BatchSearchVector_Result? batchRes = null;
                            var currentAgent = agent;
                            int attempt = 0;
                            while (batchRes == null)
                            {
                                try
                                {
                                    batchRes = await AgentRpcThrottle.RunAsync(currentAgent, async () =>
                                    {
                                        var searchClient = GrpcChannelFactory.GetClient(
                                            target: currentAgent,
                                            ctor: chan => new SearchVector.SearchVectorClient(chan),
                                            roundRobin: false, port: 5000);
                                        return await searchClient.BatchGetAsync(batchReq);
                                    });
                                }
                                catch (Exception searchEx)
                                {
                                    attempt++;
                                    if (attempt >= AgentRpcThrottle.MaxAttempts)
                                    {
                                        Console.WriteLine($"[Compress] BatchGet to {currentAgent} failed after {attempt} attempts ({searchEx.Message}); giving up");
                                        return;
                                    }
                                    var backoff = AgentRpcThrottle.BackoffFor(attempt - 1);
                                    Console.WriteLine($"[Compress] BatchGet to {currentAgent} failed (attempt {attempt}/{AgentRpcThrottle.MaxAttempts}): {searchEx.Message}; backoff {backoff.TotalSeconds}s then retry");
                                    try
                                    {
                                        await AgentHealthWatcher.Instance.WaitForAgentAsync(currentAgent, CancellationToken.None);
                                        await Task.Delay(backoff);
                                    }
                                    catch (OperationCanceledException) { return; }
                                    currentAgent = RendezvousRouter.ResolveAgentIp(currentAgent);
                                }
                            }

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
                                            pipelineState.AdoptSearchResponse(idx, new QueryResponseObject
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
                                            });
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
                        catch (OperationCanceledException) { }
                        catch (Exception ex)
                        {
                            Console.WriteLine($"[Compress] BatchGet to {agent} unexpected error: {ex.Message}");
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

        // Fill representatives that NO agent had a match for → need to store on main agent.
        // EncodeBase stays as Zeros (the default) until BatchStore runs and assigns a
        // SelfFresh / Ref via the response handler; only the pipeline flow flags
        // (NeedToStore, TargetAgent) need to be primed here so the store-groups loop
        // picks the chunk up and ships it to the right agent.
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
                // encodeBases[i] remains Zeros (set by ChunkPipelineState constructor).
            }
        }

        // Propagate representative results to non-representative group members.
        // Each non-rep shares the same agent reference as its rep — the diff encoding
        // will compute the byte-level difference against the rep's base chunk. At this
        // point BatchStore hasn't run yet, so reps are either Ref (had L1 match) or
        // Zeros (no match → fresh-store later). For Zeros reps the non-rep also stays
        // Zeros; the post-store rep propagation later folds Ref/SelfFresh/Mosaic state
        // from rep into the non-rep using the same SelfFresh→RepFresh adaptation rule.
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
            // EncodeBase mirrors rep's (which is Ref or Zeros at this point).
            encodeBases[i] = encodeBases[rep];
        }

        // Release search-phase allocations early (async state machine keeps locals alive otherwise)
        allBuckets = null!;
        agentChunkBuckets = null!;

        // ── SMART REFERENCE SELECTION + PAIR MERGE ──
        // For each chunk with Top-K candidates, find the single candidate with the fewest
        // byte-level differences. Then try all C(K,2) pairs: merge two candidates byte-by-byte
        // (picking the byte that matches the original at each position) and keep the pair
        // that produces fewer residual errors than the best single candidate.
        if (mosaicCandidates.Count > 0)
        {
            phaseSw.Restart();
            int smartUpgraded = 0;
            int pairMerged = 0;
            int totalCandidatesEvaluated = 0;
            int cs = Globals.chunkSize;
            int bitmaskLen = (cs + 7) / 8;

            Parallel.ForEach(mosaicCandidates, new ParallelOptions { MaxDegreeOfParallelism = Environment.ProcessorCount }, kv =>
            {
                int idx = kv.Key;
                var candidates = kv.Value;
                if (candidates == null || candidates.Count == 0) return;
                if (!chunkMap.TryGetValue(idx, out var original) || original == null) return;

                // Pre-convert candidate chunks to byte arrays (filter to valid-length chunks)
                var candBytes = new List<(byte[] chunk, SearchVectorObject svo)>();
                lock (candidates)
                {
                    foreach (var c in candidates)
                    {
                        if (c.Chunk != null && c.Chunk.Length >= cs)
                            candBytes.Add((c.Chunk.ToByteArray(), c));
                    }
                }
                if (candBytes.Count == 0) return;
                Interlocked.Add(ref totalCandidatesEvaluated, candBytes.Count);

                // Current best: whatever the cosine-sim search picked
                byte[]? currentBase = sorted[idx]?.Chunk != null && sorted[idx]!.Chunk.Length >= cs
                    ? sorted[idx]!.Chunk.ToByteArray()
                    : null;
                int currentDiffs = currentBase != null ? CountDiffs(original, currentBase, cs) : cs;

                // Stage 1: find the single candidate with fewest byte differences
                int bestSingleDiffs = currentDiffs;
                int bestSingleIdx = -1;
                for (int c = 0; c < candBytes.Count; c++)
                {
                    int diffs = CountDiffs(original, candBytes[c].chunk, cs);
                    if (diffs < bestSingleDiffs)
                    {
                        bestSingleDiffs = diffs;
                        bestSingleIdx = c;
                    }
                }

                // Stage 2: try all pairs, build merged chunk picking the matching byte at each position
                int bestPairDiffs = bestSingleDiffs;
                int bestPairA = -1, bestPairB = -1;
                for (int a = 0; a < candBytes.Count; a++)
                {
                    for (int b = a + 1; b < candBytes.Count; b++)
                    {
                        int remaining = 0;
                        byte[] ca = candBytes[a].chunk, cb = candBytes[b].chunk;
                        for (int j = 0; j < cs; j++)
                        {
                            if (ca[j] != original[j] && cb[j] != original[j])
                                remaining++;
                        }
                        if (remaining < bestPairDiffs)
                        {
                            bestPairDiffs = remaining;
                            bestPairA = a;
                            bestPairB = b;
                        }
                    }
                }

                // Decision: use pair merge, single upgrade, or keep current
                if (bestPairA >= 0 && bestPairDiffs < bestSingleDiffs)
                {
                    // Pair merge wins — build merged base + bitmask
                    byte[] chunkA = candBytes[bestPairA].chunk;
                    byte[] chunkB = candBytes[bestPairB].chunk;
                    byte[] merged = new byte[cs];
                    byte[] bitmask = new byte[bitmaskLen];
                    for (int j = 0; j < cs; j++)
                    {
                        if (chunkA[j] == original[j])
                        {
                            merged[j] = chunkA[j];
                        }
                        else if (chunkB[j] == original[j])
                        {
                            merged[j] = chunkB[j];
                            bitmask[j >> 3] |= (byte)(1 << (j & 7));
                        }
                        else
                        {
                            merged[j] = chunkA[j]; // neither matches, default to A
                        }
                    }

                    var svoA = candBytes[bestPairA].svo;
                    var svoB = candBytes[bestPairB].svo;
                    var info = new MosaicChunkInfo
                    {
                        IsByteMerge = true,
                        ByteMergeBitmask = bitmask,
                        StitchedBase = merged,
                        Donors = new List<(ulong BucketId, ulong BucketKey)>
                        {
                            (svoA.BucketId, (ulong)svoA.BucketKey),
                            (svoB.BucketId, (ulong)svoB.BucketKey)
                        }
                    };
                    sorted[idx]!.NeedToStore = false;
                    pipelineState.SetEncodeBase(idx, new ChunkEncodeBase.Mosaic(info));
                    Interlocked.Increment(ref pairMerged);
                }
                else if (bestSingleIdx >= 0 && bestSingleDiffs < currentDiffs)
                {
                    // Single byte-best upgrade — adopt this candidate as the Ref base.
                    var best = candBytes[bestSingleIdx].svo;
                    string keptAgent = sorted[idx]?.TargetAgent ?? "";
                    var adopted = new QueryResponseObject
                    {
                        BucketId = best.BucketId,
                        BucketKey = (ulong)best.BucketKey,
                        Similarity = best.Similarity,
                        Chunk = best.Chunk,
                        Index = idx,
                        Duplicate = true,
                        NeedToStore = false,
                        TargetAgent = keptAgent,
                        StorageGuid = best.StorageGuid ?? ""
                    };
                    pipelineState.AdoptSearchResponse(idx, adopted);
                    Interlocked.Increment(ref smartUpgraded);
                }
            });

            phaseSw.Stop();
            Console.WriteLine($"[Compress] SmartRefSelect: {phaseSw.ElapsedMilliseconds}ms, " +
                $"evaluated={totalCandidatesEvaluated} candidates across {mosaicCandidates.Count} chunks, " +
                $"singleUpgrades={smartUpgraded}, pairMerges={pairMerged}");
        }

        static int CountDiffs(byte[] a, byte[] b, int len)
        {
            int diffs = 0;
            for (int i = 0; i < len; i++)
            {
                if (a[i] != b[i]) diffs++;
            }
            return diffs;
        }

        // ── LEVEL 2 MOSAIC ASSEMBLY ──
        // For high-entropy chunks that failed Level 1 (NeedToStore=true + have mosaic candidates),
        // stitch a base chunk from donor sub-regions. The mosaic base replaces ByteString.Empty so
        // error encoding produces a smaller diff.
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
                // NOTE: NeedToStore is INTENTIONALLY not flipped here. Mosaic-L2 Assembly
                // produces a stitched base for chunks that have no L1 match (rep.BucketId == 0
                // && rep.NeedToStore == true) — the chunk still needs to be stored fresh so
                // its bytes are available for future deduplication, but the diff against the
                // stitched mosaic is written into the CCF in addition. The store-groups loop
                // picks it up because NeedToStore stays true; after BatchStore the EncodeBase
                // gets overwritten with SelfFresh, which is the desired final state.
                pipelineState.SetEncodeBase(idx, new ChunkEncodeBase.Mosaic(info));
            });

            int mosaicCount = 0;
            for (int i = 0; i < encodeBases.Length; i++)
                if (encodeBases[i] is ChunkEncodeBase.Mosaic) mosaicCount++;
            swMosaic.Stop();
            Console.WriteLine($"[Compress] Mosaic L2: {swMosaic.ElapsedMilliseconds}ms, {mosaicCount}/{mosaicCandidates.Count} chunks assembled");
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
                if (encodeBases[i] is ChunkEncodeBase.Mosaic) continue;
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
                            await AgentRpcThrottle.RunAsync(agentIp, async () =>
                            {
                                var client = GrpcChannelFactory.GetClient(
                                    target: agentIp,
                                    ctor: chan => new SearchLanes.SearchLanesClient(chan),
                                    roundRobin: false, port: 5000);

                                var req = new BatchSearchLanesReq();
                                req.Queries.AddRange(allLaneQueries);

                                var res = await client.BatchSearchAsync(req);

                                foreach (var qr in res.Results)
                                {
                                    if (qr == null || qr.Matches.Count == 0) continue;
                                    int qi = qr.QueryIndex;
                                    if (qi < 0 || qi >= queryToChunk.Count) continue;
                                    var (chunkIdx, laneIdx) = queryToChunk[qi];

                                    foreach (var m in qr.Matches)
                                        allMatches.Add((chunkIdx, laneIdx, m));
                                }
                            });
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
                        sorted[idx]!.NeedToStore = false;
                        pipelineState.SetEncodeBase(idx, new ChunkEncodeBase.Mosaic(info));
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

                            // Same contract as BatchGet above: no app deadline,
                            // throttled per-agent, bounded retries with backoff.
                            var storeAgent = agent;
                            bool stored = false;
                            int storeAttempt = 0;
                            while (!stored)
                            {
                                try
                                {
                                    var batchRes = await AgentRpcThrottle.RunAsync(storeAgent, async () =>
                                    {
                                        var storeClient = GrpcChannelFactory.GetClient(
                                            target: storeAgent,
                                            ctor: chan => new StoreVector.StoreVectorClient(chan),
                                            roundRobin: false, port: 5000);
                                        return await storeClient.BatchStoreAsync(batchReq);
                                    });

                                    int freshlyStored = 0;
                                    int dedupedAtStore = 0;
                                    int batchStoreFailures = 0;
                                    for (int j = 0; j < batchItems.Count && j < batchRes.Results.Count; j++)
                                    {
                                        var storeRes = batchRes.Results[j];

                                        // Per-item failure signal from the agent. The BatchStore
                                        // handler swallows per-item exceptions and returns
                                        // Id=0/Index=0; without this branch cross would happily
                                        // record a zero-ref in the CCF and silently corrupt the
                                        // chunk. Skip the merge and leave the existing response
                                        // intact so later passes (or a higher-level retry) can
                                        // pick it up; we also count these for diagnostics.
                                        if (storeRes.Id == 0 && storeRes.Index == 0)
                                        {
                                            batchStoreFailures++;
                                            continue;
                                        }

                                        int chunkIdx = batchItems[j].index;
                                        string storedAgent = agent;

                                        // The Mosaic-L2 Assembly path can mark a chunk
                                        // NeedToStore=true for ambient L1 source while
                                        // already holding a Mosaic EncodeBase — DO NOT
                                        // overwrite that with SelfFresh / Ref. The chunk
                                        // is encoded via stitched donors; the fresh-store
                                        // exists only so future similar chunks can find
                                        // it as an L1 candidate. Record the bucket coords
                                        // as ambient bookkeeping, leave EncodeBase alone.
                                        if (encodeBases[chunkIdx] is ChunkEncodeBase.Mosaic)
                                        {
                                            pipelineState.RecordAmbientFreshStore(
                                                chunkIdx,
                                                storeRes.Id,
                                                storeRes.Index,
                                                storeRes.StorageGuid ?? "",
                                                storedAgent);
                                            if (storeRes.WasDeduplicated)
                                            {
                                                batchItems[j].response.NeedToStore = false;
                                                batchItems[j].response.Duplicate = true;
                                                batchItems[j].response.Similarity = storeRes.Similarity;
                                                dedupedAtStore++;
                                            }
                                            else
                                            {
                                                freshlyStored++;
                                            }
                                            continue;
                                        }

                                        if (storeRes.WasDeduplicated)
                                        {
                                            // Store-time similarity dedup: the agent
                                            // matched our bytes against an existing
                                            // chunk and returned that chunk's bytes
                                            // as BaseChunk. Adopt it as our Ref base.
                                            var baseBytes = storeRes.BaseChunk ?? ByteString.Empty;
                                            pipelineState.SetEncodeBase(chunkIdx,
                                                new ChunkEncodeBase.Ref(
                                                    storeRes.Id,
                                                    storeRes.Index,
                                                    storeRes.StorageGuid ?? "",
                                                    storedAgent,
                                                    baseBytes));
                                            batchItems[j].response.NeedToStore = false;
                                            batchItems[j].response.Duplicate = true;
                                            batchItems[j].response.Similarity = storeRes.Similarity;
                                            dedupedAtStore++;
                                        }
                                        else
                                        {
                                            pipelineState.SetEncodeBase(chunkIdx,
                                                new ChunkEncodeBase.SelfFresh(
                                                    storeRes.Id,
                                                    storeRes.Index,
                                                    storeRes.StorageGuid ?? "",
                                                    storedAgent));
                                            freshlyStored++;
                                        }
                                    }
                                    if (batchStoreFailures > 0)
                                    {
                                        Console.WriteLine(
                                            $"[Compress] BatchStore to {storeAgent}: {batchStoreFailures} per-item failures (agent returned Id=0/Index=0); these rows will not be merged");
                                        // Surface in the dashboard's integrity track so it shows
                                        // up next to the round-trip mismatches.
                                        global::Cross.Services.JobEvents.JobEventBus.EmitStageDone(
                                            "IntegrityCheck:BatchStoreFailure",
                                            0,
                                            chunkCount: batchItems.Count,
                                            bucketCount: batchStoreFailures,
                                            bytes: 0);
                                    }
                                    Interlocked.Add(ref totalStored, freshlyStored);
                                    if (dedupedAtStore > 0)
                                        Console.WriteLine($"[Compress] Store-time dedup on {storeAgent}: {dedupedAtStore} chunks matched existing entries");
                                    stored = true;
                                }
                                catch (Exception storeEx)
                                {
                                    storeAttempt++;
                                    if (storeAttempt >= AgentRpcThrottle.MaxAttempts)
                                    {
                                        Console.WriteLine($"[Compress] BatchStore to {storeAgent} failed after {storeAttempt} attempts ({storeEx.Message}); giving up");
                                        return;
                                    }
                                    var backoff = AgentRpcThrottle.BackoffFor(storeAttempt - 1);
                                    Console.WriteLine($"[Compress] BatchStore to {storeAgent} failed (attempt {storeAttempt}/{AgentRpcThrottle.MaxAttempts}): {storeEx.Message}; backoff {backoff.TotalSeconds}s then retry");
                                    try
                                    {
                                        await AgentHealthWatcher.Instance.WaitForAgentAsync(storeAgent, CancellationToken.None);
                                        await Task.Delay(backoff);
                                    }
                                    catch (OperationCanceledException) { return; }
                                    storeAgent = RendezvousRouter.ResolveAgentIp(storeAgent);
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

        // ── Integrity check (1/3): verify that every storeGuid the agents
        // gave us actually equals SHA256(chunkBytesWeSent) for store-path
        // entries and SHA256(agentBaseChunk) for dedup-at-store matches.
        //
        // Split into two stages so we can tell *which* path produces any
        // mismatches:
        //   StoreRoundTrip:NewStore  — NeedToStore=true rows. Compares the
        //     bytes cross sent against the storage_guid the agent returned.
        //     A mismatch here means either (a) the agent silently dedup'd
        //     but cross didn't flip NeedToStore back to false (bookkeeping
        //     bug, benign at decompress time), or (b) the agent actually
        //     stored different bytes than what was sent (real corruption).
        //   StoreRoundTrip:DedupHit  — NeedToStore=false rows. Compares the
        //     base chunk the agent returned against its storage_guid. A
        //     mismatch here means the agent's response is internally
        //     inconsistent (would actually break decompress).
        if (global::Cross.Services.JobEvents.IntegrityDiagnostics.Enabled)
        {
            var sortedSnapshot = sorted;
            var encodeBasesSnapshot = encodeBases;
            var chunkMapSnapshot = chunkMap;
            // Mosaic chunks have a different decode contract (re-stitch from donors
            // instead of fetch-by-bucket). sha256(stored bytes) is intentionally NOT
            // equal to the agent-returned StorageGuid for mosaics — exclude them or
            // we'd flag the working mosaic feature as 12% corruption.
            int mosaicCount = 0;
            for (int i = 0; i < encodeBasesSnapshot.Length; i++)
                if (encodeBasesSnapshot[i] is ChunkEncodeBase.Mosaic) mosaicCount++;

            var swNew = Stopwatch.StartNew();
            var newStoreResult = global::Cross.Services.JobEvents.IntegrityDiagnostics.VerifyHashes(
                sortedSnapshot.Length,
                i =>
                {
                    var r = sortedSnapshot[i];
                    if (r == null || string.IsNullOrEmpty(r.StorageGuid) || !r.NeedToStore)
                        return (null, null);
                    if (chunkMapSnapshot.TryGetValue(i, out var ourChunk) && ourChunk != null && ourChunk.Length > 0)
                        return (r.StorageGuid, ourChunk);
                    return (null, null);
                });
            swNew.Stop();
            global::Cross.Services.JobEvents.IntegrityDiagnostics.Emit(
                "StoreRoundTrip:NewStore", newStoreResult, swNew.Elapsed.TotalMilliseconds);

            var swDedup = Stopwatch.StartNew();
            var dedupResult = global::Cross.Services.JobEvents.IntegrityDiagnostics.VerifyHashes(
                sortedSnapshot.Length,
                i =>
                {
                    // Only check Ref-encoded chunks — SelfFresh/RepFresh/Zeros aren't
                    // dedup-hits, Mosaic uses the donor-stitch contract (no single
                    // (B,K)→sha relationship to verify).
                    if (encodeBasesSnapshot[i] is not ChunkEncodeBase.Ref r) return (null, null);
                    if (string.IsNullOrEmpty(r.StorageGuid)) return (null, null);
                    if (r.BaseBytes.Length == 0) return (null, null);
                    return (r.StorageGuid, r.BaseBytes.ToByteArray());
                });
            swDedup.Stop();
            global::Cross.Services.JobEvents.IntegrityDiagnostics.Emit(
                "StoreRoundTrip:DedupHit", dedupResult, swDedup.Elapsed.TotalMilliseconds);

            // Surface the mosaic count for visibility — these are healthy
            // dedup rows that use the stitched-base reference type. Zero
            // mismatches expected (we don't check sha256 for them); the
            // dashboard just shows the count alongside the other stages.
            if (mosaicCount > 0)
            {
                global::Cross.Services.JobEvents.JobEventBus.EmitStageDone(
                    "IntegrityCheck:StoreRoundTrip:Mosaic",
                    0,
                    chunkCount: mosaicCount,
                    bucketCount: 0,
                    bytes: 0);
            }
        }

        // ── Post-store: propagate updated rep EncodeBase to non-reps ──
        //
        // After BatchStore, every representative has a definitive EncodeBase. Non-reps
        // need to adopt the rep's encode contract atomically — copying just one or two
        // fields out of the legacy QueryResponseObject triple (Chunk / BucketId / mosaic)
        // is exactly how the silent-corruption bug class got introduced multiple times
        // in this file's history. Going through the sum type means the encoder and the
        // decoder cannot disagree by construction.
        //
        // Adaptation rule (rep variant → non-rep variant):
        //   Zeros       → Zeros           (rep's search found nothing AND nothing
        //                                  rescued it — pathological; surfaces as 0x00
        //                                  zero-ref for both, decoder applies diff vs
        //                                  zeros for both, consistent.)
        //   SelfFresh   → RepFresh        (rep stores its OWN bytes fresh; non-rep
        //                                  must diff against rep's bytes (fileChunks[rep])
        //                                  and reference rep's bucket — that's exactly
        //                                  what the legacy DiffEncode "rep[N]-fresh"
        //                                  branch did, now made first-class.)
        //   Ref         → Ref(same)       (rep found an L1 match or got store-time
        //                                  dedup'd into an existing chunk; the L1
        //                                  match's bytes are what's stored at the
        //                                  bucket, so non-rep diffs against the same
        //                                  bytes and references the same bucket.)
        //   Mosaic      → Mosaic(same)    (rep's basis is a stitched composite of
        //                                  multiple donors; non-rep diffs against the
        //                                  same composite and the decoder re-stitches
        //                                  from the same donors. Sharing the same
        //                                  MosaicChunkInfo reference is safe — the
        //                                  struct is immutable from this point on.)
        //
        // TargetAgent propagation: SetEncodeBase projects TargetAgent from whichever
        // variant carries one (SelfFresh / RepFresh / Ref). For Mosaic the variant
        // doesn't carry one (the decoder talks to multiple agents, one per donor) so
        // sorted[i].TargetAgent stays at whatever the search phase set — which is
        // correct because no fetch-by-single-bucket happens for mosaic chunks.
        if (Globals.EnableChunkClustering)
        {
            for (int i = 0; i < sorted.Length; i++)
            {
                if (representativeSet.Contains(i)) continue;
                int rep = groupRepresentative[i];
                var repBase = encodeBases[rep];
                ChunkEncodeBase nonRepBase = repBase switch
                {
                    ChunkEncodeBase.SelfFresh sf => new ChunkEncodeBase.RepFresh(
                        sf.BucketId, sf.BucketKey, sf.StorageGuid, sf.TargetAgent, rep),
                    _ => repBase,
                };
                pipelineState.SetEncodeBase(i, nonRepBase);
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
        float averageErrorRate = 0f;
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
            //
            // CRITICAL routing rule: pick the agent via RendezvousRouter, NOT
            // sorted[idx].TargetAgent. TargetAgent was set by whichever agent
            // answered the search RPC for this chunk's *original* bitstring —
            // but after rep propagation we may have rewritten BucketId/BucketKey
            // to point at the rep's bucket, whose owning agent (per current
            // rendezvous routing) can be different. DecompressFile resolves
            // base chunks via `RendezvousRouter.PickAgent(UlongToBitstring(bId))`
            // (see line ~4299 of this file), so the encoder MUST diff against
            // the bytes THAT agent will serve. If we lazily fetch from
            // TargetAgent instead, we encode against agent A's view of (B,K)
            // and decode against agent B's view — silent corruption.
            //
            // Concrete failure mode this fixes: 62/4096 chunks per block came
            // back with case=dedup-cached, sgid (sorted[i].StorageGuid) matching
            // the decoder's RendezvousRouter-picked agent's view of (B,0), but
            // encoderBaseSha (= sha256(sorted[i].Chunk) after this lazy fetch)
            // matching a DIFFERENT chunk because the fetch hit TargetAgent and
            // got that agent's local (B,0) which had been populated by a
            // different store operation.
            if (baseChunkFetchIndices.Count > 0)
            {
                var fetchSw = Stopwatch.StartNew();
                var fetchedChunks = new byte[sorted.Length][];
                var fetchedFromAgent = new string?[sorted.Length];
                var fetchTasks = baseChunkFetchIndices.Select(async idx =>
                {
                    try
                    {
                        // Re-resolve the canonical owner agent for this bucket
                        // (post rep-propagation BucketId). This is the SAME
                        // selector DecompressFile uses, guaranteeing encode/
                        // decode agree on whose (B,K) view we're diffing against.
                        string bitstring = UlongToBitstring(sorted[idx].BucketId);
                        string canonicalAgent = RendezvousRouter.PickAgent(bitstring);
                        if (string.IsNullOrWhiteSpace(canonicalAgent))
                            canonicalAgent = sorted[idx].TargetAgent ?? "";

                        var chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(
                            sorted[idx].BucketId, sorted[idx].BucketKey,
                            targetAgent: string.IsNullOrWhiteSpace(canonicalAgent) ? null : canonicalAgent);
                        if (chunk != null && chunk.Length > 0)
                        {
                            fetchedChunks[idx] = chunk;
                            fetchedFromAgent[idx] = canonicalAgent;
                        }
                    }
                    catch { /* Will fall back to zeros */ }
                });
                await Task.WhenAll(fetchTasks);
                fetchSw.Stop();
                Observability.RecordStage("FetchBaseChunks", fetchSw.Elapsed.TotalMilliseconds,
                    ("count", baseChunkFetchIndices.Count));

                // ── Integrity check (2/3): for every base chunk we just
                // fetched, the SHA256 of the returned bytes must equal the
                // StorageGuid the agent advertised at search time.  A miss
                // here means (bucketId, bucketIndex) is pointing at different
                // bytes than what we diffed against during search — exactly
                // the failure mode where decompression deterministically
                // breaks even though every chunk "comes back". ──
                if (global::Cross.Services.JobEvents.IntegrityDiagnostics.Enabled)
                {
                    var swIntegrityFetch = Stopwatch.StartNew();
                    var sortedSnap = sorted;
                    var fetchedSnap = fetchedChunks;
                    var fetchResult = global::Cross.Services.JobEvents.IntegrityDiagnostics.VerifyHashes(
                        sortedSnap.Length,
                        i =>
                        {
                            var bytes = fetchedSnap[i];
                            if (bytes == null || bytes.Length == 0) return (null, null);
                            var r = sortedSnap[i];
                            if (r == null || string.IsNullOrEmpty(r.StorageGuid)) return (null, null);
                            return (r.StorageGuid, bytes);
                        });
                    swIntegrityFetch.Stop();
                    global::Cross.Services.JobEvents.IntegrityDiagnostics.Emit(
                        "FetchedBases", fetchResult, swIntegrityFetch.Elapsed.TotalMilliseconds);
                }

                // Inject fetched chunks into the encode contract AND re-derive the
                // (BaseBytes, StorageGuid, TargetAgent) tuple from the fetched bytes.
                // A previous version only updated Chunk, leaving StorageGuid stale
                // from the search response — so subsequent code (and the smoketest)
                // saw SHA(Chunk) ≠ StorageGuid even though both pieces individually
                // came from the cluster, just from different points in time / different
                // agents. The bytes the canonical agent JUST returned are what
                // DecompressFile will see, so that's the only source of truth that
                // matters for encode correctness: re-issue a Ref EncodeBase from
                // those bytes atomically.
                for (int i = 0; i < sorted.Length; i++)
                {
                    var fetched = fetchedChunks[i];
                    if (fetched == null) continue;
                    if (encodeBases[i] is not ChunkEncodeBase.Ref currentRef) continue;

                    string newAgent = !string.IsNullOrWhiteSpace(fetchedFromAgent[i])
                        ? fetchedFromAgent[i]!
                        : currentRef.TargetAgent;
                    pipelineState.SetEncodeBase(i, new ChunkEncodeBase.Ref(
                        currentRef.BucketId,
                        currentRef.BucketKey,
                        Convert.ToHexString(SHA256.HashData(fetched)).ToLowerInvariant(),
                        newAgent,
                        ByteString.CopyFrom(fetched)));
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
                            // No agent to store to — clear reference so zeros-diff is at least correct.
                            // Drop EncodeBase to Zeros so encoder and decoder both diff against zeros.
                            pipelineState.SetEncodeBase(idx, ChunkEncodeBase.Zeros.Instance);
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
                            await AgentRpcThrottle.RunAsync(item.agent, async () =>
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
                                var storeRes = await client.StoreAsync(storeReq);
                                // If the agent silently similarity-dedup'd the second store, the
                                // returned storage_guid is the BASE chunk's GUID — not sha256 of
                                // the bytes we just sent. Reflect that in our bookkeeping so the
                                // CCF references the right base and the integrity diagnostic
                                // doesn't fire a false positive. (See the matching logic at the
                                // primary BatchStore handler above.)
                                if (storeRes.WasDeduplicated)
                                {
                                    var baseBytes = storeRes.BaseChunk ?? ByteString.Empty;
                                    pipelineState.SetEncodeBase(item.index, new ChunkEncodeBase.Ref(
                                        storeRes.Id,
                                        storeRes.Index,
                                        storeRes.StorageGuid ?? "",
                                        item.agent,
                                        baseBytes));
                                    item.resp.NeedToStore = false;
                                    item.resp.Duplicate = true;
                                    item.resp.Similarity = storeRes.Similarity;
                                }
                                else
                                {
                                    pipelineState.SetEncodeBase(item.index, new ChunkEncodeBase.SelfFresh(
                                        storeRes.Id,
                                        storeRes.Index,
                                        storeRes.StorageGuid ?? "",
                                        item.agent));
                                    item.resp.NeedToStore = true;
                                    item.resp.Similarity = 1.0f; // base == original → empty diff
                                }
                            });
                        }
                        catch
                        {
                            // Store also failed — drop EncodeBase to Zeros (zeros-diff is correct).
                            pipelineState.SetEncodeBase(item.index, ChunkEncodeBase.Zeros.Instance);
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
            int maxAllowedRegular = (int)(Globals.chunkSize * bloatThreshold);
            int maxAllowedCluster = (int)(Globals.chunkSize * Globals.ClusterBloatGuardThreshold);

            long totalDifferingBytes = 0;
            int reusedChunkWithDiffsCount = 0;
            int[] errorRateHistogram = new int[10]; // 0-10%, 10-20%, ..., 90-100%

            // Multi-reference overlap diagnostic accumulators
            long mrefTotalErrorsBest = 0;
            long mrefTotalErrorsBoth = 0;
            long mrefTotalFixedBySecond = 0;
            int mrefChunksWithTwoCandidates = 0;

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
                // Bloat guard is only worth running when we're storing the CCF on
                // the cluster — re-storing a fresh copy then trades one extra
                // chunk-write for a smaller CCF that also lives on the cluster.
                // When CCFs are streamed back to the client (EnableCcfStore=false)
                // the user gets the bytes once and never sees them again, so the
                // extra cluster write is pure write-amplification with no payoff,
                // and crucially it adds a re-store step which is the most fragile
                // part of the compress pipeline (multiple stale-state propagation
                // paths feeding the encode case decision). Skip it.
                if (Globals.EnableCcfStore && differingByteCount > threshold)
                {
                    pipelineState.PrepareReStoreRouting(i, mainAgents[i]);
                    bloatedDiffRestore.Add(i);
                    emptyDiffCount++;
                }
                else
                {
                    if (differingByteCount == 0)
                    {
                        emptyDiffCount++;
                    }
                    else
                    {
                        diffCount++;
                        totalDifferingBytes += differingByteCount;
                        reusedChunkWithDiffsCount++;
                        int bucket = Math.Min(9, (int)(10.0 * differingByteCount / Globals.chunkSize));
                        errorRateHistogram[bucket]++;
                    }

                    // Multi-reference overlap: compare best match vs second-best candidate
                    if (mosaicCandidates.TryGetValue(i, out var candidates) && candidates.Count >= 2)
                    {
                        byte[]? secondBest = null;
                        float secondBestSim = -1f;
                        lock (candidates)
                        {
                            foreach (var cand in candidates)
                            {
                                if (cand.Chunk == null || cand.Chunk.Length == 0) continue;
                                bool isSameAsBest = cand.StorageGuid == sorted[i].StorageGuid
                                    && cand.BucketId == sorted[i].BucketId;
                                if (isSameAsBest) continue;
                                if (cand.Similarity > secondBestSim)
                                {
                                    secondBestSim = cand.Similarity;
                                    secondBest = cand.Chunk.ToByteArray();
                                }
                            }
                        }

                        if (secondBest != null && secondBest.Length >= Globals.chunkSize)
                        {
                            int errBest = 0, errSecond = 0, errBoth = 0;
                            for (int j = 0; j < Globals.chunkSize; j++)
                            {
                                bool bestDiffers = fileChunks[i][j] != baseChunk[j];
                                bool secondDiffers = fileChunks[i][j] != secondBest[j];
                                if (bestDiffers) errBest++;
                                if (secondDiffers) errSecond++;
                                if (bestDiffers && secondDiffers) errBoth++;
                            }
                            mrefTotalErrorsBest += errBest;
                            mrefTotalErrorsBoth += errBoth;
                            mrefTotalFixedBySecond += (errBest - errBoth);
                            mrefChunksWithTwoCandidates++;
                        }
                    }
                }
            }
            phaseSw.Stop();

            averageErrorRate = reusedChunkWithDiffsCount > 0
                ? (float)totalDifferingBytes / ((long)reusedChunkWithDiffsCount * Globals.chunkSize)
                : 0f;
            string histStr = string.Join(",", errorRateHistogram.Select(h =>
                reusedChunkWithDiffsCount > 0 ? $"{100.0 * h / reusedChunkWithDiffsCount:F0}%" : "0%"));

            Console.WriteLine($"[Compress] DiffEncode: {phaseSw.ElapsedMilliseconds}ms, empty={emptyDiffCount}, diffs={diffCount}, zeroRef={zeroRefCount}, bloated={bloatedDiffRestore.Count}, avgErrorRate={averageErrorRate:P1}, errorDist=[{histStr}]");
            Observability.RecordStage("DiffEncode", phaseSw.Elapsed.TotalMilliseconds,
                ("chunk_count", sorted.Length), ("empty_diff", emptyDiffCount), ("non_empty_diff", diffCount),
                ("zero_ref", zeroRefCount), ("bloated_restore", bloatedDiffRestore.Count));

            if (mrefChunksWithTwoCandidates > 0)
            {
                double mrefAvgErrBest = 100.0 * mrefTotalErrorsBest / ((long)mrefChunksWithTwoCandidates * Globals.chunkSize);
                double mrefAvgOverlap = 100.0 * mrefTotalErrorsBoth / ((long)mrefChunksWithTwoCandidates * Globals.chunkSize);
                double mrefAvgFixed = mrefTotalErrorsBest > 0 ? 100.0 * mrefTotalFixedBySecond / mrefTotalErrorsBest : 0;
                double mrefCombinedErr = 100.0 * mrefTotalErrorsBoth / ((long)mrefChunksWithTwoCandidates * Globals.chunkSize);
                Console.WriteLine($"[Compress] MultiRefPotential: chunks_with_2_candidates={mrefChunksWithTwoCandidates}, " +
                    $"avgErrorBest={mrefAvgErrBest:F1}%, avgOverlap={mrefAvgOverlap:F1}%, " +
                    $"avgFixedBySecond={mrefAvgFixed:F1}%, theoreticalCombinedError={mrefCombinedErr:F1}%");
            }
            else
            {
                Console.WriteLine("[Compress] MultiRefPotential: no chunks with 2+ candidates available for overlap analysis");
            }
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

                int maxAllowed = (int)(Globals.chunkSize * bloatThreshold);
                if (differingBytes >= maxAllowed) return; // mosaic didn't help enough

                info.Donors = donors.Select(d => (d.BucketId, d.BucketKey)).ToList();
                info.StitchedBase = stitched;

                // Undo the bloat: restore to dedup state with mosaic base
                sorted[i]!.NeedToStore = false;
                pipelineState.SetEncodeBase(i, new ChunkEncodeBase.Mosaic(info));
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
                if (encodeBases[i] is ChunkEncodeBase.Mosaic) continue; // already rescued by top-K mosaic fallback
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
                            await AgentRpcThrottle.RunAsync(agentIp, async () =>
                            {
                                var client = GrpcChannelFactory.GetClient(
                                    target: agentIp,
                                    ctor: chan => new SearchLanes.SearchLanesClient(chan),
                                    roundRobin: false, port: 5000);

                                var req = new BatchSearchLanesReq();
                                req.Queries.AddRange(allLaneQueries);

                                var res = await client.BatchSearchAsync(req);

                                foreach (var qr in res.Results)
                                {
                                    if (qr == null || qr.Matches.Count == 0) continue;
                                    int qi = qr.QueryIndex;
                                    if (qi < 0 || qi >= queryToChunk.Count) continue;
                                    var (chunkIdx, laneIdx) = queryToChunk[qi];
                                    foreach (var m in qr.Matches)
                                        allMatches.Add((chunkIdx, laneIdx, m));
                                }
                            });
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
                        int maxAllowed = (int)(Globals.chunkSize * bloatThreshold);
                        if (differingBytes >= maxAllowed) return;

                        info.Donors = donors.Select(d => (d.BucketId, d.BucketKey)).ToList();
                        info.StitchedBase = stitched;

                        sorted[idx]!.NeedToStore = false;
                        pipelineState.SetEncodeBase(idx, new ChunkEncodeBase.Mosaic(info));
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

                        // Same contract: throttled, no app deadline, bounded retries.
                        var reStoreAgent = agent;
                        bool reStored = false;
                        int reStoreAttempt = 0;
                        while (!reStored)
                        {
                            try
                            {
                                var batchRes = await AgentRpcThrottle.RunAsync(reStoreAgent, async () =>
                                {
                                    var reStoreClient = GrpcChannelFactory.GetClient(
                                        target: reStoreAgent,
                                        ctor: chan => new StoreVector.StoreVectorClient(chan),
                                        roundRobin: false, port: 5000);
                                    return await reStoreClient.BatchStoreAsync(batchReq);
                                });

                                for (int j = 0; j < batchItems.Count && j < batchRes.Results.Count; j++)
                                {
                                    var storeRes = batchRes.Results[j];
                                    int chunkIdx = batchItems[j].index;
                                    string storedAgent = agent;

                                    if (storeRes.WasDeduplicated)
                                    {
                                        var baseBytes = storeRes.BaseChunk ?? ByteString.Empty;
                                        pipelineState.SetEncodeBase(chunkIdx, new ChunkEncodeBase.Ref(
                                            storeRes.Id,
                                            storeRes.Index,
                                            storeRes.StorageGuid ?? "",
                                            storedAgent,
                                            baseBytes));
                                        batchItems[j].response.NeedToStore = false;
                                        batchItems[j].response.Duplicate = true;
                                        batchItems[j].response.Similarity = storeRes.Similarity;
                                    }
                                    else
                                    {
                                        pipelineState.SetEncodeBase(chunkIdx, new ChunkEncodeBase.SelfFresh(
                                            storeRes.Id,
                                            storeRes.Index,
                                            storeRes.StorageGuid ?? "",
                                            storedAgent));
                                        // NeedToStore stays true (legacy semantics: the chunk's
                                        // bytes live at its own bucket now, signal for stats).
                                    }
                                }
                                reStored = true;
                            }
                            catch (Exception reStoreEx)
                            {
                                reStoreAttempt++;
                                if (reStoreAttempt >= AgentRpcThrottle.MaxAttempts)
                                {
                                    Console.WriteLine($"[Compress] Re-store to {reStoreAgent} failed after {reStoreAttempt} attempts ({reStoreEx.Message}); giving up");
                                    return;
                                }
                                var backoff = AgentRpcThrottle.BackoffFor(reStoreAttempt - 1);
                                Console.WriteLine($"[Compress] Re-store to {reStoreAgent} failed (attempt {reStoreAttempt}/{AgentRpcThrottle.MaxAttempts}): {reStoreEx.Message}; backoff {backoff.TotalSeconds}s then retry");
                                try
                                {
                                    await AgentHealthWatcher.Instance.WaitForAgentAsync(reStoreAgent, CancellationToken.None);
                                    await Task.Delay(backoff);
                                }
                                catch (OperationCanceledException) { return; }
                                reStoreAgent = RendezvousRouter.ResolveAgentIp(reStoreAgent);
                            }
                        }
                    }));
                }
            }

            await Task.WhenAll(reStoreTasks);

            int reDeduped = bloatedDiffRestore.Count(i => !sorted[i].NeedToStore && sorted[i].BucketId != 0);
            int freshStored = bloatedDiffRestore.Count(i => sorted[i].NeedToStore && sorted[i].BucketId != 0);
            int storeFailures = bloatedDiffRestore.Count(i => sorted[i].BucketId == 0);

            phaseSw.Stop();
            Console.WriteLine($"[Compress] BloatRestore: {phaseSw.ElapsedMilliseconds}ms, {bloatedDiffRestore.Count} chunks" +
                $", reDeduped={reDeduped}, freshStored={freshStored}" +
                (storeFailures > 0 ? $", storeFailures={storeFailures}" : ""));
        }

        // ── Post-BloatRestore: re-propagate rep EncodeBase to non-reps ──
        // Three things may have changed rep state between the original post-store
        // propagation and now:
        //   1. The rep was bloated and re-stored → rep's EncodeBase is now SelfFresh
        //      (fresh-stored) or Ref (deduped at re-store). The bucket coords changed.
        //   2. The rep was bloated and rescued by Mosaic-Fallback / Lane-Fallback →
        //      rep's EncodeBase is now Mosaic. Non-reps must adopt the new stitched base.
        //   3. The rep was NOT bloated → rep's EncodeBase is unchanged from before.
        //
        // Non-reps whose own state was also bloated and re-stored independently must
        // NOT be overwritten: they now have their own SelfFresh/Ref bucket and we'd be
        // throwing that away. Detect that via "non-rep was in bloatedDiffRestore" —
        // those went through their own re-store and own whatever EncodeBase that
        // produced. Non-reps that weren't bloated have an EncodeBase still inherited
        // from the rep's pre-bloat state, so they need re-derivation now.
        //
        // Same SelfFresh→RepFresh adaptation as the initial post-store propagation
        // (see that block for the full rationale).
        if (Globals.EnableChunkClustering)
        {
            var bloatedSet = bloatedDiffRestore.Count > 0
                ? new HashSet<int>(bloatedDiffRestore)
                : null;
            for (int i = 0; i < sorted.Length; i++)
            {
                if (representativeSet.Contains(i)) continue;
                if (bloatedSet != null && bloatedSet.Contains(i)) continue; // owns its own EncodeBase
                int rep = groupRepresentative[i];
                var repBase = encodeBases[rep];
                ChunkEncodeBase nonRepBase = repBase switch
                {
                    ChunkEncodeBase.SelfFresh sf => new ChunkEncodeBase.RepFresh(
                        sf.BucketId, sf.BucketKey, sf.StorageGuid, sf.TargetAgent, rep),
                    _ => repBase,
                };
                pipelineState.SetEncodeBase(i, nonRepBase);
            }
        }

        // ── Integrity check (3/3): RefRoundTrip ──
        //
        // The StoreRoundTrip and FetchedBases checks above prove that
        // sha256(sorted[i].Chunk) == sorted[i].StorageGuid — i.e. the bytes
        // we hold hash to the guid we hold. They do NOT prove the round-trip
        // that decompression actually performs:
        //
        //   decompress: ref (BucketId, BucketKey)
        //     → agent looks up storageGuid' from those coordinates
        //     → fetches bytes for storageGuid'
        //     → applies our encoded diff on top.
        //
        // For the file to round-trip, those decompress-side bytes must equal
        // the bytes we encoded the diff against (sorted[i].Chunk). If the
        // agent's (BucketId, BucketKey) → storageGuid mapping is stale, lazy,
        // or out of sync with the in-memory snapshot the search returned, the
        // bytes returned at decompress will be different — and the whole-block
        // SHA fails with "block N/M: exp=... got=...".
        //
        // This check simulates that lookup at compress time for a random
        // sample of dedup-hit chunks. Sample budget is tunable via
        // INTEGRITY_REF_ROUNDTRIP_SAMPLE (default 128). Mosaic chunks are
        // excluded — they have their own multi-donor decode contract.
        if (global::Cross.Services.JobEvents.IntegrityDiagnostics.Enabled)
        {
            int sampleBudget = ParseEnvInt("INTEGRITY_REF_ROUNDTRIP_SAMPLE", 128, 1);
            // Only Ref-encoded chunks are eligible for the round-trip probe:
            //   - SelfFresh   — decoder will fetch from the chunk's own bucket, the
            //                   bytes there ARE fileChunks[i], no agent-side staleness
            //                   risk because we just wrote them.
            //   - RepFresh    — decoder fetches rep's bucket, same reasoning.
            //   - Mosaic      — decoder doesn't fetch a single bucket; re-stitches.
            //   - Zeros       — no bucket reference at all.
            //   - Ref         — decoder fetches an EXISTING stored chunk's bucket; this
            //                   is exactly where the encoder vs decoder disagreement
            //                   surfaces (agent's (B,K) → guid mapping might be stale
            //                   relative to our in-memory BaseBytes). Sample these.
            var eligible = new List<int>();
            for (int i = 0; i < encodeBases.Length; i++)
            {
                if (encodeBases[i] is not ChunkEncodeBase.Ref r) continue;
                if (r.BucketId == 0) continue;
                if (r.BaseBytes.Length == 0) continue;
                eligible.Add(i);
            }

            if (eligible.Count > 0)
            {
                // Pseudo-random but deterministic sample (Fisher–Yates partial shuffle)
                // so repeated runs over the same data probe the same indices and we
                // can correlate findings across logs.
                var rng = new Random(0x5EED5);
                int take = Math.Min(sampleBudget, eligible.Count);
                for (int s = 0; s < take; s++)
                {
                    int swap = s + rng.Next(eligible.Count - s);
                    (eligible[s], eligible[swap]) = (eligible[swap], eligible[s]);
                }
                var sampledIndices = eligible.Take(take).ToList();

                var swRT = Stopwatch.StartNew();
                var fetchedBytes = new byte[encodeBases.Length][];
                var rtTasks = sampledIndices.Select(async idx =>
                {
                    if (encodeBases[idx] is not ChunkEncodeBase.Ref r) return;
                    try
                    {
                        var got = await _chunkReferenceClient.GetChunkByReferenceAsync(
                            r.BucketId, r.BucketKey,
                            targetAgent: string.IsNullOrWhiteSpace(r.TargetAgent) ? null : r.TargetAgent);
                        if (got != null && got.Length > 0)
                            fetchedBytes[idx] = got;
                    }
                    catch { /* leave null — counted as fetch-failure mismatch */ }
                }).ToList();
                await Task.WhenAll(rtTasks);

                int total = 0;
                int mismatches = 0;
                int fetchFailures = 0;
                string? firstDetail = null;
                foreach (var idx in sampledIndices)
                {
                    if (encodeBases[idx] is not ChunkEncodeBase.Ref r) continue;
                    byte[]? got = fetchedBytes[idx];
                    if (got == null || got.Length == 0)
                    {
                        fetchFailures++;
                        mismatches++;
                        total++;
                        if (firstDetail == null)
                            firstDetail = $"idx={idx} bk=({r.BucketId},{r.BucketKey}) FETCH_FAIL exp_guid={TakeShort(r.StorageGuid, 16)}";
                        continue;
                    }
                    total++;
                    // ToByteArray() copies the ByteString into a fresh array.
                    // We can't use .Span here because the enclosing method is
                    // async (C# disallows ref-struct locals in async bodies).
                    byte[] expected = r.BaseBytes.ToByteArray();
                    bool equal = got.Length == expected.Length
                                 && got.AsSpan().SequenceEqual(expected);
                    if (!equal)
                    {
                        mismatches++;
                        if (firstDetail == null)
                        {
                            string expHash = Sha256HexLocal(expected);
                            string gotHash = Sha256HexLocal(got);
                            firstDetail =
                                $"idx={idx} bk=({r.BucketId},{r.BucketKey}) " +
                                $"exp_guid={TakeShort(r.StorageGuid, 16)} " +
                                $"got_hash={TakeShort(gotHash, 16)} " +
                                $"exp_hash={TakeShort(expHash, 16)} " +
                                $"sizes exp={expected.Length} got={got.Length}";
                        }
                    }
                }
                swRT.Stop();

                global::Cross.Services.JobEvents.JobEventBus.EmitStageDone(
                    "IntegrityCheck:RefRoundTrip",
                    swRT.Elapsed.TotalMilliseconds,
                    chunkCount: total,
                    bucketCount: mismatches,
                    bytes: (ulong)fetchFailures);

                if (mismatches > 0)
                {
                    Console.WriteLine(
                        $"[Integrity] ❌ RefRoundTrip: {mismatches}/{total} mismatch " +
                        $"({fetchFailures} fetch-fail) — sample={take} eligible={eligible.Count} — {firstDetail}");
                }
                else
                {
                    Console.WriteLine(
                        $"[Integrity] ✓ RefRoundTrip: {total} verified (sample={take}/{eligible.Count}) " +
                        $"in {swRT.Elapsed.TotalMilliseconds:F1}ms");
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
            $"rawStoredBytes={rawStoredBytes}, datacenterBytesZstd={datacenterBytesStored}, " +
            $"avgErrorRate={averageErrorRate:P1}");

        // v5.0.0: references are built by BuildV5References (ref table + compact indices)

        // ── ERROR ENCODING ──
        byte[] errorDictionaryBytes;
        {
            using var stage = Observability.StartStage("PatchEncode");
            phaseSw.Restart();

            int chunkCount = sorted.Length;
            int cs = Globals.chunkSize;
            int totalBytes = chunkCount * cs;
            int bitmaskBytes = cs / 8;

            // Phase 1: compute per-chunk diffs (shared by v5.6.0 and v6.0.0)
            var chunkErrors = new List<(ushort Offset, byte Value)>[chunkCount];
            int patchCount = 0;

            for (int i = 0; i < chunkCount; i++)
            {
                byte[] original = fileChunks[i];
                // Single source of truth: the EncodeBase tells PatchEncode exactly
                // which bytes to subtract from `original` to produce the diff. The
                // very same value is consumed by BuildV5References to write the
                // matching ref byte, so encoder and decoder cannot disagree.
                byte[] baseChunk = encodeBases[i].GetBasisBytes(i, fileChunks, cs);

                int baseLen = Math.Min(baseChunk.Length, cs);
                var errs = new List<(ushort Offset, byte Value)>();
                for (int b = 0; b < cs; b++)
                {
                    byte refByte = b < baseLen ? baseChunk[b] : (byte)0;
                    if (original[b] != refByte)
                        errs.Add(((ushort)b, original[b]));
                }
                chunkErrors[i] = errs;
                patchCount += errs.Count;
            }

            if (encodingVersion == "v6.0.0")
            {
                errorDictionaryBytes = EncodeErrorsV6(chunkErrors, chunkCount, cs, patchCount, totalBytes);
            }
            else
            {
                // v5.6.0: 3-stream bitmask encoding
                byte[] modeBitfield = new byte[(chunkCount + 7) / 8];
                using var bitmaskMs = new MemoryStream();
                using var valMs = new MemoryStream();
                int dupChunks = 0, bitmaskChunks = 0;

                for (int i = 0; i < chunkCount; i++)
                {
                    if (chunkErrors[i].Count == 0) { dupChunks++; continue; }

                    modeBitfield[i >> 3] |= (byte)(1 << (i & 7));
                    bitmaskChunks++;

                    byte[] mask = new byte[bitmaskBytes];
                    foreach (var (off, _) in chunkErrors[i])
                        mask[off >> 3] |= (byte)(1 << (off & 7));
                    bitmaskMs.Write(mask, 0, bitmaskBytes);

                    foreach (var (_, val) in chunkErrors[i])
                        valMs.WriteByte(val);
                }

                byte[] rawMode = modeBitfield;
                byte[] rawBitmasks = bitmaskMs.ToArray();
                byte[] rawVals = valMs.ToArray();

                var liveDict = Globals.EnableCcfStore ? CcfPackOptimizerService.LiveDictionary : null;
                byte[] zstdMode, zstdBitmasks, zstdVals;
                if (liveDict != null)
                {
                    using var c1 = new Compressor(19); c1.LoadDictionary(liveDict);
                    zstdMode = c1.Wrap(rawMode).ToArray();
                    using var c2 = new Compressor(19); c2.LoadDictionary(liveDict);
                    zstdBitmasks = c2.Wrap(rawBitmasks).ToArray();
                    using var c3 = new Compressor(19); c3.LoadDictionary(liveDict);
                    zstdVals = c3.Wrap(rawVals).ToArray();
                }
                else
                {
                    using var c = new Compressor(19);
                    zstdMode = c.Wrap(rawMode).ToArray();
                    zstdBitmasks = c.Wrap(rawBitmasks).ToArray();
                    zstdVals = c.Wrap(rawVals).ToArray();
                }

                using var outMs = new MemoryStream();
                using var bw2 = new BinaryWriter(outMs, Encoding.UTF8, leaveOpen: true);
                bw2.Write(patchCount);
                bw2.Write(totalBytes);
                bw2.Write(chunkCount);
                bw2.Write(zstdMode.Length);
                bw2.Write(zstdBitmasks.Length);
                bw2.Write(zstdVals.Length);
                bw2.Write(zstdMode);
                bw2.Write(zstdBitmasks);
                bw2.Write(zstdVals);
                bw2.Flush();
                errorDictionaryBytes = outMs.ToArray();

                Console.WriteLine($"[Compress] PatchEncode(v5.6.0): {phaseSw.ElapsedMilliseconds}ms, " +
                    $"totalBytes={totalBytes}, patches={patchCount} ({100.0 * patchCount / totalBytes:F1}%), " +
                    $"dup={dupChunks}, bitmask={bitmaskChunks} (of {chunkCount}), " +
                    $"modeStream={rawMode.Length}→{zstdMode.Length}, " +
                    $"bitmaskStream={rawBitmasks.Length}→{zstdBitmasks.Length}, valStream={rawVals.Length}→{zstdVals.Length}, " +
                    $"totalErrorPayload={errorDictionaryBytes.Length}");
            }

            phaseSw.Stop();
            Observability.RecordStage("PatchEncode", phaseSw.Elapsed.TotalMilliseconds,
                ("total_bytes", totalBytes), ("patch_count", patchCount), ("raw_bytes", errorDictionaryBytes.Length));
        }

        byte[] toReturn;
        {
            using var stage = Observability.StartStage("Serialize");
            var swStage = Stopwatch.StartNew();
            byte[] refBytes = BuildV5References(encodeBases);
            int storedForLog = sorted.Count(r => r != null && r.NeedToStore);

            // Defer the heavy-collection release if the smoketest is on — we need
            // sorted/encodeBases/representativeSet/groupRepresentative/fileChunks
            // to attribute any round-trip mismatch back to a specific encode case.
            bool runSmoketest = Globals.IntegrityDecompressSmoketest && _fileLen > 0;
            if (!runSmoketest)
            {
                fileChunks = null!;
                vectors = null!;
                bitStrings = null!;
                mainAgents = null!;
                chunkMap = null!;
                mosaicCandidates = null!;
                sorted = null!;
                encodeBases = null!;
                pipelineState = null!;
                representativeSet = null!;
                groupRepresentative = null!;
            }

            byte[] fileHash = SHA256.HashData(_file.AsSpan(0, _fileLen));
            toReturn = BuildCompressedPayload(
                encodingVersion,
                refBytes,
                errorDictionaryBytes,
                trimmedChunk.ToArray(),
                fileHash);
            swStage.Stop();
            Observability.RecordStage("Serialize", swStage.Elapsed.TotalMilliseconds, ("output_bytes", toReturn.Length));

            if (runSmoketest)
            {
                await RunCompressDecompressSmoketestAsync(
                    toReturn, _file, _fileLen,
                    fileChunks, sorted, encodeBases,
                    representativeSet, groupRepresentative,
                    totalChunks);

                fileChunks = null!;
                vectors = null!;
                bitStrings = null!;
                mainAgents = null!;
                chunkMap = null!;
                mosaicCandidates = null!;
                sorted = null!;
                encodeBases = null!;
                pipelineState = null!;
                representativeSet = null!;
                groupRepresentative = null!;
            }

            totalSw.Stop();
            Console.WriteLine($"[Compress] DONE: {totalSw.ElapsedMilliseconds}ms total, in={_fileLen} out={toReturn.Length} ratio={toReturn.Length/(double)Math.Max(1,_fileLen):F3}, dedup={referencesFound}, lshMatches={initialMatches}, stored={storedForLog}");
        }

        return (toReturn, referencesFound, totalChunks, datacenterBytesStored, averageErrorRate, errorDictionaryBytes.Length);
    }

    public async Task<byte[]> CompressFile(byte[] _file)
    {
        var res = await CompressFileWithStats(_file);
        return res.CompressedBytes;
    }

    // ═══════════════════════════════════════════════════════════════════════
    // In-process decompress smoke test
    // ═══════════════════════════════════════════════════════════════════════
    //
    // Decompresses the just-produced CCF inside the SAME pod that compressed
    // it, then compares byte-for-byte against the original input.  Catches:
    //
    //   1. encode-case / decode-case asymmetries (e.g. encoder thought
    //      baseChunk was X, decoder fetches Y),
    //   2. fresh-store coordinate drift (cross stored at (B, K) but the
    //      RendezvousRouter on decode picks a different agent for (B, K)),
    //   3. write-batcher / WAL races (chunk in WAL but not yet visible to
    //      GetChunkByReference),
    //   4. bloat-guard re-store coordinate updates not propagated to the
    //      eventual encode case.
    //
    // Off by default — gated by INTEGRITY_DECOMPRESS_SMOKETEST=true.  When on,
    // every Compress doubles in latency.  Worth it during integrity hunts.
    // ═══════════════════════════════════════════════════════════════════════
    private async Task RunCompressDecompressSmoketestAsync(
        byte[] toReturn,
        byte[] originalFile,
        int originalLen,
        List<byte[]> fileChunks,
        global::GatewayService.QueryResponseObject?[] sorted,
        global::Cross.Models.ChunkEncodeBase[] encodeBases,
        HashSet<int> representativeSet,
        int[] groupRepresentative,
        int totalChunks)
    {
        var sw = Stopwatch.StartNew();
        byte[]? roundTripped = null;
        Exception? decompressError = null;
        bool priorSuppress = _smoketestSuppressIntegrityThrow.Value;
        _smoketestSuppressIntegrityThrow.Value = true;
        try
        {
            roundTripped = await DecompressFile(toReturn);
        }
        catch (Exception ex)
        {
            decompressError = ex;
        }
        finally
        {
            _smoketestSuppressIntegrityThrow.Value = priorSuppress;
        }
        sw.Stop();

        string jobId = global::Cross.Services.JobEvents.JobEventBus.CurrentJobId ?? "no-job";

        if (decompressError != null)
        {
            string msg = $"smoketest decompress threw: {decompressError.GetType().Name}: {decompressError.Message}";
            Console.WriteLine($"[Integrity] ❌ {msg}");
            global::Cross.Services.JobEvents.JobEventBus.EmitFailed(
                jobId, "INTEGRITY_SMOKETEST", msg, "Compress");
            return;
        }

        if (roundTripped == null || roundTripped.Length != originalLen)
        {
            int got = roundTripped?.Length ?? 0;
            string msg = $"smoketest length mismatch: orig={originalLen}, decompressed={got}";
            Console.WriteLine($"[Integrity] ❌ {msg}");
            global::Cross.Services.JobEvents.JobEventBus.EmitFailed(
                jobId, "INTEGRITY_SMOKETEST", msg, "Compress");
            return;
        }

        int firstDiff = -1;
        for (int i = 0; i < originalLen; i++)
        {
            if (originalFile[i] != roundTripped[i]) { firstDiff = i; break; }
        }

        if (firstDiff < 0)
        {
            Console.WriteLine($"[Integrity] ✓ SMOKETEST round-trip OK ({originalLen} bytes, {totalChunks} chunks) in {sw.ElapsedMilliseconds}ms");
            global::Cross.Services.JobEvents.JobEventBus.EmitStageDone(
                "IntegrityCheck:Smoketest:Pass",
                sw.Elapsed.TotalMilliseconds,
                chunkCount: totalChunks,
                bucketCount: 0,
                bytes: (ulong)originalLen);
            return;
        }

        int cs = Globals.chunkSize;
        int chunkIdx = Math.Min(firstDiff / cs, totalChunks - 1);

        // SHA256 of the original chunk vs the round-tripped chunk.
        int chunkStart = chunkIdx * cs;
        int chunkEnd = Math.Min(chunkStart + cs, originalLen);
        int chunkLen = chunkEnd - chunkStart;

        byte[] origHash = SHA256.HashData(originalFile.AsSpan(chunkStart, chunkLen).ToArray());
        byte[] gotHash = SHA256.HashData(roundTripped.AsSpan(chunkStart, chunkLen).ToArray());

        var r = sorted != null && chunkIdx < sorted.Length ? sorted[chunkIdx] : null;
        var eb = encodeBases != null && chunkIdx < encodeBases.Length
            ? encodeBases[chunkIdx]
            : global::Cross.Models.ChunkEncodeBase.Zeros.Instance;
        ulong bucketId = eb.BucketId();
        ulong bucketKey = eb.BucketKey();
        string storageGuid = eb.StorageGuid();
        bool needToStore = r?.NeedToStore ?? false;
        bool isMosaic = eb is global::Cross.Models.ChunkEncodeBase.Mosaic;
        bool isRep = representativeSet != null && representativeSet.Contains(chunkIdx);
        int rep = (groupRepresentative != null && chunkIdx < groupRepresentative.Length)
            ? groupRepresentative[chunkIdx] : -1;
        bool repIsFresh = rep >= 0 && encodeBases != null && rep < encodeBases.Length
                          && encodeBases[rep] is global::Cross.Models.ChunkEncodeBase.SelfFresh;

        // Encode-case label comes straight from the sum type — no decision tree to
        // get out of sync with PatchEncode any more.
        string encodeCase = eb.CaseLabel();

        // What did the encoder use as the diff basis? Hash the exact same bytes
        // PatchEncode subtracted from fileChunks[chunkIdx]. If this doesn't match
        // what the decoder recovered, the EncodeBase is inconsistent with the
        // wire fields / decoder pipeline.
        string encoderBaseSha = "n/a";
        if (fileChunks != null && chunkIdx < fileChunks.Count)
        {
            try
            {
                byte[] basis = eb.GetBasisBytes(chunkIdx, fileChunks, fileChunks[chunkIdx].Length);
                encoderBaseSha = HexShort(SHA256.HashData(basis));
            }
            catch { /* leave as n/a */ }
        }

        // What does the agent return RIGHT NOW for the same ref the decode path used?
        // We query BOTH the recorded TargetAgent AND the canonical rendezvous owner,
        // so if they disagree the dashboard tells us immediately. DecompressFile
        // uses RendezvousRouter.PickAgent(bitstring) — so that's the agent whose
        // bytes the encoder MUST be diffing against.
        string targetAgent = r?.TargetAgent ?? "";
        string bitstringNow = bucketId != 0 ? UlongToBitstring(bucketId) : "";
        string canonicalAgent = bucketId != 0 ? (RendezvousRouter.PickAgent(bitstringNow) ?? "") : "";

        string targetAgentFetchSha = "n/a";
        int targetAgentFetchLen = -1;
        string canonicalAgentFetchSha = "n/a";
        int canonicalAgentFetchLen = -1;
        if (bucketId != 0)
        {
            try
            {
                var fetched = await _chunkReferenceClient.GetChunkByReferenceAsync(
                    bucketId, bucketKey,
                    targetAgent: string.IsNullOrWhiteSpace(targetAgent) ? null : targetAgent);
                if (fetched != null && fetched.Length > 0)
                {
                    targetAgentFetchLen = fetched.Length;
                    targetAgentFetchSha = HexFull(SHA256.HashData(fetched));
                }
                else
                {
                    targetAgentFetchSha = "empty";
                }
            }
            catch (Exception ex)
            {
                targetAgentFetchSha = $"err:{ex.GetType().Name}";
            }

            // Only re-fetch from canonical if it differs from TargetAgent — saves an
            // RPC per chunk in the common case.
            if (!string.IsNullOrWhiteSpace(canonicalAgent) && canonicalAgent != targetAgent)
            {
                try
                {
                    var fetched = await _chunkReferenceClient.GetChunkByReferenceAsync(
                        bucketId, bucketKey, targetAgent: canonicalAgent);
                    if (fetched != null && fetched.Length > 0)
                    {
                        canonicalAgentFetchLen = fetched.Length;
                        canonicalAgentFetchSha = HexFull(SHA256.HashData(fetched));
                    }
                    else
                    {
                        canonicalAgentFetchSha = "empty";
                    }
                }
                catch (Exception ex)
                {
                    canonicalAgentFetchSha = $"err:{ex.GetType().Name}";
                }
            }
            else
            {
                canonicalAgentFetchSha = "same-as-target";
                canonicalAgentFetchLen = targetAgentFetchLen;
            }
        }

        // Also probe what the agent has for the StorageGuid cross is carrying —
        // catches the case where sorted[i].StorageGuid points at a guid the
        // cluster doesn't have any chunk for (e.g., guid leaked from a different
        // bucket's record into this chunk's sorted[] slot).
        string sgidFetchSha = "n/a";
        int sgidFetchLen = -1;
        if (!string.IsNullOrEmpty(storageGuid))
        {
            try
            {
                var fetched = await _chunkReferenceClient.GetChunkByStorageGuidAsync(
                    storageGuid,
                    targetAgent: string.IsNullOrWhiteSpace(canonicalAgent) ? null : canonicalAgent);
                if (fetched != null && fetched.Length > 0)
                {
                    sgidFetchLen = fetched.Length;
                    sgidFetchSha = HexFull(SHA256.HashData(fetched));
                }
                else
                {
                    sgidFetchSha = "empty";
                }
            }
            catch (Exception ex)
            {
                sgidFetchSha = $"err:{ex.GetType().Name}";
            }
        }

        // Full encoder-base sha (same EncodeBase basis bytes, full hash).
        string encoderBaseShaFull = "n/a";
        if (fileChunks != null && chunkIdx < fileChunks.Count)
        {
            try
            {
                byte[] basis = eb.GetBasisBytes(chunkIdx, fileChunks, fileChunks[chunkIdx].Length);
                encoderBaseShaFull = HexFull(SHA256.HashData(basis));
            }
            catch { /* leave as n/a */ }
        }

        // Always include both the truncated (for the 1500-char EmitFailed cap)
        // and the full hashes (stdout-only, kubectl logs has them).
        string console =
            $"chunk #{chunkIdx}/{totalChunks} firstByte={firstDiff} " +
            $"case={encodeCase} isRep={isRep} rep={rep} needToStore={needToStore} " +
            $"bId={bucketId:x} bKey={bucketKey:x} " +
            $"sgid={storageGuid} " +
            $"origSha={HexFull(origHash)} gotSha={HexFull(gotHash)} " +
            $"encoderBaseSha={encoderBaseShaFull} " +
            $"targetAgent={targetAgent} targetAgentFetchSha={targetAgentFetchSha} targetAgentFetchLen={targetAgentFetchLen} " +
            $"canonicalAgent={canonicalAgent} canonicalAgentFetchSha={canonicalAgentFetchSha} canonicalAgentFetchLen={canonicalAgentFetchLen} " +
            $"sgidFetchSha={sgidFetchSha} sgidFetchLen={sgidFetchLen}";

        Console.WriteLine($"[Integrity] ❌ SMOKETEST MISMATCH: {console}");

        // Dashboard event — keep this terser (EmitFailed truncates to 1500 chars
        // now, see JobEventBus.cs). Full diagnostic is always in stdout above.
        string sgidShort = string.IsNullOrEmpty(storageGuid)
            ? "(none)"
            : storageGuid.Substring(0, Math.Min(16, storageGuid.Length));
        string targetAgentShort = string.IsNullOrEmpty(targetAgent) ? "(none)" : targetAgent;
        string canonicalAgentShort = string.IsNullOrEmpty(canonicalAgent) ? "(none)" : canonicalAgent;
        string msgDetail =
            $"chunk #{chunkIdx}/{totalChunks} firstByte={firstDiff} " +
            $"case={encodeCase} isRep={isRep} rep={rep} needToStore={needToStore} " +
            $"bId={bucketId:x} bKey={bucketKey:x} " +
            $"sgid={sgidShort} " +
            $"origSha={HexShort(origHash)} gotSha={HexShort(gotHash)} " +
            $"encBaseSha={HexShort(encoderBaseShaFull.AsSpan())} " +
            $"tgtAgent={targetAgentShort} tgtFetchSha={HexShort(targetAgentFetchSha.AsSpan())} " +
            $"canAgent={canonicalAgentShort} canFetchSha={HexShort(canonicalAgentFetchSha.AsSpan())} " +
            $"sgidFetchSha={HexShort(sgidFetchSha.AsSpan())}";

        global::Cross.Services.JobEvents.JobEventBus.EmitFailed(
            jobId, "INTEGRITY_SMOKETEST", msgDetail, "Compress");
    }

    private static string HexFull(ReadOnlySpan<byte> bytes)
    {
        var sb = new StringBuilder(bytes.Length * 2);
        for (int i = 0; i < bytes.Length; i++)
            sb.Append(bytes[i].ToString("x2"));
        return sb.ToString();
    }

    // Truncate an already-hex string to 16 chars without re-hashing — used for
    // the dashboard message where we already have full hex strings and don't
    // want to re-hash. If the input isn't hex (e.g., "empty", "err:..."),
    // return it as-is.
    private static string HexShort(ReadOnlySpan<char> hexOrLabel)
    {
        if (hexOrLabel.Length <= 16) return hexOrLabel.ToString();
        // Treat anything non-hex (contains '-', ':', etc.) as a label.
        for (int i = 0; i < hexOrLabel.Length && i < 16; i++)
        {
            char c = hexOrLabel[i];
            bool isHex = (c >= '0' && c <= '9') || (c >= 'a' && c <= 'f') || (c >= 'A' && c <= 'F');
            if (!isHex) return hexOrLabel.ToString();
        }
        return hexOrLabel.Slice(0, 16).ToString();
    }

    private static string HexShort(ReadOnlySpan<byte> bytes, int hexChars = 16)
    {
        int n = Math.Min(hexChars, bytes.Length * 2);
        var sb = new StringBuilder(n);
        int bytesNeeded = (n + 1) / 2;
        for (int i = 0; i < bytesNeeded && i < bytes.Length; i++)
            sb.Append(bytes[i].ToString("x2"));
        if (sb.Length > n) sb.Length = n;
        return sb.ToString();
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
            // Pool the 1 MB hashing buffer — every file compression allocates it
            // and lets it die on the LOH. ArrayPool reuses across calls so a
            // batch of 60 000 JPEGs no longer churns 60 000 × 1 MB through Gen2.
            var pool = System.Buffers.ArrayPool<byte>.Shared;
            byte[] buf = pool.Rent(1024 * 1024);
            try
            {
                int read;
                while ((read = await hashStream.ReadAsync(buf, 0, buf.Length, ct)) > 0)
                    sha.TransformBlock(buf, 0, read, null, 0);
                sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                originalHash = sha.Hash!;
            }
            finally { pool.Return(buf); }
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

            var (compressedBlock, refs, chunks, _, _, _) = await CompressFileWithStats(windowData);
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
            var (compressedBlock, refs, chunks, _, _, _) = await CompressFileWithStats(windowData);
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
        // Byte-merge metadata for 0x05 refs (pair merge with per-byte bitmask)
        var byteMergeRefs = new Dictionary<int, (List<(ulong BucketId, string StorageGuid, ulong BucketIndex)> Donors, byte[] Bitmask)>();

        if (header.Version is "v5.0.0" or "v5.1.0" or "v5.2.0" or "v5.3.0" or "v5.4.0" or "v5.5.0" or "v5.6.0" or "v6.0.0")
        {
            // v5.x: compact ref table + ushort indices
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
                    case 0x05:
                    {
                        // Byte-level pair merge: 2 donor refs + per-byte bitmask
                        int bitmaskLen = (Globals.chunkSize + 7) / 8;
                        var donors = new List<(ulong BucketId, string StorageGuid, ulong BucketIndex)>(2);
                        for (int d = 0; d < 2; d++)
                        {
                            ushort tableIdx = BitConverter.ToUInt16(file, off);
                            off += sizeof(ushort);
                            if (tableIdx >= refTableSize)
                                throw new InvalidDataException($"v5 byte-merge donor table index {tableIdx} out of range");
                            donors.Add((refTable[tableIdx].BucketId, "", refTable[tableIdx].BucketIndex));
                        }
                        var byteMergeBitmask = new byte[bitmaskLen];
                        Buffer.BlockCopy(file, off, byteMergeBitmask, 0, bitmaskLen);
                        off += bitmaskLen;

                        byteMergeRefs[i] = (donors, byteMergeBitmask);
                        refBucketIds[i] = ulong.MaxValue;
                        refBucketIndices[i] = ulong.MaxValue;
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
                    case 0x05: // byte-level pair merge (v3.x inline format)
                    {
                        int bitmaskLen = (Globals.chunkSize + 7) / 8;
                        var donors = new List<(ulong BucketId, string StorageGuid, ulong BucketIndex)>(2);
                        for (int d = 0; d < 2; d++)
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
                        var byteMergeBitmask = new byte[bitmaskLen];
                        Buffer.BlockCopy(file, off, byteMergeBitmask, 0, bitmaskLen);
                        off += bitmaskLen;

                        byteMergeRefs[i] = (donors, byteMergeBitmask);
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

        // ── Parse error encoding ──
        // v5.6.0: bitmask-only with direct bytes, 1-bit mode (0=dup, 1=bitmask)
        // v5.5.0: skip:value with direct bytes, 2-bit mode (00=dup, 01=skip, 10=bitmask)
        // v5.4.0: four-stream with XOR (mode bitfield + skip + bitmask + value)
        // v5.3.0: split-stream (skip stream + value stream, each Zstd-19 compressed internally)
        // v5.2.0: varint(skip) + byte(xor) pairs (Zstd-19 compressed)
        // v5.1.0: uint32(skip) + byte(xor) pairs (Zstd-19 compressed)
        // v5.0.0: flat XOR patch (Zstd-9 compressed)
        // v3.1.0: RLE runs (Zstd-compressed), older: raw RLE
        byte[] errorBytes;
        bool isXorPatch = (header.Version == "v5.0.0");
        bool isSkipXorPatch = (header.Version == "v5.1.0");
        bool isRunXorPatch = (header.Version == "v5.2.0");
        bool isSplitStreamPatch = (header.Version == "v5.3.0");
        bool isFourStreamPatch = (header.Version == "v5.4.0");
        bool isDirectPatch = (header.Version == "v5.5.0");
        bool isBitmaskOnlyPatch = (header.Version == "v5.6.0");
        bool isTransformProgram = (header.Version == "v6.0.0");
        (int startPos, int runLength, short diffValue)[]? patches = null;

        if (header.Version is "v5.3.0" or "v5.4.0" or "v5.5.0" or "v5.6.0" or "v6.0.0")
        {
            errorBytes = new byte[header.ErrorLength];
            Buffer.BlockCopy(file, errorOffset, errorBytes, 0, header.ErrorLength);
        }
        else if (header.Version is "v3.1.0" or "v5.0.0" or "v5.1.0" or "v5.2.0")
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

        if (!isXorPatch && !isSkipXorPatch && !isRunXorPatch && !isSplitStreamPatch && !isFourStreamPatch && !isDirectPatch && !isBitmaskOnlyPatch && !isTransformProgram)
        {
            using var errorMs = new MemoryStream(errorBytes, writable: false);
            using var reader = new BinaryReader(errorMs, Encoding.UTF8, leaveOpen: true);
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
            else if (byteMergeRefs.ContainsKey(i))
            {
                int idx = i;
                var (bmDonors, bitmask) = byteMergeRefs[idx];
                fetchTasks[i] = Task.Run(async () =>
                {
                    int chSize = Globals.chunkSize;
                    var donorChunks = new byte[2][];
                    var donorFetches = new Task[2];
                    for (int d = 0; d < 2; d++)
                    {
                        int dIdx = d;
                        var (dBucketId, dGuid, dBucketIdx) = bmDonors[dIdx];
                        donorFetches[dIdx] = Task.Run(async () =>
                        {
                            string bitstring = UlongToBitstring(dBucketId);
                            string targetAgent = RendezvousRouter.PickAgent(bitstring);
                            byte[]? chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(dBucketId, dBucketIdx, targetAgent);
                            if (chunk == null)
                            {
                                for (int retry = 0; retry < 3; retry++)
                                {
                                    await Task.Delay(100 * (retry + 1));
                                    chunk = await _chunkReferenceClient.GetChunkByReferenceAsync(dBucketId, dBucketIdx, targetAgent);
                                    if (chunk != null) break;
                                }
                            }
                            donorChunks[dIdx] = chunk ?? new byte[chSize];
                        });
                    }
                    await Task.WhenAll(donorFetches);

                    var merged = new byte[chSize];
                    for (int j = 0; j < chSize; j++)
                    {
                        bool useDonorB = (bitmask[j >> 3] & (1 << (j & 7))) != 0;
                        merged[j] = useDonorB ? donorChunks[1][j] : donorChunks[0][j];
                    }
                    baseChunks[idx] = merged;
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

        // ── Apply error encoding ──
        if (isTransformProgram)
        {
            // v6.0.0: transform program decoder
            using var errMs = new MemoryStream(errorBytes, writable: false);
            using var errBr = new BinaryReader(errMs);
            int v6PatchCount = errBr.ReadInt32();
            int v6TotalBytes = errBr.ReadInt32();
            int v6ChunkCount = errBr.ReadInt32();
            int v6ChunkSize = errBr.ReadUInt16();
            int progBlobLen = errBr.ReadInt32();
            int resBlobLen = errBr.ReadInt32();

            byte[] progBlob = new byte[progBlobLen];
            errMs.ReadExactly(progBlob, 0, progBlobLen);
            byte[] resBlob = new byte[resBlobLen];
            errMs.ReadExactly(resBlob, 0, resBlobLen);

            var v6Dict = Globals.EnableCcfStore ? CcfPackOptimizerService.LiveDictionary : null;
            byte[] rawProg, rawRes;
            if (v6Dict != null)
            {
                try
                {
                    using var dd = new Decompressor(); dd.LoadDictionary(v6Dict);
                    rawProg = dd.Unwrap(progBlob).ToArray();
                    rawRes = dd.Unwrap(resBlob).ToArray();
                }
                catch
                {
                    using var dd2 = new Decompressor();
                    rawProg = dd2.Unwrap(progBlob).ToArray();
                    rawRes = dd2.Unwrap(resBlob).ToArray();
                }
            }
            else
            {
                using var d = new Decompressor();
                rawProg = d.Unwrap(progBlob).ToArray();
                rawRes = d.Unwrap(resBlob).ToArray();
            }

            // Execute program instructions
            using var pMs = new MemoryStream(rawProg, writable: false);
            using var pBr = new BinaryReader(pMs);
            int instrCount = pBr.ReadUInt16();

            for (int inst = 0; inst < instrCount; inst++)
            {
                byte opcode = pBr.ReadByte();
                switch (opcode)
                {
                    case 0x01: // CONST_FIELD
                    {
                        int cStart = pBr.ReadUInt16();
                        int cEnd = pBr.ReadUInt16();
                        int bOff = pBr.ReadUInt16();
                        int fLen = pBr.ReadByte();
                        byte[] vals = pBr.ReadBytes(fLen);
                        for (int ci = cStart; ci <= cEnd; ci++)
                        {
                            int bufOff = ci * v6ChunkSize + bOff;
                            for (int f = 0; f < fLen && bufOff + f < baseBuffer.Length; f++)
                                baseBuffer[bufOff + f] = vals[f];
                        }
                        break;
                    }
                    case 0x02: // SEQ_FIELD
                    {
                        int cStart = pBr.ReadUInt16();
                        int cEnd = pBr.ReadUInt16();
                        int bOff = pBr.ReadUInt16();
                        int fLen = pBr.ReadByte();
                        byte[] startVal = pBr.ReadBytes(fLen);
                        byte[] delta = pBr.ReadBytes(fLen);
                        for (int ci = cStart; ci <= cEnd; ci++)
                        {
                            int step = ci - cStart;
                            int bufOff = ci * v6ChunkSize + bOff;
                            for (int f = 0; f < fLen && bufOff + f < baseBuffer.Length; f++)
                                baseBuffer[bufOff + f] = (byte)(startVal[f] + step * delta[f]);
                        }
                        break;
                    }
                    case 0x03: // LITERAL_PATCH
                    {
                        int cIdx = pBr.ReadUInt16();
                        int bOff = pBr.ReadUInt16();
                        int pLen = pBr.ReadUInt16();
                        byte[] vals = pBr.ReadBytes(pLen);
                        int bufOff = cIdx * v6ChunkSize + bOff;
                        for (int f = 0; f < pLen && bufOff + f < baseBuffer.Length; f++)
                            baseBuffer[bufOff + f] = vals[f];
                        break;
                    }
                    default:
                        throw new InvalidDataException($"v6.0.0: unknown opcode 0x{opcode:X2}");
                }
            }

            // Apply residual: v5.6.0-style bitmask encoding
            // Format: [modeBitfield] then per-errored-chunk [bitmask][values]
            int resModeBytes = (v6ChunkCount + 7) / 8;
            int resBmBytes = v6ChunkSize / 8;

            // Read mode bitfield
            int resBmIdx = resModeBytes; // bitmasks start after mode
            // Count residual chunks to locate values
            int resChunkCount = 0;
            for (int bi = 0; bi < resModeBytes && bi < rawRes.Length; bi++)
                resChunkCount += BitCount(rawRes[bi]);
            int resValStart = resModeBytes + resChunkCount * resBmBytes;
            int resValIdx = resValStart;

            for (int ci = 0; ci < v6ChunkCount; ci++)
            {
                bool hasResidual = (ci >> 3 < resModeBytes)
                    && ((rawRes[ci >> 3] >> (ci & 7)) & 1) != 0;
                if (!hasResidual) continue;

                int baseOff = ci * v6ChunkSize;
                for (int bytePos = 0; bytePos < resBmBytes && resBmIdx + bytePos < rawRes.Length; bytePos++)
                {
                    byte maskByte = rawRes[resBmIdx + bytePos];
                    if (maskByte == 0) continue;
                    for (int bit = 0; bit < 8; bit++)
                    {
                        if (((maskByte >> bit) & 1) == 1)
                        {
                            int p = bytePos * 8 + bit;
                            if (baseOff + p < baseBuffer.Length && resValIdx < rawRes.Length)
                                baseBuffer[baseOff + p] = rawRes[resValIdx++];
                        }
                    }
                }
                resBmIdx += resBmBytes;
            }
        }
        else if (isBitmaskOnlyPatch)
        {
            // v5.6.0: bitmask-only with direct byte replacement, 1-bit mode per chunk
            using var errMs = new MemoryStream(errorBytes, writable: false);
            using var errBr = new BinaryReader(errMs);
            int patchCount = errBr.ReadInt32();
            int originalSize = errBr.ReadInt32();
            if (originalSize != baseBuffer.Length)
                throw new InvalidDataException(
                    $"v5.6.0 originalSize {originalSize} does not match base buffer {baseBuffer.Length}.");
            int bmChunkCount = errBr.ReadInt32();
            int compModeLen = errBr.ReadInt32();
            int compBitmaskLen = errBr.ReadInt32();
            int compValLen = errBr.ReadInt32();

            int modeOffset = (int)errMs.Position;
            int bitmaskOffset = modeOffset + compModeLen;
            int valOffset = bitmaskOffset + compBitmaskLen;

            var dictForDecomp = Globals.EnableCcfStore
                ? CcfPackOptimizerService.LiveDictionary
                : null;

            byte[] modeStream, bitmaskStream, valStream;
            if (dictForDecomp != null)
            {
                try
                {
                    using var dd = new Decompressor();
                    dd.LoadDictionary(dictForDecomp);
                    modeStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                    bitmaskStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                    valStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
                }
                catch
                {
                    using var dd2 = new Decompressor();
                    modeStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                    bitmaskStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                    valStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
                }
            }
            else
            {
                using var d = new Decompressor();
                modeStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                bitmaskStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                valStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
            }

            int cs = Globals.chunkSize;
            int bitmaskBytes = cs / 8;
            int valIdx = 0;
            int bmIdx = 0;

            for (int ci = 0; ci < bmChunkCount; ci++)
            {
                bool hasErrors = (ci >> 3 < modeStream.Length)
                    && ((modeStream[ci >> 3] >> (ci & 7)) & 1) != 0;

                if (!hasErrors) continue;

                int baseOff = ci * cs;
                for (int bytePos = 0; bytePos < bitmaskBytes; bytePos++)
                {
                    byte maskByte = bitmaskStream[bmIdx + bytePos];
                    if (maskByte == 0) continue;
                    for (int bit = 0; bit < 8; bit++)
                    {
                        if (((maskByte >> bit) & 1) == 1)
                        {
                            int p = bytePos * 8 + bit;
                            if (baseOff + p < baseBuffer.Length)
                                baseBuffer[baseOff + p] = valStream[valIdx++];
                        }
                    }
                }
                bmIdx += bitmaskBytes;
            }
        }
        else if (isDirectPatch)
        {
            // v5.5.0: skip:value with direct byte replacement, 2-bit mode per chunk
            using var errMs = new MemoryStream(errorBytes, writable: false);
            using var errBr = new BinaryReader(errMs);
            int patchCount = errBr.ReadInt32();
            int originalSize = errBr.ReadInt32();
            if (originalSize != baseBuffer.Length)
                throw new InvalidDataException(
                    $"v5.5.0 originalSize {originalSize} does not match base buffer {baseBuffer.Length}.");
            int directChunkCount = errBr.ReadInt32();
            int compModeLen = errBr.ReadInt32();
            int compSkipLen = errBr.ReadInt32();
            int compBitmaskLen = errBr.ReadInt32();
            int compValLen = errBr.ReadInt32();

            int modeOffset = (int)errMs.Position;
            int skipOffset = modeOffset + compModeLen;
            int bitmaskOffset = skipOffset + compSkipLen;
            int valOffset = bitmaskOffset + compBitmaskLen;

            var dictForDecomp = Globals.EnableCcfStore
                ? CcfPackOptimizerService.LiveDictionary
                : null;

            byte[] modeStream, skipStream, bitmaskStream, valStream;
            if (dictForDecomp != null)
            {
                try
                {
                    using var dd = new Decompressor();
                    dd.LoadDictionary(dictForDecomp);
                    modeStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                    skipStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipOffset, compSkipLen)).ToArray();
                    bitmaskStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                    valStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
                }
                catch
                {
                    using var dd2 = new Decompressor();
                    modeStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                    skipStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipOffset, compSkipLen)).ToArray();
                    bitmaskStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                    valStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
                }
            }
            else
            {
                using var d = new Decompressor();
                modeStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                skipStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipOffset, compSkipLen)).ToArray();
                bitmaskStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                valStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
            }

            int cs = Globals.chunkSize;
            int bitmaskBytes = cs / 8;
            using var skipRdr = new MemoryStream(skipStream, writable: false);
            int valIdx = 0;
            int bmIdx = 0;

            for (int ci = 0; ci < directChunkCount; ci++)
            {
                int baseOff = ci * cs;
                int bitPos = ci * 2;
                int mode = (bitPos / 8 < modeStream.Length)
                    ? (modeStream[bitPos / 8] >> (bitPos % 8)) & 0x03
                    : 0;

                if (mode == 0)
                {
                    // Duplicate: reference chunk is correct, do nothing
                }
                else if (mode == 1)
                {
                    // Skip:value — direct byte replacement
                    int pos = 0;
                    while (pos < cs)
                    {
                        uint skipVal = ReadVarint(skipRdr);
                        pos += (int)skipVal;
                        if (pos >= cs) break;
                        if (baseOff + pos < baseBuffer.Length)
                            baseBuffer[baseOff + pos] = valStream[valIdx++];
                        pos++;
                    }
                }
                else if (mode == 2)
                {
                    // Bitmask: direct byte replacement
                    for (int bytePos = 0; bytePos < bitmaskBytes; bytePos++)
                    {
                        byte maskByte = bitmaskStream[bmIdx + bytePos];
                        if (maskByte == 0) continue;
                        for (int bit = 0; bit < 8; bit++)
                        {
                            if (((maskByte >> bit) & 1) == 1)
                            {
                                int p = bytePos * 8 + bit;
                                if (baseOff + p < baseBuffer.Length)
                                    baseBuffer[baseOff + p] = valStream[valIdx++];
                            }
                        }
                    }
                    bmIdx += bitmaskBytes;
                }
            }
        }
        else if (isFourStreamPatch)
        {
            // v5.4.0: four-stream with per-chunk bitmask/skip mode (XOR)
            using var errMs = new MemoryStream(errorBytes, writable: false);
            using var errBr = new BinaryReader(errMs);
            int patchCount = errBr.ReadInt32();
            int originalSize = errBr.ReadInt32();
            if (originalSize != baseBuffer.Length)
                throw new InvalidDataException(
                    $"v5.4.0 originalSize {originalSize} does not match base buffer {baseBuffer.Length}.");
            int fourStreamChunkCount = errBr.ReadInt32();
            int compModeLen = errBr.ReadInt32();
            int compSkipLen = errBr.ReadInt32();
            int compBitmaskLen = errBr.ReadInt32();
            int compValLen = errBr.ReadInt32();

            int modeOffset = (int)errMs.Position;
            int skipOffset = modeOffset + compModeLen;
            int bitmaskOffset = skipOffset + compSkipLen;
            int valOffset = bitmaskOffset + compBitmaskLen;

            var dictForDecomp = Globals.EnableCcfStore
                ? CcfPackOptimizerService.LiveDictionary
                : null;

            byte[] modeStream, skipStream, bitmaskStream, valStream;
            if (dictForDecomp != null)
            {
                try
                {
                    using var dd = new Decompressor();
                    dd.LoadDictionary(dictForDecomp);
                    modeStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                    skipStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipOffset, compSkipLen)).ToArray();
                    bitmaskStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                    valStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
                }
                catch
                {
                    using var dd2 = new Decompressor();
                    modeStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                    skipStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipOffset, compSkipLen)).ToArray();
                    bitmaskStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                    valStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
                }
            }
            else
            {
                using var d = new Decompressor();
                modeStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, modeOffset, compModeLen)).ToArray();
                skipStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipOffset, compSkipLen)).ToArray();
                bitmaskStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, bitmaskOffset, compBitmaskLen)).ToArray();
                valStream = d.Unwrap(new ReadOnlySpan<byte>(errorBytes, valOffset, compValLen)).ToArray();
            }

            int cs = Globals.chunkSize;
            int bitmaskBytes = cs / 8;
            using var skipMs = new MemoryStream(skipStream, writable: false);
            int valIdx = 0;
            int bitmaskIdx = 0;

            for (int ci = 0; ci < fourStreamChunkCount; ci++)
            {
                int baseOff = ci * cs;
                bool isBitmask = (ci / 8 < modeStream.Length) && ((modeStream[ci / 8] >> (ci % 8)) & 1) == 1;

                if (isBitmask)
                {
                    for (int bytePos = 0; bytePos < bitmaskBytes; bytePos++)
                    {
                        byte maskByte = bitmaskStream[bitmaskIdx + bytePos];
                        if (maskByte == 0) continue;
                        for (int bit = 0; bit < 8; bit++)
                        {
                            if (((maskByte >> bit) & 1) == 1)
                            {
                                int pos = bytePos * 8 + bit;
                                if (baseOff + pos < baseBuffer.Length)
                                    baseBuffer[baseOff + pos] ^= valStream[valIdx++];
                            }
                        }
                    }
                    bitmaskIdx += bitmaskBytes;
                }
                else
                {
                    int pos = 0;
                    while (pos < cs)
                    {
                        uint skipVal = ReadVarint(skipMs);
                        pos += (int)skipVal;
                        if (pos >= cs) break;
                        if (baseOff + pos < baseBuffer.Length)
                            baseBuffer[baseOff + pos] ^= valStream[valIdx++];
                        pos++;
                    }
                }
            }
        }
        else if (isSplitStreamPatch)
        {
            // v5.3.0: split-stream — two independently Zstd'd sub-streams
            using var errMs = new MemoryStream(errorBytes, writable: false);
            using var errBr = new BinaryReader(errMs);
            int patchCount = errBr.ReadInt32();
            int originalSize = errBr.ReadInt32();
            if (originalSize != baseBuffer.Length)
                throw new InvalidDataException(
                    $"v5.3.0 originalSize {originalSize} does not match base buffer {baseBuffer.Length}.");
            int compSkipLen = errBr.ReadInt32();
            int compValLen = errBr.ReadInt32();

            int skipDataOffset = (int)errMs.Position;
            int valDataOffset = skipDataOffset + compSkipLen;

            byte[] skipStream, valStream;
            var dictForDecomp = Globals.EnableCcfStore
                ? CcfPackOptimizerService.LiveDictionary
                : null;

            if (dictForDecomp != null)
            {
                try
                {
                    using var dd = new Decompressor();
                    dd.LoadDictionary(dictForDecomp);
                    skipStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipDataOffset, compSkipLen)).ToArray();
                    valStream = dd.Unwrap(new ReadOnlySpan<byte>(errorBytes, valDataOffset, compValLen)).ToArray();
                }
                catch
                {
                    using var dd2 = new Decompressor();
                    skipStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipDataOffset, compSkipLen)).ToArray();
                    valStream = dd2.Unwrap(new ReadOnlySpan<byte>(errorBytes, valDataOffset, compValLen)).ToArray();
                }
            }
            else
            {
                using var decompressor = new Decompressor();
                skipStream = decompressor.Unwrap(new ReadOnlySpan<byte>(errorBytes, skipDataOffset, compSkipLen)).ToArray();
                valStream = decompressor.Unwrap(new ReadOnlySpan<byte>(errorBytes, valDataOffset, compValLen)).ToArray();
            }

            using var skipMs = new MemoryStream(skipStream, writable: false);
            int cursor = 0;
            for (int p = 0; p < patchCount; p++)
            {
                uint skip = ReadVarint(skipMs);
                byte xorByte = valStream[p];
                cursor += (int)skip;
                if (cursor < 0 || cursor >= baseBuffer.Length)
                    throw new InvalidDataException(
                        $"v5.3.0 patch {p}/{patchCount}: cursor {cursor} out of range (buffer {baseBuffer.Length}, skip {skip}).");
                baseBuffer[cursor] ^= xorByte;
                cursor++;
            }
        }
        else if (isRunXorPatch)
        {
            // v5.2.0: varint(skip) + byte(xor) pairs
            var patchStream = new MemoryStream(errorBytes, writable: false);
            using var patchReader = new BinaryReader(patchStream);
            int patchCount = patchReader.ReadInt32();
            int originalSize = patchReader.ReadInt32();
            if (originalSize != baseBuffer.Length)
                throw new InvalidDataException(
                    $"v5.2.0 originalSize {originalSize} does not match base buffer {baseBuffer.Length}.");
            int cursor = 0;
            for (int p = 0; p < patchCount; p++)
            {
                uint skip = ReadVarint(patchStream);
                byte xorByte = (byte)patchStream.ReadByte();
                cursor += (int)skip;
                if (cursor < 0 || cursor >= baseBuffer.Length)
                    throw new InvalidDataException(
                        $"v5.2.0 patch {p}/{patchCount}: cursor {cursor} out of range (buffer {baseBuffer.Length}, skip {skip}).");
                baseBuffer[cursor] ^= xorByte;
                cursor++;
            }
        }
        else if (isSkipXorPatch)
        {
            // v5.1.0: (skip, xor_byte) pairs — walk the base buffer in-place
            using var patchReader = new BinaryReader(new MemoryStream(errorBytes, writable: false));
            int patchCount = patchReader.ReadInt32();
            int originalSize = patchReader.ReadInt32();
            if (originalSize != baseBuffer.Length)
                throw new InvalidDataException(
                    $"v5.1.0 originalSize {originalSize} does not match base buffer {baseBuffer.Length}.");
            int cursor = 0;
            for (int p = 0; p < patchCount; p++)
            {
                uint skip = patchReader.ReadUInt32();
                byte xorByte = patchReader.ReadByte();
                cursor += (int)skip;
                if (cursor < 0 || cursor >= baseBuffer.Length)
                    throw new InvalidDataException(
                        $"v5.1.0 patch cursor {cursor} out of range (buffer size {baseBuffer.Length}, patch {p}/{patchCount}, skip {skip}).");
                baseBuffer[cursor] ^= xorByte;
                cursor++;
            }
        }
        else if (isXorPatch)
        {
            // v5.0.0: flat XOR buffer
            if (errorBytes.Length != baseBuffer.Length)
                throw new InvalidDataException(
                    $"XOR patch size {errorBytes.Length} does not match base buffer {baseBuffer.Length}.");
            for (int i = 0; i < baseBuffer.Length; i++)
                baseBuffer[i] = (byte)(baseBuffer[i] ^ errorBytes[i]);
        }
        else
        {
            // v3.x: arithmetic RLE patches
            int cursor = 0;
            foreach (var (startPos, runLength, diffValue) in patches!)
            {
                cursor += startPos;
                if (cursor < 0 || cursor >= baseBuffer.Length)
                    throw new InvalidDataException(
                        $"Patch cursor {cursor} out of range (buffer size {baseBuffer.Length}).");
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
                cursor += runLength;
            }
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

                // Surface to control center so we can correlate with the
                // compression-side integrity checks above. The block index /
                // count are forwarded to the dashboard so a single bad block
                // doesn't get lost in a 16-block file.
                int refsResolved = 0;
                if (refBucketIds != null)
                {
                    for (int rr = 0; rr < refBucketIds.Length; rr++)
                        if (refBucketIds[rr] != 0 && refBucketIds[rr] != ulong.MaxValue) refsResolved++;
                }
                global::Cross.Services.JobEvents.IntegrityDiagnostics.EmitDecompressFailure(
                    blockIndex: 0,
                    blockCount: 1,
                    expectedHash: expectedHash,
                    actualHash: actualHash,
                    chunkCount: chunkCount,
                    refsResolved: refsResolved);

                // Smoketest path: return the bad bytes so the caller can
                // byte-diff them against the original and attribute the
                // failure to a specific chunk + encode case.
                if (!_smoketestSuppressIntegrityThrow.Value)
                {
                    throw new InvalidDataException(
                        "Decompression integrity check failed: SHA256 hash of reconstructed file does not match the original. " +
                        "This means at least one base chunk was corrupted or missing.");
                }
            }
            Console.WriteLine($"[Decompress] ✅ SHA256 integrity verified");
        }

        applySw.Stop();
        Observability.RecordStage("ApplyPatchAndEgress", applySw.Elapsed.TotalMilliseconds,
            ("chunk_count", chunkCount), ("patches", patches?.Length ?? 0));

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
