using System.Buffers.Binary;
using System.Security.Cryptography;

namespace Cross.Services.CcfStore;

internal static class CcfFamilyKey
{
    public static string Build(string version, byte[] refs)
    {
        // Prefer overlap-aware key for v5 compact refs; fallback to exact hash for older versions.
        if (version.StartsWith("v5.", StringComparison.Ordinal) &&
            TryExtractV5RefBucketIds(refs, out var buckets) &&
            buckets.Length > 0)
        {
            ulong m0 = MinHash(buckets, 0x9E3779B185EBCA87UL);
            ulong m1 = MinHash(buckets, 0xC2B2AE3D27D4EB4FUL);
            ulong m2 = MinHash(buckets, 0x165667B19E3779F9UL);
            int countBin = CountBin(buckets.Length);
            return $"v5ov:{countBin}:{m0:x16}:{m1:x16}:{m2:x16}";
        }

        return $"exact:{Convert.ToHexString(SHA256.HashData(refs)).ToLowerInvariant()}";
    }

    private static int CountBin(int count)
    {
        if (count <= 8) return 8;
        if (count <= 16) return 16;
        if (count <= 32) return 32;
        if (count <= 64) return 64;
        if (count <= 128) return 128;
        if (count <= 256) return 256;
        return 512;
    }

    private static ulong MinHash(ulong[] values, ulong seed)
    {
        ulong min = ulong.MaxValue;
        for (int i = 0; i < values.Length; i++)
        {
            ulong h = Mix64(values[i] ^ seed);
            if (h < min) min = h;
        }
        return min;
    }

    private static ulong Mix64(ulong x)
    {
        x += 0x9E3779B97F4A7C15UL;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9UL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBUL;
        return x ^ (x >> 31);
    }

    private static bool TryExtractV5RefBucketIds(byte[] refs, out ulong[] bucketIds)
    {
        bucketIds = Array.Empty<ulong>();
        try
        {
            if (refs.Length < 6) return false;
            ReadOnlySpan<byte> span = refs;

            // Layout (v5 refs): [int chunkCount][ushort refTableSize][refTable entries...]
            int chunkCount = BinaryPrimitives.ReadInt32LittleEndian(span[..4]);
            if (chunkCount <= 0) return false;
            int tableSize = BinaryPrimitives.ReadUInt16LittleEndian(span.Slice(4, 2));
            if (tableSize <= 0) return false;

            int headerBytes = 6;
            int tableBytes = checked(tableSize * 16); // each table entry = bucketId(8) + bucketIndex(8)
            if (headerBytes + tableBytes > refs.Length) return false;

            var set = new HashSet<ulong>();
            int off = headerBytes;
            for (int i = 0; i < tableSize; i++)
            {
                ulong bucketId = BinaryPrimitives.ReadUInt64LittleEndian(span.Slice(off, 8));
                off += 16;
                if (bucketId != 0) set.Add(bucketId);
            }

            if (set.Count == 0) return false;
            bucketIds = set.ToArray();
            Array.Sort(bucketIds);
            return true;
        }
        catch
        {
            return false;
        }
    }
}
