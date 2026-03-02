namespace Cross.Models;

/// <summary>
/// Level 2 Mosaic Dedup metadata for a single chunk.
/// Describes how to reconstruct a mosaic base from multiple donor chunks'
/// sub-regions. The mosaic base is used as the diff target for error encoding,
/// producing smaller diffs than the default zeros base for high-entropy chunks.
/// </summary>
internal class MosaicChunkInfo
{
    /// <summary>Unique donor chunk references (BucketId + StorageGuid). Max 15 donors.</summary>
    public List<(ulong BucketId, string StorageGuid)> Donors = new();

    /// <summary>64-bit bitmap: bit i = 1 means sub-chunk i has a donor match, 0 = zeros base.</summary>
    public ulong MatchBitmap;

    /// <summary>32 bytes: 64 x 4-bit indices into Donors list. Nibble i = donor index for sub-chunk i.</summary>
    public byte[] Selectors = new byte[32];

    /// <summary>The assembled mosaic base chunk (chunkSize bytes).</summary>
    public byte[] StitchedBase = Array.Empty<byte>();

    public static int GetSelector(byte[] selectors, int elementIndex)
    {
        int byteIdx = elementIndex / 2;
        return (elementIndex % 2 == 0)
            ? selectors[byteIdx] & 0x0F
            : (selectors[byteIdx] >> 4) & 0x0F;
    }

    public static void SetSelector(byte[] selectors, int elementIndex, int donorIndex)
    {
        int byteIdx = elementIndex / 2;
        if (elementIndex % 2 == 0)
            selectors[byteIdx] = (byte)((selectors[byteIdx] & 0xF0) | (donorIndex & 0x0F));
        else
            selectors[byteIdx] = (byte)((selectors[byteIdx] & 0x0F) | ((donorIndex & 0x0F) << 4));
    }
}
