using Google.Protobuf;

namespace Cross.Models;

/// <summary>
/// Single source of truth for the "what is this chunk's diff basis?" contract.
/// Replaces the legacy triple {sorted[i].Chunk, sorted[i].(BucketId,BucketKey,
/// StorageGuid,TargetAgent), mosaicInfos[i]} that was maintained by convention
/// across ~20 mutation sites in Cross.cs. Every encode case is a single value
/// here, and the encoder + decoder + smoketest all read from this one value —
/// they cannot disagree.
///
/// Invariant (enforced by construction):
///   encoder.basisBytes ≡ decoder.recoveredBaseBytes
/// where:
///   encoder.basisBytes      = <see cref="ChunkEncodeBaseExt.GetBasisBytes"/>
///   decoder.recoveredBytes  = bytes the v5 ref byte directs the decoder to
///                             produce (fetch by (BucketId,BucketKey), copy
///                             from rep, restitch from mosaic donors, etc.)
/// </summary>
internal abstract record ChunkEncodeBase
{
    /// <summary>
    /// No base. Encoder diffs source bytes against an all-zero buffer.
    /// V5 ref byte: 0x00. Decoder applies the diff on top of zeros.
    /// </summary>
    public sealed record Zeros : ChunkEncodeBase
    {
        public static readonly Zeros Instance = new();
        private Zeros() { }
    }

    /// <summary>
    /// This chunk's own bytes are being (or have been) stored at
    /// (BucketId, BucketKey) on TargetAgent with StorageGuid == SHA256(fileChunks[i]).
    /// Encoder basis IS the source bytes → diff is trivially zero.
    /// V5 ref byte: 0x02. Decoder fetches by ref and the empty diff yields the
    /// original bytes.
    /// </summary>
    public sealed record SelfFresh(
        ulong BucketId,
        ulong BucketKey,
        string StorageGuid,
        string TargetAgent) : ChunkEncodeBase;

    /// <summary>
    /// Non-representative cluster member whose representative is <see cref="SelfFresh"/>.
    /// Encoder basis is fileChunks[<see cref="RepIndex"/>] (the rep's original bytes,
    /// which are guaranteed to live at (BucketId, BucketKey) on TargetAgent because the
    /// rep is fresh-stored). Decoder fetches the same bucket and applies the diff.
    /// V5 ref byte: 0x01.
    /// </summary>
    public sealed record RepFresh(
        ulong BucketId,
        ulong BucketKey,
        string StorageGuid,
        string TargetAgent,
        int RepIndex) : ChunkEncodeBase;

    /// <summary>
    /// Similarity match against an existing stored chunk at (BucketId, BucketKey).
    /// Encoder basis is <see cref="BaseBytes"/>; decoder fetches the same bucket from
    /// TargetAgent and applies the diff.
    /// V5 ref byte: 0x01.
    /// INVARIANT: SHA256(BaseBytes) == StorageGuid (verified by the StoreRoundTrip /
    /// FetchedBases integrity checks).
    /// </summary>
    public sealed record Ref(
        ulong BucketId,
        ulong BucketKey,
        string StorageGuid,
        string TargetAgent,
        ByteString BaseBytes) : ChunkEncodeBase;

    /// <summary>
    /// Mosaic-assembled basis from multiple donor sub-regions or a byte-level pair
    /// merge. Encoder basis is <see cref="MosaicChunkInfo.StitchedBase"/>; decoder
    /// re-assembles the same bytes from <see cref="MosaicChunkInfo.Donors"/> and
    /// applies the diff. No single-bucket fetch is involved.
    /// V5 ref byte: 0x04 (sub-chunk mosaic) or 0x05 (byte-level pair merge).
    /// </summary>
    public sealed record Mosaic(MosaicChunkInfo Info) : ChunkEncodeBase;
}

internal static class ChunkEncodeBaseExt
{
    /// <summary>
    /// V5 reference byte for this encode base. Drives <c>BuildV5References</c>.
    /// </summary>
    public static byte RefByte(this ChunkEncodeBase b) => b switch
    {
        ChunkEncodeBase.Zeros => (byte)0x00,
        ChunkEncodeBase.SelfFresh => (byte)0x02,
        ChunkEncodeBase.RepFresh => (byte)0x01,
        ChunkEncodeBase.Ref => (byte)0x01,
        ChunkEncodeBase.Mosaic m => m.Info.IsByteMerge ? (byte)0x05 : (byte)0x04,
        _ => throw new System.InvalidOperationException($"Unknown ChunkEncodeBase variant: {b.GetType().Name}")
    };

    public static ulong BucketId(this ChunkEncodeBase b) => b switch
    {
        ChunkEncodeBase.SelfFresh s => s.BucketId,
        ChunkEncodeBase.RepFresh r => r.BucketId,
        ChunkEncodeBase.Ref r => r.BucketId,
        _ => 0UL
    };

    public static ulong BucketKey(this ChunkEncodeBase b) => b switch
    {
        ChunkEncodeBase.SelfFresh s => s.BucketKey,
        ChunkEncodeBase.RepFresh r => r.BucketKey,
        ChunkEncodeBase.Ref r => r.BucketKey,
        _ => 0UL
    };

    public static string StorageGuid(this ChunkEncodeBase b) => b switch
    {
        ChunkEncodeBase.SelfFresh s => s.StorageGuid,
        ChunkEncodeBase.RepFresh r => r.StorageGuid,
        ChunkEncodeBase.Ref r => r.StorageGuid,
        _ => string.Empty
    };

    public static string TargetAgent(this ChunkEncodeBase b) => b switch
    {
        ChunkEncodeBase.SelfFresh s => s.TargetAgent,
        ChunkEncodeBase.RepFresh r => r.TargetAgent,
        ChunkEncodeBase.Ref r => r.TargetAgent,
        _ => string.Empty
    };

    /// <summary>
    /// Returns the exact byte array the encoder must subtract from
    /// <paramref name="fileChunks"/>[<paramref name="chunkIdx"/>] to produce the diff.
    /// The returned buffer is at least <paramref name="chunkSize"/> bytes; callers
    /// should only inspect the first <paramref name="chunkSize"/> bytes.
    ///
    /// May allocate (Ref: ByteString.ToByteArray). Zeros / SelfFresh / RepFresh
    /// return references into existing buffers; Mosaic returns the stitched buffer.
    /// </summary>
    public static byte[] GetBasisBytes(
        this ChunkEncodeBase b,
        int chunkIdx,
        System.Collections.Generic.IReadOnlyList<byte[]> fileChunks,
        int chunkSize) => b switch
        {
            ChunkEncodeBase.Zeros => new byte[chunkSize],
            ChunkEncodeBase.SelfFresh => fileChunks[chunkIdx],
            ChunkEncodeBase.RepFresh r => fileChunks[r.RepIndex],
            ChunkEncodeBase.Ref r => r.BaseBytes.ToByteArray(),
            ChunkEncodeBase.Mosaic m => m.Info.StitchedBase,
            _ => throw new System.InvalidOperationException($"Unknown ChunkEncodeBase variant: {b.GetType().Name}")
        };

    /// <summary>
    /// Short human-readable label matching the legacy smoketest encodeCase strings
    /// so existing dashboards / log greps keep working.
    /// </summary>
    public static string CaseLabel(this ChunkEncodeBase b) => b switch
    {
        ChunkEncodeBase.Mosaic => "mosaic",
        ChunkEncodeBase.SelfFresh => "self-fresh",
        ChunkEncodeBase.RepFresh r => $"rep[{r.RepIndex}]-fresh",
        ChunkEncodeBase.Ref => "dedup-cached",
        ChunkEncodeBase.Zeros => "empty-base",
        _ => "unknown"
    };
}
