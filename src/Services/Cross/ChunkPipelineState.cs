using GatewayService;
using Google.Protobuf;
using Cross.Models;

namespace Cross.Services.Cross;

/// <summary>
/// Per-window pipeline state. Owns the <see cref="ChunkEncodeBase"/> source of truth
/// for every chunk and is the ONLY API allowed to mutate the encode/decode contract.
///
/// The legacy <see cref="QueryResponseObject"/>[] array is kept alongside as a wire
/// DTO mirror: <see cref="SetEncodeBase"/> projects every relevant field onto it so
/// code paths that still read from <c>sorted[i].BucketId</c> / <c>.BucketKey</c> /
/// <c>.StorageGuid</c> / <c>.TargetAgent</c> / <c>.Chunk</c> for agent routing,
/// store-groups iteration, or statistics keep observing values that are guaranteed
/// consistent with <c>encodeBases[i]</c>.
///
/// Forbidden: any code path that writes to one of those five fields directly. Doing
/// so reintroduces the bug class this type exists to eliminate (encoder and decoder
/// disagreeing on the diff basis). The only acceptable mutation entry point is
/// <see cref="SetEncodeBase"/>.
/// </summary>
internal sealed class ChunkPipelineState
{
    public QueryResponseObject?[] Sorted { get; }
    public ChunkEncodeBase[] EncodeBases { get; }
    public int Length => Sorted.Length;

    // Per-chunk lock used to serialize writers competing for the same chunk
    // index from multiple parallel tasks (e.g. the per-agent BatchGet workers
    // in CompressFileWithStats merging search responses for the same chunk).
    // Without this, the (Sorted[i], EncodeBases[i]) pair could be torn between
    // two responses — encoder reads EncodeBases[i] from one response and
    // sorted[i].TargetAgent from a different one, sending bytes to the wrong
    // agent. The lock makes "decide if this is better" + "adopt as best" a
    // single atomic step per chunk; locks for different chunks don't interact,
    // so search merge stays fully parallel across chunks.
    private readonly object[] _locks;

    /// <summary>
    /// Per-chunk lock object. Callers competing for the same chunk index MUST
    /// take this lock around the read-then-write sequence (e.g. compare-against-
    /// best-similarity then <see cref="AdoptSearchResponse"/>).
    /// </summary>
    public object LockFor(int i) => _locks[i];

    public ChunkPipelineState(int length)
    {
        Sorted = new QueryResponseObject?[length];
        EncodeBases = new ChunkEncodeBase[length];
        _locks = new object[length];
        for (int i = 0; i < length; i++)
        {
            EncodeBases[i] = ChunkEncodeBase.Zeros.Instance;
            _locks[i] = new object();
        }
    }

    /// <summary>
    /// Atomically set the encode contract for chunk <paramref name="i"/> and project
    /// the relevant wire-DTO fields onto <c>Sorted[i]</c>. <c>Sorted[i]</c> is created
    /// lazily if it does not yet exist.
    ///
    /// Projection rules (per encode-base variant):
    /// <list type="bullet">
    ///   <item><c>Zeros</c> — no projection. Preserves whatever the search phase put
    ///         into <c>Sorted[i]</c> (TargetAgent for fresh-store routing, etc.).</item>
    ///   <item><c>SelfFresh</c> / <c>RepFresh</c> — projects all five wire fields:
    ///         BucketId, BucketKey, StorageGuid, TargetAgent, Chunk = Empty.</item>
    ///   <item><c>Ref</c> — projects all five: BucketId, BucketKey, StorageGuid,
    ///         TargetAgent, Chunk = BaseBytes.</item>
    ///   <item><c>Mosaic</c> — projects ONLY <c>Chunk</c> = StitchedBase. Does NOT
    ///         touch BucketId/BucketKey/StorageGuid/TargetAgent because those slots
    ///         are reserved for ambient-storage bookkeeping (e.g. when Mosaic-L2
    ///         Assembly stitches a base for a chunk the pipeline still wants to
    ///         fresh-store on TargetAgent so future similar chunks can find it as
    ///         an L1 source). The decode contract for Mosaic is donor-driven and
    ///         independent of any single bucket — those wire-DTO bucket fields
    ///         are pure side-channel bookkeeping.</item>
    /// </list>
    ///
    /// Pipeline-flow flags on <c>Sorted[i]</c> (NeedToStore, Similarity, Duplicate)
    /// are never touched — they remain the caller's responsibility.
    /// </summary>
    public void SetEncodeBase(int i, ChunkEncodeBase newBase)
    {
        EncodeBases[i] = newBase;
        var s = Sorted[i];
        if (s == null)
        {
            s = new QueryResponseObject { Index = i, Chunk = ByteString.Empty };
            Sorted[i] = s;
        }
        switch (newBase)
        {
            case ChunkEncodeBase.SelfFresh sf:
                s.BucketId = sf.BucketId;
                s.BucketKey = sf.BucketKey;
                s.StorageGuid = sf.StorageGuid;
                s.TargetAgent = sf.TargetAgent;
                s.Chunk = ByteString.Empty;
                s.IsMatched = true;
                break;
            case ChunkEncodeBase.RepFresh rf:
                s.BucketId = rf.BucketId;
                s.BucketKey = rf.BucketKey;
                s.StorageGuid = rf.StorageGuid;
                s.TargetAgent = rf.TargetAgent;
                s.Chunk = ByteString.Empty;
                s.IsMatched = true;
                break;
            case ChunkEncodeBase.Ref r:
                s.BucketId = r.BucketId;
                s.BucketKey = r.BucketKey;
                s.StorageGuid = r.StorageGuid;
                s.TargetAgent = r.TargetAgent;
                s.Chunk = r.BaseBytes;
                s.IsMatched = true;
                break;
            case ChunkEncodeBase.Mosaic m:
                s.Chunk = ByteString.CopyFrom(m.Info.StitchedBase);
                break;
            case ChunkEncodeBase.Zeros:
                break;
            default:
                throw new System.InvalidOperationException(
                    $"Unknown ChunkEncodeBase variant: {newBase.GetType().Name}");
        }
    }

    /// <summary>
    /// Record an ambient fresh-store on <c>Sorted[i]</c> without touching the encode
    /// contract. Used for chunks that have an EncodeBase the decoder doesn't fetch
    /// by single bucket (today: Mosaic) but whose own bytes are still being stored
    /// on an agent so future similar chunks can find them via L1 search.
    ///
    /// Updates BucketId/BucketKey/StorageGuid/TargetAgent on the wire DTO so the
    /// StoreRoundTrip integrity check (which verifies SHA256(sentBytes) ==
    /// returnedStorageGuid for NeedToStore=true rows) still works end-to-end.
    /// </summary>
    public void RecordAmbientFreshStore(int i, ulong bucketId, ulong bucketKey, string storageGuid, string targetAgent)
    {
        var s = Sorted[i];
        if (s == null) return;
        s.BucketId = bucketId;
        s.BucketKey = bucketKey;
        s.StorageGuid = storageGuid;
        s.TargetAgent = targetAgent;
        s.IsMatched = true;
    }

    /// <summary>
    /// Convenience: install <paramref name="response"/> as both the wire-level row
    /// for chunk <paramref name="i"/> and project an <see cref="ChunkEncodeBase.Ref"/>
    /// from its (BucketId, BucketKey, StorageGuid, TargetAgent, Chunk). Used by the
    /// search-response handlers where the gateway hands us an already-populated
    /// QueryResponseObject and we want to adopt it whole.
    ///
    /// Pipeline flags on <paramref name="response"/> (NeedToStore, Duplicate,
    /// Similarity, Index) are preserved; only the encode contract is normalised.
    /// </summary>
    public void AdoptSearchResponse(int i, QueryResponseObject response)
    {
        Sorted[i] = response;
        if (!response.IsMatched)
        {
            EncodeBases[i] = ChunkEncodeBase.Zeros.Instance;
        }
        else
        {
            EncodeBases[i] = new ChunkEncodeBase.Ref(
                response.BucketId,
                response.BucketKey,
                response.StorageGuid ?? string.Empty,
                response.TargetAgent ?? string.Empty,
                response.Chunk ?? ByteString.Empty);
        }
    }

    /// <summary>
    /// Mark a chunk for re-store on <paramref name="reStoreAgent"/>. The encode
    /// contract (EncodeBases[i]) is INTENTIONALLY left alone — it remains valid
    /// for any in-flight Mosaic-Fallback / Lane-Fallback that might rescue the
    /// chunk before BloatRestore runs. The downstream contract is:
    ///   * BloatRestore reads <c>Sorted[i].TargetAgent</c> to route the re-store.
    ///   * If a fallback rescues the chunk first it calls <see cref="SetEncodeBase"/>
    ///     with a Mosaic, which leaves TargetAgent untouched (Mosaic projection
    ///     preserves wire-DTO bucket fields).
    ///   * If no fallback rescues it, BloatRestore eventually calls
    ///     <see cref="SetEncodeBase"/> with a SelfFresh or Ref, atomically writing
    ///     the final encode contract and overwriting these routing fields with
    ///     the agent-returned bucket info.
    /// In other words: between this call and BloatRestore-or-fallback, the
    /// wire-DTO TargetAgent is a "where to send the bytes next" signal that is
    /// independent of (and orthogonal to) the encode contract, but the encoder
    /// itself never runs in that window so no inconsistency is observable.
    /// </summary>
    public void PrepareReStoreRouting(int i, string reStoreAgent)
    {
        var s = Sorted[i];
        if (s == null) return;
        s.NeedToStore = true;
        s.Similarity = 1.0f;
        s.TargetAgent = reStoreAgent;
    }
}
