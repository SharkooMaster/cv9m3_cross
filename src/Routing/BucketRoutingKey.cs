namespace Cross.Routing;

/// <summary>
/// Bidirectional helper for the deterministic bitstring ↔ ulong mapping
/// used by both the agent's bucket store and cross's replication ring.
/// Lifted from <c>RocksDbBucketStorage.BitstringToUlong</c> so cross can
/// route chunks to replicas without needing to talk to the agent first.
///
/// Why this matters for replication:
///   The replication ring needs the SAME routing key on the write side
///   (cross encoder, has the bitstring) and the read side (cross decoder,
///   has the bucketId from the CCF ref). Because BitstringToUlong is
///   bijective on 64-char "0"/"1" strings, we can derive the routing key
///   from either form. Using the bucketId-as-string is the choice with
///   the smallest hash key (8 bytes → ~17 chars in decimal vs 64 chars
///   for the raw bitstring) and a minor speedup on the hot path.
///
/// Same-bitstring chunks → same bucketId → same routing key → same R
/// replicas. That's what keeps bucket affinity intact under replication
/// (every chunk for bucket B lives on the same R agents, so the agent
/// owning the bucket index has all of B's chunks indexed locally).
/// </summary>
public static class BucketRoutingKey
{
    /// <summary>
    /// Pack a 64-char "0"/"1" bitstring into a ulong, LSB-first
    /// (matching agent's <c>BitstringToUlong</c>).
    /// </summary>
    public static ulong BitstringToUlong(ReadOnlySpan<char> bitstring)
    {
        ulong result = 0;
        int len = Math.Min(64, bitstring.Length);
        for (int i = 0; i < len; i++)
            if (bitstring[i] == '1') result |= (1UL << i);
        return result;
    }

    public static ulong BitstringToUlong(string bitstring) =>
        BitstringToUlong(bitstring.AsSpan());

    /// <summary>
    /// Routing key derived from a 64-char bitstring. Cross encoder uses
    /// this on the write side; the value is keyed on the consistent-hash
    /// ring to pick R replicas.
    /// </summary>
    public static string FromBitstring(string bitstring)
    {
        return BitstringToUlong(bitstring).ToString("x16");
    }

    /// <summary>
    /// Routing key derived from a bucketId (already a packed bitstring
    /// ulong). Cross decoder uses this on the read side: the CCF ref
    /// stores bucketId, decoder asks "which replicas have this bucket?".
    /// Returns the same key as <see cref="FromBitstring"/> for the same
    /// underlying bitstring.
    /// </summary>
    public static string FromBucketId(ulong bucketId)
    {
        return bucketId.ToString("x16");
    }
}
