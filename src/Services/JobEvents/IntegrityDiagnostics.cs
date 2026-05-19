using System.Security.Cryptography;
using System.Text;

namespace Cross.Services.JobEvents;

/// <summary>
/// Integrity assertions executed at well-defined points of the compression
/// pipeline.  The decompress-side check already throws when a per-block SHA256
/// mismatch lands, but by then the run is already lost and we have no
/// visibility into <i>where</i> the corruption was introduced.
///
/// This helper closes that gap: cheap content-vs-StorageGuid round-trip checks
/// during compression are emitted as structured <c>IntegrityCheck:*</c> stage
/// events.  The dashboard reads those events and surfaces aggregate counts
/// next to the existing KPIs.  Any mismatch is also written to stdout with the
/// loud <c>[Integrity] ❌</c> prefix so it's grep-able from <c>kubectl logs</c>.
///
/// Gated behind <c>INTEGRITY_DIAGNOSTICS</c> (default <c>true</c>).
/// </summary>
public static class IntegrityDiagnostics
{
    public static readonly bool Enabled =
        (Environment.GetEnvironmentVariable("INTEGRITY_DIAGNOSTICS") ?? "true")
            .Equals("true", StringComparison.OrdinalIgnoreCase);

    public readonly record struct Result(int Checked, int Mismatches, string Detail);

    /// <summary>
    /// Generic content-vs-GUID check.  For each <paramref name="count"/> index
    /// the caller exposes <c>(expectedStorageGuid, chunkBytes)</c>. Indices
    /// where either is empty are skipped.
    /// </summary>
    public static Result VerifyHashes(int count, Func<int, (string? Guid, byte[]? Bytes)> probe)
    {
        if (!Enabled) return new Result(0, 0, "disabled");

        int total = 0;
        int mismatches = 0;
        string? firstMismatch = null;
        int sampleSize = 0;
        // Track the first-byte distribution of mismatched chunks so the operator
        // can tell at a glance whether they're truncated, zero-filled, etc.

        for (int i = 0; i < count; i++)
        {
            var (guid, bytes) = probe(i);
            if (string.IsNullOrEmpty(guid) || bytes == null || bytes.Length == 0) continue;
            total++;
            string actual = HashHex(bytes);
            if (!string.Equals(actual, guid, StringComparison.OrdinalIgnoreCase))
            {
                mismatches++;
                if (firstMismatch == null)
                {
                    sampleSize = bytes.Length;
                    string head = BytesHead(bytes, 8);
                    firstMismatch =
                        $"idx={i} exp={Take(guid, 16)} got={Take(actual, 16)} {bytes.Length}B head[{head}]";
                }
            }
        }

        return new Result(total, mismatches, firstMismatch ?? $"ok ({total} verified, {sampleSize}B)");
    }

    /// <summary>
    /// Emit an <c>IntegrityCheck:{stage}</c> StageDone event.  Always emits
    /// when the diagnostic ran (even on success) so the dashboard can show
    /// running totals.  On mismatch this also writes a loud line to stdout.
    /// </summary>
    public static void Emit(string stage, Result r, double elapsedMs)
    {
        if (!Enabled || r.Checked == 0) return;
        JobEventBus.EmitStageDone(
            "IntegrityCheck:" + stage,
            elapsedMs,
            chunkCount: r.Checked,
            bucketCount: r.Mismatches,
            bytes: 0);
        if (r.Mismatches > 0)
            Console.WriteLine($"[Integrity] ❌ {stage}: {r.Mismatches}/{r.Checked} mismatch — {r.Detail}");
        else
            Console.WriteLine($"[Integrity] ✓ {stage}: {r.Checked} verified ({r.Detail}) in {elapsedMs:F1}ms");
    }

    /// <summary>
    /// Emit a per-block decompression integrity failure with detail.  Tagged
    /// as <c>FAILED</c> with <c>error_class = "INTEGRITY_DECOMPRESS"</c>.
    /// </summary>
    public static void EmitDecompressFailure(
        int blockIndex, int blockCount, byte[] expectedHash, byte[] actualHash,
        int chunkCount, int refsResolved)
    {
        if (!Enabled) return;
        var jobId = JobEventBus.CurrentJobId ?? "no-job";
        var msg =
            $"block {blockIndex + 1}/{blockCount}: " +
            $"exp={Take(ToHex(expectedHash), 16)} got={Take(ToHex(actualHash), 16)} " +
            $"chunks={chunkCount} refs={refsResolved}";
        JobEventBus.EmitFailed(jobId, "INTEGRITY_DECOMPRESS", msg, "Decompress");
        Console.WriteLine($"[Integrity] ❌ DECOMPRESS {msg}");
    }

    private static string HashHex(byte[] data)
    {
        Span<byte> hash = stackalloc byte[32];
        SHA256.HashData(data, hash);
        return ToHex(hash);
    }

    private static string ToHex(ReadOnlySpan<byte> data)
    {
        var sb = new StringBuilder(data.Length * 2);
        for (int i = 0; i < data.Length; i++) sb.Append(data[i].ToString("x2"));
        return sb.ToString();
    }

    private static string Take(string s, int n) => s.Length <= n ? s : s.Substring(0, n);

    private static string BytesHead(byte[] data, int n)
    {
        int take = Math.Min(n, data.Length);
        var sb = new StringBuilder(take * 2 + 2);
        for (int i = 0; i < take; i++) sb.Append(data[i].ToString("x2"));
        if (data.Length > take) sb.Append("..");
        return sb.ToString();
    }
}
