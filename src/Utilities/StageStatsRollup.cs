using System.Collections.Concurrent;

namespace Cross.Utilities;

/// <summary>
/// Tiny rolling histogram per stage name. Each stage keeps a fixed-size ring of
/// the most recent N observations with their timestamps; percentiles are
/// computed on demand by snapshotting the ring, copying it to a scratch array,
/// sorting in place, and reading the requested quantile.
///
/// Design constraints:
///   - Hot-path Record() is allocation-free and lock-free (CAS on the ring
///     index). It is intentionally cheap so we can wire it into
///     <see cref="Observability.RecordStage"/> and let every existing stage
///     callsite contribute to the dashboard with no further changes.
///   - The /stats/runtime handler calls Snapshot() at scrape cadence (~30 s),
///     which is the only place that allocates (one scratch array per call).
///   - Memory is bounded at exactly <see cref="RingSize"/> doubles + 1 long
///     timestamp per stage × number-of-distinct-stage-names. With ~12 stages
///     and a 4096-deep ring: ~400 KB managed, fixed.
///
/// This is NOT a precise t-digest or HDR histogram. It's a small reservoir of
/// raw samples that's accurate enough for "is search 10× slower now than it
/// was an hour ago?" diagnostics without dragging in a histogram library.
/// </summary>
public static class StageStatsRollup
{
    /// <summary>
    /// Per-stage sample ring size. 4096 samples × at most ~10 stages-of-interest
    /// = ~320 KB managed. At 1000 stage emits/sec across the pod this still
    /// covers the most recent ~4 s of activity, which is fine: percentile
    /// drift over a 30 s scrape interval is dominated by the freshest samples.
    /// </summary>
    public const int RingSize = 4096;

    /// <summary>
    /// Samples older than this are excluded from percentile calculations even
    /// if the ring still holds them. Prevents a stale outlier from a 1-hour-ago
    /// burst from poisoning the live p99 reading after a quiet period.
    /// </summary>
    public static readonly TimeSpan WindowTtl = TimeSpan.FromMinutes(5);

    private static readonly ConcurrentDictionary<string, StageRing> _byStage = new(StringComparer.Ordinal);

    /// <summary>Hot-path: O(1), allocation-free. Safe from any thread.</summary>
    public static void Record(string stage, double durationMs)
    {
        if (string.IsNullOrEmpty(stage)) return;
        var ring = _byStage.GetOrAdd(stage, _ => new StageRing());
        ring.Record(durationMs);
    }

    /// <summary>Snapshot of every stage that has been recorded since startup.</summary>
    public static IReadOnlyList<StageSnapshot> SnapshotAll()
    {
        var nowTicks = DateTime.UtcNow.Ticks;
        var cutoffTicks = nowTicks - WindowTtl.Ticks;
        var result = new List<StageSnapshot>(_byStage.Count);
        foreach (var (stage, ring) in _byStage)
        {
            var snap = ring.Snapshot(stage, cutoffTicks);
            // Skip stages with no observations in the window AND no lifetime
            // observations: we still want to surface a stage that's been
            // silent for >5 min so the dashboard can flag the absence.
            if (snap.CountWindow > 0 || snap.TotalCount > 0) result.Add(snap);
        }
        return result;
    }

    private sealed class StageRing
    {
        private readonly double[] _values = new double[RingSize];
        private readonly long[] _ticks = new long[RingSize];
        private int _next;            // monotonically increasing index, mod RingSize
        private long _totalCount;     // total observations since process start

        public void Record(double durationMs)
        {
            // Single-CAS hot path: claim a slot and write into it. A racy reader
            // can observe an in-progress write (we don't try to make Snapshot
            // strictly serializable with Record) — for percentile sampling that's
            // an acceptable tradeoff for an allocation-free emitter.
            int slot = (int)((uint)Interlocked.Increment(ref _next) % (uint)RingSize);
            _values[slot] = durationMs;
            _ticks[slot] = DateTime.UtcNow.Ticks;
            Interlocked.Increment(ref _totalCount);
        }

        public StageSnapshot Snapshot(string stage, long cutoffTicks)
        {
            // Copy out the ring, filter by ticks > cutoff, sort, derive quantiles.
            // We allocate exactly once per call (the scratch array). The /stats/runtime
            // scrape rate caps this at ~one per 30s per stage.
            var scratch = new double[RingSize];
            int n = 0;
            for (int i = 0; i < RingSize; i++)
            {
                if (_ticks[i] >= cutoffTicks)
                {
                    var v = _values[i];
                    if (!double.IsNaN(v)) scratch[n++] = v;
                }
            }

            if (n == 0)
                return new StageSnapshot(stage, 0, Interlocked.Read(ref _totalCount), 0, 0, 0, 0, 0);

            Array.Sort(scratch, 0, n);
            double Quantile(double q)
            {
                if (n == 1) return scratch[0];
                double pos = q * (n - 1);
                int lo = (int)pos;
                int hi = Math.Min(n - 1, lo + 1);
                double frac = pos - lo;
                return scratch[lo] * (1 - frac) + scratch[hi] * frac;
            }

            return new StageSnapshot(
                Stage: stage,
                CountWindow: n,
                TotalCount: Interlocked.Read(ref _totalCount),
                Min: scratch[0],
                P50: Quantile(0.50),
                P95: Quantile(0.95),
                P99: Quantile(0.99),
                Max: scratch[n - 1]);
        }
    }
}

/// <summary>
/// Per-stage rollup as exposed via /stats/runtime. <see cref="CountWindow"/> is
/// the number of observations within <see cref="StageStatsRollup.WindowTtl"/>;
/// <see cref="TotalCount"/> is monotonic since process start so the dashboard
/// can spot stages that are no longer firing at all.
/// </summary>
public sealed record StageSnapshot(
    string Stage,
    int CountWindow,
    long TotalCount,
    double Min,
    double P50,
    double P95,
    double P99,
    double Max);
