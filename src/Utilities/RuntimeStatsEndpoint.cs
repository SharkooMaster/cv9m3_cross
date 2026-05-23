using System.Threading;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Cross.Utilities;

/// <summary>
/// Exposes <c>GET /stats/runtime</c> on this pod's HTTP/1 port (5001).
///
/// Pulled by the control-center pod every ~30 s. The handler is allocation-bounded
/// and never touches the compression hot path:
///   - <see cref="System.GC.GetGCMemoryInfo()"/> is ~1 µs and lock-free.
///   - <see cref="System.Diagnostics.Process.WorkingSet64"/> reads
///     <c>/proc/self/status</c> on Linux (~10–100 µs).
///   - <see cref="System.Threading.ThreadPool"/> counters are atomic reads.
///   - <c>StageStatsRollup.SnapshotAll()</c> sorts ~4k doubles per stage; with
///     ~12 stages that's a few hundred microseconds. Acceptable at 30 s cadence.
///
/// The shape mirrors what the control-center scraper deserialises into
/// (see <c>RuntimeStatsStore.RuntimeSample</c>). Field names must stay in sync
/// across cross / gateway / agent / controlcenter — adding new fields is fine
/// (controlcenter ignores unknown keys), renaming or repurposing is not.
/// </summary>
public static class RuntimeStatsEndpoint
{
    private static readonly System.Diagnostics.Stopwatch _uptime = System.Diagnostics.Stopwatch.StartNew();

    public static void Map(WebApplication app, string component)
    {
        app.MapGet("/stats/runtime", () =>
        {
            var gc = System.GC.GetGCMemoryInfo();
            var p = System.Diagnostics.Process.GetCurrentProcess();
            p.Refresh();
            var gens = gc.GenerationInfo;

            // ── ThreadPool occupancy. The min/max bounds are configurable via
            //    DOTNET_ThreadPool_* env vars. Busy = max - available; queue
            //    length is a direct reading of the global work queue. A
            //    sustained busy ≈ max with a non-zero queue is the textbook
            //    signature of threadpool starvation (which the gateway sync-
            //    Store bug we just fixed used to produce). ───────────────
            ThreadPool.GetAvailableThreads(out var workerAvail, out var ioAvail);
            ThreadPool.GetMaxThreads(out var workerMax, out var ioMax);
            ThreadPool.GetMinThreads(out var workerMin, out var ioMin);
            var threadpoolQueue = ThreadPool.PendingWorkItemCount;

            // ── time_in_gc_pct: cumulative GC pause time / process uptime.
            //    Anything north of ~5 % means GC is meaningfully eating CPU
            //    and the heap-pooling fixes (ArrayPool / UnsafeWrap / LOH
            //    compaction) are warranted. ────────────────────────────────
            double uptimeSec = _uptime.Elapsed.TotalSeconds;
            double gcPauseSec = 0;
            try { gcPauseSec = System.GC.GetTotalPauseDuration().TotalSeconds; } catch { /* runtime < 7 */ }
            double timeInGcPct = uptimeSec > 0 ? gcPauseSec * 100.0 / uptimeSec : 0;

            int openFdCount = TryReadOpenFdCount();

            // Stage rollup is a per-pod live percentile readout. Useful for
            // "is search slower right now than it was ten minutes ago?"
            // without round-tripping to the journal.
            var stages = StageStatsRollup.SnapshotAll();

            return Results.Json(new
            {
                ts_unix_ms              = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                component               = component,
                pod                     = System.Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown",
                node                    = System.Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown",
                process_uptime_sec      = uptimeSec,

                heap_size_bytes         = gc.HeapSizeBytes,
                heap_committed_bytes    = gc.TotalCommittedBytes,
                heap_fragmented_bytes   = gc.FragmentedBytes,

                gen0_size               = gens.Length > 0 ? gens[0].SizeAfterBytes : 0,
                gen1_size               = gens.Length > 1 ? gens[1].SizeAfterBytes : 0,
                gen2_size               = gens.Length > 2 ? gens[2].SizeAfterBytes : 0,
                loh_size                = gens.Length > 3 ? gens[3].SizeAfterBytes : 0,
                loh_fragmented_bytes    = gens.Length > 3 ? gens[3].FragmentationAfterBytes : 0,
                poh_size                = gens.Length > 4 ? gens[4].SizeAfterBytes : 0,

                gen0_collections        = System.GC.CollectionCount(0),
                gen1_collections        = System.GC.CollectionCount(1),
                gen2_collections        = System.GC.CollectionCount(2),

                gc_pause_total_sec      = gcPauseSec,
                time_in_gc_pct          = timeInGcPct,

                rss_bytes               = p.WorkingSet64,
                private_bytes           = p.PrivateMemorySize64,
                memory_load_bytes       = gc.MemoryLoadBytes,
                memory_load_threshold   = gc.HighMemoryLoadThresholdBytes,
                native_overhead_bytes   = System.Math.Max(0, p.WorkingSet64 - gc.HeapSizeBytes),

                threadpool_workers_busy       = System.Math.Max(0, workerMax - workerAvail),
                threadpool_workers_max        = workerMax,
                threadpool_workers_min        = workerMin,
                threadpool_completion_busy    = System.Math.Max(0, ioMax - ioAvail),
                threadpool_completion_max     = ioMax,
                threadpool_completion_min     = ioMin,
                threadpool_queue_length       = threadpoolQueue,

                open_fd_count           = openFdCount,

                grpc_channel_cache_count = GrpcChannelFactory.ChannelCacheCount,
                grpc_client_cache_count  = GrpcChannelFactory.ClientCacheCount,

                // ── Replication health (Phase 7) ──
                // ring_size = number of agents currently on the
                // consistent-hash ring (matches what the etcd watcher
                // last published). topology_version increments on every
                // membership delta; the control center plots its rate
                // to surface "is the cluster churning?".
                ring_size           = Cross.Routing.RingState.Current.Agents.Count,
                topology_version    = Cross.Routing.RingState.TopologyVersion,
                replication_factor  = Cross.Routing.ReplicaResolver.ReplicationFactor,
                write_quorum        = Cross.Routing.ReplicaResolver.WriteQuorum,
                vnodes_per_agent    = Cross.Routing.RingState.Current.VnodesPerAgent,

                // Per-agent circuit breaker snapshot. Open / HalfOpen entries
                // here are the leading indicator of a wedged agent.
                breakers = AgentRpcThrottle.Snapshot(),

                // Live rolling p50/p95/p99 per pipeline stage (last 5 min).
                stage_stats = stages.Select(s => new
                {
                    stage           = s.Stage,
                    count_window    = s.CountWindow,
                    total_count     = s.TotalCount,
                    min_ms          = s.Min,
                    p50_ms          = s.P50,
                    p95_ms          = s.P95,
                    p99_ms          = s.P99,
                    max_ms          = s.Max,
                }),
            });
        });
    }

    /// <summary>
    /// Counts entries in /proc/self/fd. Linux-only; returns 0 on Windows / macOS
    /// or if the directory can't be enumerated (e.g. inside a sandboxed
    /// container without /proc). Never throws.
    /// </summary>
    private static int TryReadOpenFdCount()
    {
        try
        {
            const string path = "/proc/self/fd";
            if (!System.IO.Directory.Exists(path)) return 0;
            int count = 0;
            foreach (var _ in System.IO.Directory.EnumerateFileSystemEntries(path)) count++;
            return count;
        }
        catch { return 0; }
    }
}
