using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;

namespace Cross.Utilities;

/// <summary>
/// Exposes <c>GET /stats/runtime</c> on this pod's HTTP/1 port (5001).
///
/// Pulled by the control-center pod every ~30 s. The handler is allocation-bounded
/// (one anonymous object + one JSON serialisation) and never touches the
/// compression hot path. <see cref="System.GC.GetGCMemoryInfo()"/> is ~1 µs
/// and lock-free; <see cref="System.Diagnostics.Process.WorkingSet64"/> reads
/// <c>/proc/self/status</c> on Linux (~10–100 µs) — both safe to call inline
/// from a request handler.
///
/// The shape mirrors what the control-center scraper deserialises into; if you
/// change a field name here you must update <c>RuntimeStatsStore</c> on the
/// other side.
/// </summary>
public static class RuntimeStatsEndpoint
{
    public static void Map(WebApplication app, string component)
    {
        app.MapGet("/stats/runtime", () =>
        {
            var gc = System.GC.GetGCMemoryInfo();
            var p = System.Diagnostics.Process.GetCurrentProcess();
            p.Refresh();
            var gens = gc.GenerationInfo;

            return Results.Json(new
            {
                ts_unix_ms              = System.DateTimeOffset.UtcNow.ToUnixTimeMilliseconds(),
                component               = component,
                pod                     = System.Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown",
                node                    = System.Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown",

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

                rss_bytes               = p.WorkingSet64,
                private_bytes           = p.PrivateMemorySize64,
                memory_load_bytes       = gc.MemoryLoadBytes,
                memory_load_threshold   = gc.HighMemoryLoadThresholdBytes,

                // RSS minus managed heap = roughly native + pinned + reserve. Growing
                // while heap_size_bytes is flat ⇒ native bloat (RocksDB / glibc /
                // gRPC pinned buffers).
                native_overhead_bytes   = System.Math.Max(0, p.WorkingSet64 - gc.HeapSizeBytes),

                // Per-agent circuit breaker snapshot. Open or HalfOpen entries
                // here are the leading indicator of a wedged agent and the
                // mechanism that yesterday's "stuck" symptom was missing.
                // Surfaced only on cross — on other components this is empty.
                breakers = component == "cross" ? AgentRpcThrottle.Snapshot() : null,
            });
        });
    }
}
