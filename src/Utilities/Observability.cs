using System.Diagnostics;
using System.Diagnostics.Metrics;
using OpenTelemetry.Resources;

namespace Cross.Utilities;

public static class Observability
{
    public const string ServiceName = "crossv9-cross";
    public static readonly ActivitySource ActivitySource = new("CrossV9.Cross");
    public static readonly Meter Meter = new("CrossV9.Cross");
    public static readonly Histogram<double> StageDurationMs =
        Meter.CreateHistogram<double>("crossv9_cross_stage_duration_ms", "ms", "Stage duration in milliseconds");

    public static Activity? StartStage(string stageName)
    {
        var activity = ActivitySource.StartActivity(stageName, ActivityKind.Internal);
        activity?.SetTag("stage", stageName);
        activity?.SetTag("k8s.pod.name", Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown");
        activity?.SetTag("k8s.node.name", Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown");
        return activity;
    }

    public static void RecordStage(string stageName, double durationMs, params (string Key, object? Value)[] tags)
    {
        var tagList = new TagList
        {
            { "stage", stageName },
            { "k8s.pod.name", Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown" },
            { "k8s.node.name", Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown" }
        };
        int? chunkCount = null;
        int? bucketCount = null;
        ulong? bytes = null;
        foreach (var (key, value) in tags)
        {
            tagList.Add(key, value);
            // Best-effort attribution to the JobEvent fields. Done here (vs. at every
            // call site) so the existing RecordStage callers automatically participate
            // in the dashboard with no further changes.
            switch (key)
            {
                case "chunk_count" when value is int ic: chunkCount = ic; break;
                case "bucket_count" when value is int ib: bucketCount = ib; break;
                case "output_bytes" when value is int ob: bytes = (ulong)ob; break;
                case "output_bytes" when value is long olb: bytes = (ulong)olb; break;
            }
        }
        StageDurationMs.Record(durationMs, tagList);
        Cross.Services.JobEvents.JobEventBus.EmitStageDone(stageName, durationMs, chunkCount, bucketCount, bytes);
        // Feed the live rolling-percentile rollup that /stats/runtime surfaces.
        // Allocation-free, lock-free; safe to call from anywhere RecordStage
        // already runs — which means every existing stage callsite is now
        // automatically dashboard-visible without further code changes.
        StageStatsRollup.Record(stageName, durationMs);
    }

    public static ResourceBuilder CreateResourceBuilder() =>
        ResourceBuilder.CreateDefault()
            .AddAttributes(new[]
            {
                new KeyValuePair<string, object>("service.name", ServiceName),
                new KeyValuePair<string, object>("k8s.pod.name", Environment.GetEnvironmentVariable("MY_POD_NAME") ?? "unknown"),
                new KeyValuePair<string, object>("k8s.node.name", Environment.GetEnvironmentVariable("MY_NODE_NAME") ?? "unknown")
            });

    public static string GetOtlpEndpoint() =>
        Environment.GetEnvironmentVariable("OTEL_EXPORTER_OTLP_ENDPOINT")
        ?? "http://crossv9-crossv9-otel-collector:4317";
}

