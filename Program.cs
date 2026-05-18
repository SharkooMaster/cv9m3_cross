using Cross.Modules;
using Cross.Services.Clms;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Cross.Utilities;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options => {
    options.MaxReceiveMessageSize = 32 * 1024 * 1024;
    options.MaxSendMessageSize = 32 * 1024 * 1024;
});

builder.Services.AddOpenTelemetry()
    .WithTracing(tracing =>
    {
        tracing
            .SetResourceBuilder(Observability.CreateResourceBuilder())
            .AddSource("CrossV9.Cross")
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddOtlpExporter(otlp =>
            {
                otlp.Endpoint = new Uri(Observability.GetOtlpEndpoint());
                otlp.Protocol = OpenTelemetry.Exporter.OtlpExportProtocol.Grpc;
            });
    })
    .WithMetrics(metrics =>
    {
        metrics
            .SetResourceBuilder(Observability.CreateResourceBuilder())
            .AddMeter("CrossV9.Cross")
            .AddAspNetCoreInstrumentation()
            .AddHttpClientInstrumentation()
            .AddRuntimeInstrumentation()
            .AddPrometheusExporter();
    });

builder.Services.AddHostedService<AgentHealthWatcher>();
builder.Services.AddHostedService<Cross.Services.JobEvents.JobEventForwarderService>();
if (Globals.EnableCcfBackgroundServices)
{
    builder.Services.AddHostedService<Cross.Services.CcfStore.CcfPackOptimizerService>();
    builder.Services.AddHostedService<Cross.Services.CcfStore.ChunkConsolidationService>();
}
else
{
    Console.WriteLine("[Cross] CCF background services disabled by env flag");
}
ConfigureServices(builder.Services);

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http1);
    options.Limits.MaxRequestBodySize = null;
    
    options.Limits.KeepAliveTimeout = TimeSpan.FromMinutes(10);
    options.Limits.Http2.KeepAlivePingDelay = TimeSpan.FromSeconds(30);
    options.Limits.Http2.KeepAlivePingTimeout = TimeSpan.FromSeconds(60);
});

builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

var clmsClientService = app.Services.GetRequiredService<ClmsClientService>();
ClmsHandler.SetClmsInstance(clmsClientService);

// LOH compaction: cross's compress pipeline cycles 16–256 MB window buffers,
// ZSTD output buffers and per-batch protobuf payloads through the LOH at high
// rate. The LOH does not compact unless explicitly told to + a Gen2 GC fires.
// Without this, frag % climbs to 50%+ between pressure-induced compactions
// (observed in the controlcenter fleet panel). 60 s cadence with Forced mode
// caps peak frag at ~20 % with a ~50–150 ms STW pause per minute.
var crossLohCompactTimer = new System.Threading.Timer(_ =>
{
    System.Runtime.GCSettings.LargeObjectHeapCompactionMode =
        System.Runtime.GCLargeObjectHeapCompactionMode.CompactOnce;
    GC.Collect(2, GCCollectionMode.Forced, blocking: false);
}, null, TimeSpan.FromMinutes(1), TimeSpan.FromMinutes(1));

app.UseRouting();
app.MapPrometheusScrapingEndpoint("/metrics");

app.MapGrpcService<CompressFileService>();

// Tiny pod-self-reporting endpoint pulled by the control-center every ~30 s.
// Lives on the existing HTTP/1 port (5001), allocation-bounded, never touches
// the compression hot path.
Cross.Utilities.RuntimeStatsEndpoint.Map(app, "cross");

app.MapGet("/", () => "Hello World!");

// ── Global exception handlers to prevent silent crashes ──
AppDomain.CurrentDomain.UnhandledException += (sender, eventArgs) =>
{
    Console.WriteLine($"[CROSS UNHANDLED EXCEPTION]: {eventArgs.ExceptionObject}");
};

TaskScheduler.UnobservedTaskException += (sender, e) =>
{
    Console.WriteLine($"[CROSS UNOBSERVED TASK EXCEPTION]: {e.Exception}");
    e.SetObserved(); // Prevent process termination
};

// AgentHealthWatcher (hosted service) handles continuous discovery.
// The old manual warmup is no longer needed — the health watcher runs
// every 5s and the readiness gate in CompressFileService ensures we
// don't accept work until at least one agent is found.
Console.WriteLine("[Cross] Agent discovery delegated to AgentHealthWatcher (background service)");

app.Run();

void ConfigureServices(IServiceCollection services)
{
    services.AddSingleton<ClmsClientService>(new ClmsClientService());
}
