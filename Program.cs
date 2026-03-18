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
builder.Services.AddHostedService<Cross.Services.CcfStore.CcfPackOptimizerService>();
builder.Services.AddHostedService<Cross.Services.CcfStore.ChunkConsolidationService>();
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

app.UseRouting();
app.MapPrometheusScrapingEndpoint("/metrics");

app.MapGrpcService<CompressFileService>();

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
