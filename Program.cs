using Cross.Modules;
using Cross.Services.Clms;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Cross.Utilities;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options => {
    options.MaxReceiveMessageSize = 1000 * 1024 * 1024;
    options.MaxSendMessageSize = 1000 * 1024 * 1024;
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

ConfigureServices(builder.Services);

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http1);
    options.Limits.MaxRequestBodySize = 1024 * 1024 * 1024;
    
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

app.Run();

void ConfigureServices(IServiceCollection services)
{
    services.AddSingleton<ClmsClientService>(new ClmsClientService());
}
