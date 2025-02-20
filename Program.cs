using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc();

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(8100, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(8101, o => o.Protocols = HttpProtocols.Http1);
});

builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

app.UseRouting();

app.MapGrpcService<SearchAllServiceClient>();
app.MapGrpcService<CompressFileService>();

app.MapGet("/", () => "Hello World!");

app.Run();
