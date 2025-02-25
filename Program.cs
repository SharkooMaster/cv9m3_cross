using Microsoft.AspNetCore.Server.Kestrel.Core;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddGrpc(options => {
    options.MaxReceiveMessageSize = 1 * 1024 * 1024 * 1024;
});

// Configure Kestrel to allow HTTP/2 without TLS
builder.WebHost.ConfigureKestrel(options =>
{
    options.ListenAnyIP(5000, o => o.Protocols = HttpProtocols.Http2);
    options.ListenAnyIP(5001, o => o.Protocols = HttpProtocols.Http1);
});

builder.Services.AddEndpointsApiExplorer();

var app = builder.Build();

app.UseRouting();

app.MapGrpcService<CompressFileService>();

app.MapGet("/", () => "Hello World!");

app.Run();
