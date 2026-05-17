using Crossv9.Jobevents;
using Grpc.Net.Client;
using Microsoft.Extensions.Hosting;

namespace Cross.Services.JobEvents;

/// <summary>
/// Drains <see cref="JobEventBus.Reader"/> on a single background thread and forwards
/// each event to the control-center pod over a long-lived gRPC client-streaming RPC.
///
/// Failure-mode contract:
///   - If the control-center is unreachable, this service backs off and retries
///     while the bounded channel in <see cref="JobEventBus"/> drops new events.
///     The compression path is unaffected.
///   - If the gRPC stream errors mid-flight, we drop the in-flight event and
///     reconnect. Events are best-effort; the user accepted that trade-off.
///   - If <see cref="JobEventBus.Enabled"/> is false, this service exits immediately
///     after start so it consumes no resources.
/// </summary>
public sealed class JobEventForwarderService : BackgroundService
{
    private readonly string _endpoint;
    private readonly TimeSpan _initialBackoff = TimeSpan.FromSeconds(1);
    private readonly TimeSpan _maxBackoff = TimeSpan.FromSeconds(30);

    public JobEventForwarderService()
    {
        _endpoint = Environment.GetEnvironmentVariable("JOB_EVENTS_ENDPOINT")
                    ?? "http://crossv9-controlcenter:5000";
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        if (!JobEventBus.Enabled)
        {
            Console.WriteLine("[JobEvents] Disabled by JOB_EVENTS_ENABLED=false; forwarder is idle.");
            return;
        }

        Console.WriteLine($"[JobEvents] Forwarder targeting {_endpoint}");

        var backoff = _initialBackoff;

        while (!stoppingToken.IsCancellationRequested)
        {
            GrpcChannel? channel = null;
            try
            {
                channel = GrpcChannel.ForAddress(_endpoint, new GrpcChannelOptions
                {
                    HttpHandler = new SocketsHttpHandler
                    {
                        EnableMultipleHttp2Connections = false,
                        KeepAlivePingDelay = TimeSpan.FromSeconds(30),
                        KeepAlivePingTimeout = TimeSpan.FromSeconds(15),
                        PooledConnectionIdleTimeout = TimeSpan.FromMinutes(5),
                    }
                });

                var client = new JobEventService.JobEventServiceClient(channel);
                using var call = client.Push(cancellationToken: stoppingToken);

                Console.WriteLine($"[JobEvents] Forwarder connected to {_endpoint}");
                backoff = _initialBackoff; // reset on successful connect

                long sent = 0;
                while (await JobEventBus.Reader.WaitToReadAsync(stoppingToken))
                {
                    while (JobEventBus.Reader.TryRead(out var ev))
                    {
                        await call.RequestStream.WriteAsync(ev, stoppingToken);
                        sent++;
                    }
                }

                await call.RequestStream.CompleteAsync();
                var ack = await call.ResponseAsync;
                Console.WriteLine($"[JobEvents] Forwarder shutdown; sent={sent}, acked={ack.EventsReceived}");
                return;
            }
            catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[JobEvents] Forwarder error: {ex.GetType().Name}: {ex.Message}. " +
                                  $"Reconnecting in {backoff.TotalSeconds:F0}s. " +
                                  $"emitted={JobEventBus.Emitted}, dropped={JobEventBus.Dropped}");
            }
            finally
            {
                try { channel?.Dispose(); } catch { }
            }

            try { await Task.Delay(backoff, stoppingToken); }
            catch (OperationCanceledException) { return; }
            backoff = TimeSpan.FromMilliseconds(Math.Min(_maxBackoff.TotalMilliseconds, backoff.TotalMilliseconds * 2));
        }
    }
}
