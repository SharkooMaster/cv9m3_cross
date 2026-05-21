using Cross.Modules.Agneta;
using Cross.Utilities;
using GatewayService;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

public class SearchAllServiceClient
{
    public SearchAllServiceClient()
    {
    }

    public async Task<QueryResponse> SearchAllAsync(QueryRequest request, CallOptions options = default)
    {
        // Bounded retry around the gateway round-robin client.
        // The headless gateway service goes NXDOMAIN ("Name or service not
        // known") whenever no gateway pods are Ready — startup, rolling
        // updates, scale events. The built-in gRPC retry policy can only
        // retry on Unavailable/Internal *after* the call started, but a
        // pre-call DNS SocketException is surfaced as RpcException
        // (Unavailable) before the first attempt. Wrap a small outer loop
        // so a transient resolution failure is invisible to callers.
        const int maxAttempts = 6;
        Exception? lastEx = null;
        for (int attempt = 0; attempt < maxAttempts; attempt++)
        {
            try
            {
                var _client = GrpcChannelFactory.GetClient(
                    target: Globals.GatewayLoadbalancer,
                    ctor: ch => new GatewayService.GatewayService.GatewayServiceClient(ch),
                    roundRobin: true,
                    port: 5000
                );
                return await _client.SearchAllAsync(request, options);
            }
            catch (RpcException ex) when (IsTransient(ex) && attempt + 1 < maxAttempts)
            {
                // After a transport failure against the gateway dns:/// channel,
                // evict the cached channel + clients so the next attempt
                // re-resolves DNS and rebuilds a fresh HTTP/2 connection.
                // Without this, a single PROTOCOL_ERROR on the cached channel
                // can poison every subsequent retry too.
                GrpcChannelFactory.EvictOnFailure(Globals.GatewayLoadbalancer, ex);
                lastEx = ex;
                var backoff = ComputeBackoff(attempt);
                if (attempt == 0 || attempt == maxAttempts - 2)
                    Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail} (attempt {attempt + 1}/{maxAttempts}, backoff {backoff.TotalMilliseconds:F0}ms)");
                await Task.Delay(backoff);
            }
            catch (RpcException ex)
            {
                Console.WriteLine($"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
                throw;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SearchAll] General error: {ex.Message}");
                throw;
            }
        }
        throw lastEx ?? new InvalidOperationException("[SearchAll] retry loop exited with no exception");
    }

    /// <summary>
    /// Errors we expect to be transient in cluster startup / rolling-update
    /// scenarios. DNS NXDOMAIN bubbles up as Unavailable with detail
    /// "Error getting DNS hosts ... Name or service not known".
    /// </summary>
    private static bool IsTransient(RpcException ex)
    {
        return ex.StatusCode == StatusCode.Unavailable
            || ex.StatusCode == StatusCode.Internal
            || ex.StatusCode == StatusCode.DeadlineExceeded;
    }

    private static TimeSpan ComputeBackoff(int attempt)
    {
        // 250 ms, 500 ms, 1 s, 2 s, 4 s — caps at 4 s so a longer-than-
        // expected gateway boot (≤ ~10 s combined) is hidden from callers.
        var ms = Math.Min(4000, 250 * (1 << attempt));
        return TimeSpan.FromMilliseconds(ms);
    }

    public async IAsyncEnumerable<QueryResponseObject> SearchAllStreamAsync(
        IAsyncEnumerable<QueryObject> queryObjects,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        var _client = GrpcChannelFactory.GetClient(
            target: Globals.GatewayLoadbalancer,
            ctor: ch => new GatewayService.GatewayService.GatewayServiceClient(ch),
            roundRobin: true,
            port: 5000
        );

        using var call = _client.SearchAllStream(cancellationToken: cancellationToken);
        
        // Start background task to write requests
        var writeTask = Task.Run(async () =>
        {
            try
            {
                await foreach (var queryObj in queryObjects.WithCancellation(cancellationToken))
                {
                    await call.RequestStream.WriteAsync(queryObj);
                }
                await call.RequestStream.CompleteAsync();
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[SearchAllStream] Error writing requests: {ex.Message}");
                try { await call.RequestStream.CompleteAsync(); } catch { }
                throw;
            }
        }, cancellationToken);

        // Read responses as they arrive
        // Cannot use try-catch around yield, so errors will propagate to caller
        var responseStream = call.ResponseStream;
        while (await responseStream.MoveNext(cancellationToken))
        {
            yield return responseStream.Current;
        }

        // Wait for write task to complete after all responses are read
        await writeTask;
    }
}