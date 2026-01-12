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