using Cross.Modules.Agneta;
using Cross.Utilities;
using GatewayService;
using Grpc.Core;
using Grpc.Net.Client;
using System;
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
                roundRobin: true
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
}