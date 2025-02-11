using Cross.Modules.Agneta;
using Cross.Utilities;
using GatewayService;
using Grpc.Core;
using Grpc.Net.Client;
using System;
using System.Threading.Tasks;

public class SearchAllServiceClient
{
    private readonly GatewayService.GatewayService.GatewayServiceClient _client;

    public SearchAllServiceClient()
    {
        var channel = GrpcChannel.ForAddress(Globals.GatewayLoadbalancer);
        _client = new GatewayService.GatewayService.GatewayServiceClient(channel);
    }

    public async Task<QueryResponse> SearchAllAsync(QueryRequest request, CallOptions options = default)
    {
        try
        {
            return await _client.SearchAllAsync(request, options);
        }
        catch (RpcException ex)
        {
            AgnetaHandler.Log(2, $"gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            throw;
        }
        catch (Exception ex)
        {
            AgnetaHandler.Log(2, $"[SearchAll] General error: {ex.Message}");
            throw;
        }
    }
}