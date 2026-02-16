using Grpc.Core;
using Cross.Utilities;

namespace Cross.Services.Grpc.Agent;

public class ChunkReferenceServiceClient
{
    public async Task<byte[]?> GetChunkByReferenceAsync(
        ulong bucketId,
        ulong bucketIndex,
        string? targetAgent = null,
        CancellationToken ct = default)
    {
        string agentTarget = string.IsNullOrWhiteSpace(targetAgent)
            ? global::Cross.Utilities.Globals.AgentsLoadbalancer
            : targetAgent;

        try
        {
            var client = GrpcChannelFactory.GetClient(
                target: agentTarget,
                ctor: chan => new ChunkReferenceService.ChunkReferenceServiceClient(chan),
                roundRobin: LocalModeDetector.IsLocalMode(),
                port: 5000
            );

            var req = new GetChunkByReference_Req
            {
                BucketId = bucketId,
                BucketIndex = bucketIndex
            };

            var res = await client.GetChunkByReferenceAsync(
                req,
                deadline: DateTime.UtcNow.AddSeconds(30),
                cancellationToken: ct);

            if (!res.Found || res.Chunk == null || res.Chunk.Length == 0)
            {
                return null;
            }

            return res.Chunk.ToByteArray();
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"[ChunkReferenceServiceClient] gRPC error: {ex.Status.StatusCode} - {ex.Status.Detail}");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ChunkReferenceServiceClient] Error: {ex.Message}");
            return null;
        }
    }
}

