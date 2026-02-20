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

    /// <summary>
    /// Try all agents in parallel to find a chunk by reference.
    /// Used during decompression when we don't know which agent owns the chunk.
    /// Returns the first successful result, or null if all agents fail.
    /// </summary>
    public async Task<byte[]?> GetChunkByReferenceFromAnyAgentAsync(
        ulong bucketId,
        ulong bucketIndex,
        CancellationToken ct = default)
    {
        var agents = RendezvousRouter.GetAgents();
        if (agents.Length == 0)
            return null;

        // Query all agents in parallel, return first successful result
        var tasks = agents.Select(async agent =>
        {
            try
            {
                var client = GrpcChannelFactory.GetClient(
                    target: agent,
                    ctor: chan => new ChunkReferenceService.ChunkReferenceServiceClient(chan),
                    roundRobin: false,
                    port: 5000);

                var req = new GetChunkByReference_Req
                {
                    BucketId = bucketId,
                    BucketIndex = bucketIndex
                };

                var res = await client.GetChunkByReferenceAsync(
                    req,
                    deadline: DateTime.UtcNow.AddSeconds(5), // Shorter timeout for parallel queries
                    cancellationToken: ct);

                if (res.Found && res.Chunk != null && res.Chunk.Length > 0)
                {
                    return res.Chunk.ToByteArray();
                }
            }
            catch { /* Try next agent */ }
            return null;
        }).ToList();

        // Wait for all tasks, return first non-null result
        var results = await Task.WhenAll(tasks);
        return results.FirstOrDefault(r => r != null);
    }
}

