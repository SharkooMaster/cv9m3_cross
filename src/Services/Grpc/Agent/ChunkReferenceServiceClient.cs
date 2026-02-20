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
    /// Decompression fallback: when target ownership is unknown, query all agents in parallel
    /// and return the first successful chunk. This guarantees correctness when references
    /// don't carry TargetAgent metadata.
    /// </summary>
    public async Task<byte[]?> GetChunkByReferenceFromAnyAgentAsync(
        ulong bucketId,
        ulong bucketIndex,
        CancellationToken ct = default)
    {
        var agents = RendezvousRouter.GetAgents();
        if (agents.Length == 0)
        {
            return await GetChunkByReferenceAsync(bucketId, bucketIndex, null, ct);
        }

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var tasks = new List<Task<byte[]?>>(agents.Length);

        foreach (var agent in agents)
        {
            tasks.Add(GetChunkByReferenceAsync(bucketId, bucketIndex, agent, linkedCts.Token));
        }

        while (tasks.Count > 0)
        {
            var completed = await Task.WhenAny(tasks);
            tasks.Remove(completed);

            var chunk = await completed;
            if (chunk != null && chunk.Length > 0)
            {
                linkedCts.Cancel(); // best-effort cancel remaining calls
                return chunk;
            }
        }

        return null;
    }
}

