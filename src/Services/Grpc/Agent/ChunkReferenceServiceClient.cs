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
        catch (RpcException ex) when (ex.StatusCode == StatusCode.Cancelled)
        {
            return null;
        }
        catch (RpcException ex)
        {
            Console.WriteLine($"[ChunkReferenceServiceClient] gRPC error targeting {agentTarget}: {ex.Status.StatusCode} - {ex.Status.Detail}");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ChunkReferenceServiceClient] Error targeting {agentTarget}: {ex.Message}");
            return null;
        }
    }

    /// <summary>
    /// Fallback for decompression: try ALL agents (excluding a specific one we already tried).
    /// Safe because the in-memory dedup counters guarantee no duplicate (bucketId, bucketIndex)
    /// across agents. This handles files compressed under a previous routing scheme.
    /// </summary>
    public async Task<byte[]?> GetChunkFromOtherAgentsAsync(
        ulong bucketId,
        ulong bucketIndex,
        string excludeAgent,
        CancellationToken ct = default)
    {
        var agents = RendezvousRouter.GetAgents();
        if (agents.Length <= 1)
            return null;

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        linkedCts.CancelAfter(TimeSpan.FromSeconds(15));

        var tasks = new List<Task<byte[]?>>();
        foreach (var agent in agents)
        {
            if (agent == excludeAgent) continue;
            tasks.Add(GetChunkByReferenceAsync(bucketId, bucketIndex, agent, linkedCts.Token));
        }

        while (tasks.Count > 0)
        {
            var completed = await Task.WhenAny(tasks);
            tasks.Remove(completed);

            try
            {
                var chunk = await completed;
                if (chunk != null && chunk.Length > 0)
                {
                    linkedCts.Cancel();
                    return chunk;
                }
            }
            catch { /* agent failed, try next */ }
        }

        return null;
    }
}
