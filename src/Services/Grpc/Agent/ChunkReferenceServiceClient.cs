using Grpc.Core;
using Cross.Utilities;

namespace Cross.Services.Grpc.Agent;

public class ChunkReferenceServiceClient
{
    /// <summary>
    /// Fetch a chunk by (bucketId, bucketIndex) — used for v2.0.0/v2.1.0 compressed files.
    /// The agent looks up storageGuid via bucket metadata, then fetches the chunk bytes.
    /// </summary>
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
    /// Fetch a chunk by storageGuid (SHA256 content hash) — used for v3.0.0+ compressed files.
    /// O(1) direct lookup. Content-addressable: any agent with this GUID has the CORRECT data.
    /// Safe to query multiple agents — impossible to get stale/wrong data.
    /// </summary>
    public async Task<byte[]?> GetChunkByStorageGuidAsync(
        string storageGuid,
        string? targetAgent = null,
        CancellationToken ct = default)
    {
        if (string.IsNullOrWhiteSpace(storageGuid))
            return null;

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

            var req = new GetChunkByKey_Req { ChunkKey = storageGuid };

            var res = await client.GetChunkByKeyAsync(
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
            Console.WriteLine($"[ChunkReferenceServiceClient] GetChunkByKey gRPC error targeting {agentTarget}: {ex.Status.StatusCode} - {ex.Status.Detail}");
            return null;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ChunkReferenceServiceClient] GetChunkByKey error targeting {agentTarget}: {ex.Message}");
            return null;
        }
    }
}
