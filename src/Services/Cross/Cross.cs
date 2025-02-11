
using Cross.Interfaces.Cross;
using Cross.Utilities;
using GatewayService;

namespace Cross.Services.Cross;

public class CrossService : ICross
{
    SearchAllServiceClient searchAllServiceClient = new SearchAllServiceClient();
    public async Task<byte[]> CompressFile(byte[] _file)
    {
        // Divide into (N) chunks.
        List<byte[]> fileChunks = Misc.SplitFile(_file, Globals.chunkSize);
        byte[] trimmedChunk = fileChunks.Last();
        fileChunks.RemoveAt(fileChunks.Count - 1);

        // Vectorize
        List<float[]> vectors = Misc.Compute64ElementLSHVectors(fileChunks);

        // Extract bitstring
        List<string> bitStrings = Misc.ComputeBitStringFromVectors(vectors);

        // Search
        QueryRequest request = new QueryRequest();
        for (int i = 0; i < vectors.Count; i++)
        {
            QueryObject qo = new QueryObject() { BucketString = bitStrings[i] };
            qo.Vector.AddRange(vectors[i]);

            request.QueryObjects.Add(qo);
        }

        QueryResponse response = await searchAllServiceClient.SearchAllAsync(request);

        // Compare results
        // Encode results
        // Add Dictionary and trimming
        // Return
        return trimmedChunk;
    }

    public async Task<byte[]> DecompressFile(byte[] _file)
    {
        return new byte[10];
    }
}
