
using Cross.Interfaces.Cross;
using Cross.Utilities;

namespace Cross.Services.Cross;

public class CrossService : ICross
{
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
