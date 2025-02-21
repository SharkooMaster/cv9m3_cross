
using Microsoft.AspNetCore.Routing.Constraints;

namespace Cross.Utilities;

static public class Misc
{
    static public List<byte[]> SplitFile(byte[] source, int chunkSize)
    {
        if (source == null)
            throw new ArgumentNullException(nameof(source));
        if (chunkSize <= 0)
            throw new ArgumentOutOfRangeException(nameof(chunkSize), "Chunk size must be greater than zero.");

        int totalChunks = source.Length / chunkSize;
        int remainingBytes = source.Length % chunkSize;

        List<byte[]> chunks = new List<byte[]>(totalChunks + (remainingBytes > 0 ? 1 : 0));

        for (int i = 0; i < totalChunks; i++)
        {
            byte[] chunk = new byte[chunkSize];
            Buffer.BlockCopy(source, i * chunkSize, chunk, 0, chunkSize);
            chunks.Add(chunk);
        }

        if (remainingBytes > 0)
        {
            byte[] lastChunk = new byte[remainingBytes];
            Buffer.BlockCopy(source, totalChunks * chunkSize, lastChunk, 0, remainingBytes);
            chunks.Add(lastChunk);
        }

        return chunks;
    }

    public static List<float[]> Compute64ElementLSHVectors(IEnumerable<byte[]> chunks)
    {
        if (chunks == null)
            throw new ArgumentNullException(nameof(chunks));

        // Convert input to a list to get the count.
        var chunkList = chunks is IList<byte[]> list ? list : new List<byte[]>(chunks);
        int count = chunkList.Count;

        // Preallocate the results array.
        var results = new float[count][];

        // Create a random projection matrix of size 64 x dataSize.
        // For each call, we create a new projection matrix.
        // (If you need to reuse the matrix across calls, consider caching it.)
        int nComponents = 64;
        int dataSize = Globals.chunkSize;
        float[,] randomProjection = new float[nComponents, dataSize];
        Random random = new Random(42);
    
        for (int row = 0; row < nComponents; row++)
        {
            for (int col = 0; col < dataSize; col++)
            {
                // Generates a value in the range [-0.5, 0.5)
                randomProjection[row, col] = (float)(random.NextDouble() - 0.5);
            }
        }

        // Process each chunk in parallel.
        Parallel.For(0, count, i =>
        {
            results[i] = Compute64ElementLSHVector(chunkList[i], randomProjection);
        });

        return new List<float[]>(results);
    }

    private static float[] Compute64ElementLSHVector(byte[] chunk, float[,] randomProjection)
    {
        const int nComponents = 64;
        int dataSize = chunk.Length;
    
        // Project the data vector using the random projection matrix.
        float[] lshVector = new float[nComponents];
        for (int row = 0; row < nComponents; row++)
        {
            float sum = 0;
            for (int col = 0; col < dataSize; col++)
            {
                sum += randomProjection[row, col] * chunk[col];
            }
            lshVector[row] = sum;
        }
    
        return lshVector;
    }

    public static List<string> ComputeBitStringFromVectors(List<float[]> vectors)
    {
        var results = new string[vectors.Count];
        Parallel.For(0, vectors.Count, i => {
            results[i] = ComputeBitStringFromVector(vectors[i]);
        });

        return new List<string>(results);
    }

    private static string ComputeBitStringFromVector(float[] vector)
    {
        string to_return = "";
        for (int i = 0; i < vector.Length; i++)
        {
            to_return += (vector[i] < 0) ? "0" : "1";
        }
        return to_return;
    }

}
