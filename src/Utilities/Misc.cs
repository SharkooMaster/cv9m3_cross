
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

        // Process each chunk in parallel.
        Parallel.For(0, count, i =>
        {
            results[i] = Compute64ElementLSHVector(chunkList[i]);
        });

        return new List<float[]>(results);
    }

    private static float[] Compute64ElementLSHVector(byte[] chunk)
    {
        const int vectorSize = 64;
        float[] vector = new float[vectorSize];

        // Return a zero vector if the chunk is null or empty.
        if (chunk == null || chunk.Length == 0)
            return vector;

        // Determine the segment length. Use at least 1 byte per segment.
        int segmentLength = Math.Max(1, chunk.Length / vectorSize);

        for (int i = 0; i < vectorSize; i++)
        {
            int start = i * segmentLength;
            // Ensure we don't exceed the chunk length.
            int end = Math.Min(chunk.Length, start + segmentLength);

            long sum = 0;
            for (int j = start; j < end; j++)
            {
                sum += chunk[j];
            }

            int count = end - start;
            vector[i] = count > 0 ? (float)sum / count : 0;
        }

        return vector;
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
