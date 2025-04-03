
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

    public static Dictionary<int, int> GetErrorEncoding(byte[] a, byte[] b)
    {
        Dictionary<int, int> to_return = new Dictionary<int, int>();
        int last_append = 0;

        for (int i = 0; i < a.Length; i++)
        {
            int dif = b[i] - a[i];
            if(dif != 0)
            {
                to_return.Add(i - last_append, dif);
                last_append = i;
            }
        }
        return to_return;
    }

    public static (byte[], int) GetErrorEncodingBytes(Dictionary<int, int> a, int offset)
    {
        List<byte> to_return = new List<byte>();
        int offset_return = offset;
        int[] _keys = a.Keys.ToArray();
        for (int i = 0; i < a.Count; i++)
        {
            to_return.AddRange(BitConverter.GetBytes(_keys[i] + offset));
            to_return.AddRange(BitConverter.GetBytes((Int16)a[i]));
            offset_return += _keys[i];
        }
        return (to_return.ToArray(), offset_return);
    }

    public static List<T> CreateList<T>(int count, Func<T> factory)
    {
        var list = new List<T>(count);
        for (int i = 0; i < count; i++)
        {
            list.Add(factory());
        }
        return list;
    }

    public static double GetMemoryUsagePercentage()
    {
        double totalMemory = 0;
        double freeMemory = 0;

        var lines = File.ReadAllLines("/proc/meminfo");

        foreach (var line in lines)
        {
            if (line.StartsWith("MemTotal:"))
            {
                totalMemory = ParseMemValue(line);
            }
            else if (line.StartsWith("MemAvailable:"))
            {
                freeMemory = ParseMemValue(line);
                break;
            }
        }

        double usedMemory = totalMemory - freeMemory;
        return usedMemory / totalMemory;
    }

    private static double ParseMemValue(string line)
    {
        var parts = line.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
        return double.Parse(parts[1]) / 1024;
    }

    public static long GetAvailableMemory()
    {
        return GC.GetGCMemoryInfo().TotalAvailableMemoryBytes;
    }

    public static double GetLoadAverage()
    {
        string[] parts = File.ReadAllText("/proc/loadavg").Split(' ', StringSplitOptions.RemoveEmptyEntries);
        return double.Parse(parts[0]);
    }

}
