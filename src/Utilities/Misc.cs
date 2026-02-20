
using System.Numerics;

namespace Cross.Utilities;

static public class Misc
{
    // Cached projection matrix to avoid recreation on every call
    private static float[,]? _cachedProjection = null;
    private static readonly object _projectionLock = new object();
    private static int _cachedChunkSize = 0;

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
            byte[] ownedChunk = new byte[chunkSize];
            Buffer.BlockCopy(source, i * chunkSize, ownedChunk, 0, chunkSize);
            chunks.Add(ownedChunk);
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

        // Get or create cached projection matrix (thread-safe)
        int nComponents = 64;
        int dataSize = Globals.chunkSize;
        float[,] randomProjection = GetOrCreateProjectionMatrix(nComponents, dataSize);

        // Uncapped parallelism — vectorization is pure CPU, no I/O
        Parallel.For(0, count, new ParallelOptions { MaxDegreeOfParallelism = -1 }, i =>
        {
            results[i] = Compute64ElementLSHVector(chunkList[i], randomProjection);
        });

        return new List<float[]>(results);
    }

    /// <summary>
    /// Gets or creates a cached projection matrix. Thread-safe initialization.
    /// This avoids recreating the matrix on every compression call.
    /// </summary>
    private static float[,] GetOrCreateProjectionMatrix(int nComponents, int dataSize)
    {
        // Check if we need to recreate the cache (chunk size changed)
        if (_cachedProjection == null || _cachedChunkSize != dataSize)
        {
            lock (_projectionLock)
            {
                // Double-check after acquiring lock
                if (_cachedProjection == null || _cachedChunkSize != dataSize)
                {
                    Console.WriteLine($"[Misc] Creating cached projection matrix: {nComponents}x{dataSize}");
                    _cachedProjection = new float[nComponents, dataSize];
                    Random random = new Random(42); // Fixed seed for determinism
    
                    for (int row = 0; row < nComponents; row++)
                    {
                        for (int col = 0; col < dataSize; col++)
                        {
                            // Generates a value in the range [-0.5, 0.5)
                            _cachedProjection[row, col] = (float)(random.NextDouble() - 0.5);
                        }
                    }
                    _cachedChunkSize = dataSize;
                    Console.WriteLine($"[Misc] Projection matrix cached successfully");
                }
            }
        }

        return _cachedProjection;
    }

    // Thread-local pre-converted chunk floats to avoid per-call allocation
    [ThreadStatic] private static float[]? _tlsChunkFloats;

    /// <summary>
    /// LSH projection: 64 × chunkSize matrix multiply.
    /// Zero allocations in the hot loop — pre-converts chunk bytes to floats once,
    /// then uses a flat projection array for cache-friendly sequential access.
    /// </summary>
    private static float[] Compute64ElementLSHVector(byte[] chunk, float[,] randomProjection)
    {
        const int nComponents = 64;
        int dataSize = chunk.Length;

        // Pre-convert bytes → floats ONCE (reuse thread-local buffer)
        if (_tlsChunkFloats == null || _tlsChunkFloats.Length < dataSize)
            _tlsChunkFloats = new float[dataSize];

        var chunkFloats = _tlsChunkFloats;
        for (int i = 0; i < dataSize; i++)
            chunkFloats[i] = chunk[i];

        float[] lshVector = new float[nComponents];

        for (int row = 0; row < nComponents; row++)
        {
            float sum = 0f;
            // Sequential multiply-add — compiler auto-vectorizes this with /O2.
            // The 2D array indexing is row-major so randomProjection[row, col] is
            // sequential in memory for a given row → cache-friendly.
            for (int col = 0; col < dataSize; col++)
            {
                sum += randomProjection[row, col] * chunkFloats[col];
            }
            lshVector[row] = sum;
        }

        return lshVector;
    }

    public static List<string> ComputeBitStringFromVectors(List<float[]> vectors)
    {
        var results = new string[vectors.Count];
        Parallel.For(0, vectors.Count, new ParallelOptions { MaxDegreeOfParallelism = -1 }, i => {
            results[i] = ComputeBitStringFromVector(vectors[i]);
        });

        return new List<string>(results);
    }

    private static string ComputeBitStringFromVector(float[] vector)
    {
        char[] chars = new char[vector.Length];
        for (int i = 0; i < vector.Length; i++)
        {
            chars[i] = vector[i] < 0 ? '0' : '1';
        }
        return new string(chars);
    }

    public static List<(int key, int value)> GetErrorEncoding(byte[] a, byte[] b)
    {
        List<(int key, int value)> to_return = new List<(int, int)>();
        int last_append = 0;

        for (int i = 0; i < a.Length; i++)
        {
            // Delta should represent how much to add to BASE to get ORIGINAL:
            // original = base + delta  =>  delta = original - base
            int dif = a[i] - b[i];
            if(dif != 0)
            {
                int key = i - last_append;
                // Allow duplicate keys - use List to store multiple entries with same key
                // Example: {1:1, 1:255, 2:1, 2:20} means from 1 byte distance, corrections are 1 and 255, etc.
                to_return.Add((key, dif));
                last_append = i;
            }
        }
        return to_return;
    }

    public static (byte[], int) GetErrorEncodingBytes(List<(int key, int value)> a, int offset)
    {
        List<byte> to_return = new List<byte>();
        int offset_return = offset;
        for (int i = 0; i < a.Count; i++)
        {
            to_return.AddRange(BitConverter.GetBytes(a[i].key + offset));
            to_return.AddRange(BitConverter.GetBytes((Int16)a[i].value));
            offset_return += a[i].key;
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
    
    public static List<List<T>> SplitToN<T>(List<T> values, int size)
    {
        if(size <= 0)
        {
            throw new ArgumentException("Chunk size must be greater than 0.", nameof(size));
        }
        
        var result = new List<List<T>>();
        for (int i = 0; i < values.Count; i += size)
        {
            var chunk = values.GetRange(i, Math.Min(size, values.Count - i));
            result.Add(chunk);
        }
        return result;
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
