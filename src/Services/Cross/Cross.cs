
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using Cross.Interfaces.Cross;
using Cross.Modules;
using Cross.Utilities;
using GatewayService;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Cross.Services.Cross;

public class CrossService : ICross
{
    string headID = "";
    List<byte[]> fileChunks = new List<byte[]>();
    List<byte> trimmedChunk = new List<byte>();

    private async Task initCLMS(string _name, string _id)
    {
        headID = await ClmsHandler.RegisterHeadRoute();
        await ClmsHandler.RegisterRoutePoint(headID, _name, _id);
    }

    private async Task addEvent(string _step, string _message, string _type = "step", string _level = "1")
    {
        await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = _level, stepName = _step, type = _type, message = _message
        });
    }

    private void SplitChunks(byte[] _bytes, int _chunkSize)
    {
        fileChunks = Misc.SplitFile(_bytes, _chunkSize);
        trimmedChunk.AddRange(fileChunks.Last());
        fileChunks.RemoveAt(fileChunks.Count - 1);
    }

    public async Task<byte[]> CompressFile(byte[] _file)
    {
        // CLMS
        await initCLMS("Cross", "A2");
        await addEvent("Preprocessing", "Starting compression process. Splitting file");

        Stopwatch sw = Stopwatch.StartNew();
        // Divide into (N) chunks.
        SplitChunks(_file, Globals.chunkSize);

        // Vectorize
        await addEvent("Preprocessing", "Vectorizing chunks");
        List<float[]> vectors = Misc.Compute64ElementLSHVectors(fileChunks);

        // Extract bitstring
        await addEvent("Preprocessing", "Extracting (bucket id) bitstring");
        List<string> bitStrings = Misc.ComputeBitStringFromVectors(vectors);

        // Search
        await addEvent("Preprocessing", "Preparing query request");

        List<QueryObject> queryObjects = new List<QueryObject>();
        for (int i = 0; i < vectors.Count; i++)
        {
            QueryObject qo = new QueryObject() { BucketString = bitStrings[i] };
            qo.Vector.AddRange(vectors[i]);
            qo.Chunk = ByteString.CopyFrom(fileChunks[i]);
            qo.Index = i;
            qo.IsNeighbour = false;
            queryObjects.Add(qo);
        }

        await addEvent("Searching", "Sending search request");
        ConcurrentBag<QueryResponseObject> queryResponseObjects = new ConcurrentBag<QueryResponseObject>();

        ParallelOptions options = new () { MaxDegreeOfParallelism = 14 };
        await Parallel.ForAsync(0, queryObjects.Count, options, async (i, ct) => {
            QueryRequest request = new QueryRequest();
            request.HeadRouteID = headID;
            request.QueryObjects.Add(queryObjects[i]);

            QueryResponse response = await Globals.searchAllServiceClient.SearchAllAsync(request);
            for (int j = 0; j < response.Results.Count; j++)
            {
                queryResponseObjects.Add(response.Results[j]);
            }
        });

        // Sort results
        await addEvent("PostProcessing", "Sorting results");

        List<List<QueryResponseObject>> chunk_results = Misc.CreateList(vectors.Count, () => new List<QueryResponseObject>());
        foreach (var responseObject in queryResponseObjects)
        {
            chunk_results[responseObject.Index].Add(responseObject);
        }

        _ = ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent()
        {
            level = "1",
            stepName = "PostProcessing",
            type = "step",
            message = "Comparing results"
        });
        Console.WriteLine($"|Compare res|: final_res_len: {chunk_results.Count}");

        List<QueryResponseObject> final_results = Misc.CreateList(vectors.Count, () => new QueryResponseObject());
        List<Dictionary<int, int>> final_error_results = Misc.CreateList(vectors.Count, () => new Dictionary<int, int>());

        var parallelOptions = new ParallelOptions
        {
            MaxDegreeOfParallelism = Environment.ProcessorCount
        };

        await Parallel.ForEachAsync(
            Enumerable.Range(0, chunk_results.Count),
            parallelOptions,
            (i, ct) =>
            {
                var candidates = chunk_results[i];

                if (candidates.Count == 0)
                {
                    Console.WriteLine($"ERROR: Gateway didnt return a response for this index [{i}]");
                }
                else if (candidates.Count == 1)
                {
                    final_results[i] = candidates[0];
                }
                else
                {
                    // 1) Pre-convert all ByteStrings to byte[]
                    var chunkBytesArray = candidates
                        .Select(r => r.Chunk.ToByteArray())
                        .ToArray();

                    // 2) First pass: find best by error COUNT only
                    int bestErrorCount = Globals.chunkSize;
                    int bestIndex = -1;
                    for (int j = 0; j < chunkBytesArray.Length; j++)
                    {
                        var count = Misc.GetErrorEncoding(
                            fileChunks[i],
                            chunkBytesArray[j]
                        ).Count;

                        if (count < bestErrorCount)
                        {
                            bestErrorCount = count;
                            bestIndex = j;
                        }
                    }

                    // 3) Second pass: full encoding for the best candidate
                    var bestEncoding = Misc.GetErrorEncoding(
                        fileChunks[i],
                        chunkBytesArray[bestIndex]
                    );

                    final_results[i] = candidates[bestIndex];
                    final_error_results[i] = bestEncoding;
                }

                return ValueTask.CompletedTask;
            }
        );

        // Encode results
        await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = "1", stepName = "Encoding", type = "step", message = "Encoding results"
        });
        Console.WriteLine($"|Encode res|: final_res_len: {final_results.Count}");
        List<M_EncodedResult> encoded_objects = new List<M_EncodedResult>();
        for (int i = 0; i < final_results.Count; i++)
        {
            encoded_objects.Add(new M_EncodedResult(){
                bucket_id = final_results[i].Id,
                row_id = final_results[i].IdPost,
                error_encoding = final_error_results[i]
            });
        }

        List<byte> first_bytes = new List<byte>();
        List<byte> error_bytes = new List<byte>();
        int error_bytes_offset = 0;
        for (int i = 0; i < encoded_objects.Count; i++)
        {
            first_bytes.AddRange(BitConverter.GetBytes(encoded_objects[i].bucket_id));
            first_bytes.AddRange(BitConverter.GetBytes(encoded_objects[i].row_id));
            (byte[], int) _errors = Misc.GetErrorEncodingBytes(encoded_objects[i].error_encoding, error_bytes_offset);
            error_bytes_offset = _errors.Item2;
            error_bytes.AddRange(_errors.Item1);
        }

        // Add Dictionary and trimming
        await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = "1", stepName = "Encoding", type = "step", message = "Adding Dictionary and trimming"
        });
        Console.WriteLine($"|Add Dict|: first_bytes: {first_bytes.Count}, error_bytes: {error_bytes.Count}");
        List<byte> output_bytes =
        [
            .. BitConverter.GetBytes((long)first_bytes.Count),
            .. BitConverter.GetBytes((long)error_bytes.Count),
            .. first_bytes,
            .. error_bytes,
            .. trimmedChunk
        ];

        // Return
        await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = "1", stepName = "Final", type = "step", message = "Returning compressed file"
        });
        ClmsHandler._instance.routePoints[headID].Status = "Success";
        await ClmsHandler.SendRoutePoint(headID);

        sw.Stop();
        Console.WriteLine($"Total compression time: {sw.ElapsedMilliseconds}ms");
        return output_bytes.ToArray();
    }

    public async Task<byte[]> DecompressFile(byte[] file)
    {
        const int LongSize = sizeof(long);
        const int ULongSize = sizeof(ulong);
        const int IntSize = sizeof(int);

        if (file == null || file.Length < 2 * LongSize)
            throw new ArgumentException("Input too short to contain sizes", nameof(file));

        long refSize = BitConverter.ToInt64(file, 0);
        long errSize = BitConverter.ToInt64(file, LongSize);

        int refOffset = 2 * LongSize;
        int errOffset = refOffset + (int)refSize;
        if (file.Length < errOffset + errSize)
            throw new ArgumentException("Declared sizes exceed file length", nameof(file));

        var bucketRow = new Dictionary<ulong, ulong>();
        for (int i = 0; i + 2 * ULongSize <= refSize; i += 2 * ULongSize)
        {
            ulong bucketId = BitConverter.ToUInt64(file, refOffset + i);
            ulong rowId = BitConverter.ToUInt64(file, refOffset + i + ULongSize);
            bucketRow[bucketId] = rowId;
        }

        var errorEncoding = new Dictionary<int, int>();
        for (int i = 0; i + 2 * IntSize <= errSize; i += 2 * IntSize)
        {
            int index = BitConverter.ToInt32(file, errOffset + i);
            int offset = BitConverter.ToInt32(file, errOffset + i + IntSize);
            errorEncoding[index] = offset;
        }

        // Get chunks at bucket_id, row_id
        // Correct using error_encodings
        // Return
        return new byte[10];
    }
}
