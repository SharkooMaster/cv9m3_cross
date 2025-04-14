
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

        ParallelOptions options = new () { MaxDegreeOfParallelism = 4 };
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

        // Compare results
        await ClmsHandler.AddEventToRoutePoint(headID, new M_CLMSEvent(){
            level = "1", stepName = "PostProcessing", type = "step", message = "Comparing results"
        });
        Console.WriteLine($"|Compare res|: final_res_len: {chunk_results.Count}");
        List<QueryResponseObject> final_results = Misc.CreateList(vectors.Count, () => new QueryResponseObject());
        List<Dictionary<int, int>> final_error_results = Misc.CreateList(vectors.Count, () => new Dictionary<int, int>());
        for (int i = 0; i < chunk_results.Count; i++)
        {
            if(chunk_results[i].Count == 0){ Console.WriteLine($"ERROR: Gateway didnt return a response for this index [{i}]"); }
            else if(chunk_results[i].Count == 1)
            {
                final_results[i] = chunk_results[i][0];
            }
            else
            {
                Dictionary<int, int> error_encoding = new Dictionary<int, int>();
                int _error_count = Globals.chunkSize;
                int _best_index = -1;
                for(int j = 0; j < chunk_results[i].Count; j++)
                {
                    Dictionary<int, int> temp_error_encoding = Misc.GetErrorEncoding(fileChunks[i], chunk_results[i][j].Chunk.ToByteArray());
                    if(temp_error_encoding.Count < _error_count)
                    {
                        _best_index = j;
                        error_encoding = temp_error_encoding;
                    }
                }
                final_results[i] = chunk_results[i][_best_index];
                final_error_results[i] = error_encoding;
            }
        }

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

    public async Task<byte[]> DecompressFile(byte[] _file)
    {
        return new byte[10];
    }
}
