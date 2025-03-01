
using System.Collections.Concurrent;
using System.Diagnostics;
using Cross.Interfaces.Cross;
using Cross.Utilities;
using GatewayService;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;

namespace Cross.Services.Cross;

public class CrossService : ICross
{
    public async Task<byte[]> CompressFile(byte[] _file)
    {
        Stopwatch sw = Stopwatch.StartNew();
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
            qo.Chunk = ByteString.CopyFrom(fileChunks[i]);
            qo.Index = i;
            qo.IsNeighbour = false;
            request.QueryObjects.Add(qo);
        }

        Console.WriteLine($"Searching for chunks");
        Stopwatch sw_search = new Stopwatch();
        sw_search.Start();
        QueryResponse response = await Globals.searchAllServiceClient.SearchAllAsync(request);
        sw_search.Stop();
        Console.WriteLine($"Search complete in {sw.ElapsedMilliseconds}ms");

        // Sort results
        Console.WriteLine($"|Sort res|: final_res_len: {response.Results.Count}");
        List<List<QueryResponseObject>> chunk_results = Misc.CreateList(vectors.Count, () => new List<QueryResponseObject>());
        for (int i = 0; i < response.Results.Count; i++)
        {
            Console.WriteLine($"index: {response.Results[i].Index}:{chunk_results.Count}");
            chunk_results[response.Results[i].Index].Add(response.Results[i]);
        }

        // Compare results
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
        sw.Stop();
        Console.WriteLine($"Total compression time: {sw.ElapsedMilliseconds}ms");
        return output_bytes.ToArray();
    }

    public async Task<byte[]> DecompressFile(byte[] _file)
    {
        return new byte[10];
    }
}
