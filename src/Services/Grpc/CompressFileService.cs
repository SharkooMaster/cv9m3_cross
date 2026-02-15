

using Cross.Services.Cross;
using CrossService;
using Google.Protobuf;
using Grpc.Core;
using System.Security.Cryptography;

public class CompressFileService : FileService.FileServiceBase
{
    private static long GetMaxUploadBytes()
    {
        var raw = Environment.GetEnvironmentVariable("CROSS_MAX_UPLOAD_BYTES");
        if (long.TryParse(raw, out var v) && v > 0) return v;
        return 1024L * 1024 * 1024; // 1 GiB default
    }

    public override async Task<FileResponse> ProcessFile(FileRequest request, ServerCallContext context)
    {
        Console.WriteLine("REQUEST RECIEVED");
        Cross.Services.Cross.CrossService crossService = new Cross.Services.Cross.CrossService();

        FileResponse to_return = new FileResponse();
        to_return.FileContent = ByteString.CopyFrom(await crossService.CompressFile(request.FileContent.ToByteArray()));
        return to_return;
    }

    public override async Task<FileResponse> DecompressFile(FileRequest request, ServerCallContext context)
    {
        var crossService = new Cross.Services.Cross.CrossService();
        var decompressed = await crossService.DecompressFile(request.FileContent.ToByteArray());
        return new FileResponse
        {
            FileContent = ByteString.CopyFrom(decompressed)
        };
    }

    public override async Task ProcessFileStream(
        IAsyncStreamReader<FileUploadRequest> requestStream,
        IServerStreamWriter<FileUploadResponse> responseStream,
        ServerCallContext context)
    {
        string? fileName = null;
        ulong declaredSize = 0;
        uint clientChunkSize = 0;
        ByteString? declaredSha256 = null;

        string tempPath = Path.Combine(Path.GetTempPath(), $"cross-upload-{Guid.NewGuid():N}.bin");
        long receivedBytes = 0;
        long maxUploadBytes = GetMaxUploadBytes();

        try
        {
            byte[] uploadedSha256;
            {
                await using var fs = new FileStream(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 1024 * 1024,
                    useAsync: true);

                using var sha = SHA256.Create();

                while (await requestStream.MoveNext(context.CancellationToken))
                {
                    var msg = requestStream.Current;
                    if (msg.PayloadCase == FileUploadRequest.PayloadOneofCase.Metadata)
                    {
                        fileName = msg.Metadata.FileName;
                        declaredSize = msg.Metadata.OriginalSize;
                        clientChunkSize = msg.Metadata.ChunkSize;
                        declaredSha256 = msg.Metadata.Sha256;

                        if (declaredSize > 0 && declaredSize > (ulong)maxUploadBytes)
                            throw new RpcException(new Status(StatusCode.InvalidArgument, $"File too large. Declared {declaredSize} bytes, max allowed {maxUploadBytes}."));
                        continue;
                    }

                    if (msg.PayloadCase != FileUploadRequest.PayloadOneofCase.Chunk)
                        continue;

                    var chunk = msg.Chunk;
                    if (chunk.Data == null || chunk.Data.Length == 0)
                    {
                        if (chunk.Eof)
                            break;
                        continue;
                    }

                    byte[] data = chunk.Data.ToByteArray();
                    await fs.WriteAsync(data, 0, data.Length, context.CancellationToken);
                    sha.TransformBlock(data, 0, data.Length, null, 0);
                    receivedBytes += data.Length;
                    if (receivedBytes > maxUploadBytes)
                        throw new RpcException(new Status(StatusCode.InvalidArgument, $"File too large. Received {receivedBytes} bytes, max allowed {maxUploadBytes}."));

                    if (chunk.Eof)
                        break;
                }

                sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                await fs.FlushAsync(context.CancellationToken);
                uploadedSha256 = sha.Hash ?? Array.Empty<byte>();
            }

            if (declaredSize > 0 && (ulong)receivedBytes != declaredSize)
                throw new RpcException(new Status(StatusCode.InvalidArgument, $"Declared size {declaredSize} does not match received {receivedBytes}."));

            if (declaredSha256 != null && declaredSha256.Length == 32)
            {
                if (!declaredSha256.Span.SequenceEqual(uploadedSha256))
                    throw new RpcException(new Status(StatusCode.InvalidArgument, "SHA-256 mismatch for uploaded file."));
            }

            byte[] fileBytes = await File.ReadAllBytesAsync(tempPath, context.CancellationToken);

            var crossService = new Cross.Services.Cross.CrossService();
            var (compressedBytes, referencesFound, totalChunks) = await crossService.CompressFileWithStats(fileBytes);

            byte[] compressedHash;
            using (var compressedSha = SHA256.Create())
            {
                compressedHash = compressedSha.ComputeHash(compressedBytes);
            }

            // Send stats first so callers can set headers before streaming bytes.
            await responseStream.WriteAsync(new FileUploadResponse
            {
                Stats = new CompressionStats
                {
                    OriginalSize = (ulong)fileBytes.Length,
                    CompressedSize = (ulong)compressedBytes.Length,
                    ReferencesFound = (uint)Math.Max(0, referencesFound),
                    TotalChunks = (uint)Math.Max(0, totalChunks),
                    CompressedSha256 = ByteString.CopyFrom(compressedHash)
                }
            });

            const int outChunkSize = 1024 * 1024; // 1 MiB
            uint seq = 0;
            for (int offset = 0; offset < compressedBytes.Length; offset += outChunkSize, seq++)
            {
                int len = Math.Min(outChunkSize, compressedBytes.Length - offset);
                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Chunk = new FileChunk
                    {
                        Seq = seq,
                        Data = ByteString.CopyFrom(compressedBytes, offset, len),
                        Eof = false
                    }
                });
            }

            await responseStream.WriteAsync(new FileUploadResponse
            {
                Chunk = new FileChunk
                {
                    Seq = seq,
                    Data = ByteString.Empty,
                    Eof = true
                }
            });
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            throw new RpcException(new Status(StatusCode.Internal, $"Compression stream failed: {ex.Message}"));
        }
        finally
        {
            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch
            {
                // Best-effort cleanup.
            }
        }
    }
}
