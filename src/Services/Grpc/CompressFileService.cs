

using Cross.Services.Cross;
using Cross.Utilities;
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
        return 1024L * 1024 * 1024 * 100; // 100 GiB default (windowed handles the RAM)
    }

    /// <summary>
    /// Files above this threshold use windowed v4.0.0 compression.
    /// Below this, the monolithic v3.0.0 path is used.
    /// </summary>
    private static long GetWindowedThreshold()
    {
        var raw = Environment.GetEnvironmentVariable("CROSS_WINDOWED_THRESHOLD_MB");
        if (long.TryParse(raw, out var mb) && mb > 0) return mb * 1024 * 1024;
        return 256L * 1024 * 1024; // 256 MiB default
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

    // ═══════════════════════════════════════════════════════════════════
    //  STREAMING DECOMPRESSION
    // ═══════════════════════════════════════════════════════════════════
    public override async Task DecompressFileStream(
        IAsyncStreamReader<FileUploadRequest> requestStream,
        IServerStreamWriter<FileUploadResponse> responseStream,
        ServerCallContext context)
    {
        string tempPath = Path.Combine(Path.GetTempPath(), $"cross-decompress-{Guid.NewGuid():N}.bin");
        long receivedBytes = 0;
        long maxUploadBytes = GetMaxUploadBytes();

        try
        {
            // ── 1. Receive .ccf via stream → temp file ──
            {
                await using var fs = new FileStream(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.None,
                    bufferSize: 1024 * 1024,
                    useAsync: true);

                while (await requestStream.MoveNext(context.CancellationToken))
                {
                    var msg = requestStream.Current;

                    // Skip metadata messages (we don't need file info for decompression)
                    if (msg.PayloadCase == FileUploadRequest.PayloadOneofCase.Metadata)
                        continue;
                    if (msg.PayloadCase != FileUploadRequest.PayloadOneofCase.Chunk)
                        continue;

                    var chunk = msg.Chunk;
                    if (chunk.Data == null || chunk.Data.Length == 0)
                    {
                        if (chunk.Eof) break;
                        continue;
                    }

                    byte[] data = chunk.Data.ToByteArray();
                    await fs.WriteAsync(data, 0, data.Length, context.CancellationToken);
                    receivedBytes += data.Length;

                    if (receivedBytes > maxUploadBytes)
                        throw new RpcException(new Status(StatusCode.InvalidArgument,
                            $"File too large. Received {receivedBytes} bytes, max {maxUploadBytes}."));

                    if (chunk.Eof) break;
                }
                await fs.FlushAsync(context.CancellationToken);
            }

            Console.WriteLine($"[DecompressStream] Received {receivedBytes} bytes → temp file");

            // ── 2. Detect format: v4.0.0 windowed or v3.0.0/v2.x monolithic ──
            byte[] magicBytes = new byte[4];
            {
                await using var peekFs = new FileStream(tempPath, FileMode.Open, FileAccess.Read,
                                  FileShare.Read, bufferSize: 4096);
                _ = await peekFs.ReadAsync(magicBytes, 0, 4, context.CancellationToken);
            }

            bool isV4 = Cross.Services.Cross.CrossService.IsV4Format(magicBytes);
            Console.WriteLine($"[DecompressStream] Format={( isV4 ? "v4.0.0 windowed" : "v3.0.0/v2.x monolithic")}");

            var crossService = new Cross.Services.Cross.CrossService();

            if (isV4)
            {
                // ═══════════════════════════════════════════════════════════
                //  WINDOWED v4.0.0 DECOMPRESSION — O(windowSize) RAM
                // ═══════════════════════════════════════════════════════════
                string decompressedTempPath = Path.Combine(Path.GetTempPath(), $"cross-v4dec-{Guid.NewGuid():N}.bin");
                try
                {
                    long decompressedSize = await crossService.DecompressFileWindowedAsync(
                        tempPath, decompressedTempPath, context.CancellationToken);

                    // Stats
                    byte[] decompressedHash;
                    using (var sha = SHA256.Create())
                    await using (var hashFs = new FileStream(decompressedTempPath, FileMode.Open, FileAccess.Read,
                                     FileShare.Read, bufferSize: 1024 * 1024, useAsync: true))
                    {
                        decompressedHash = await sha.ComputeHashAsync(hashFs, context.CancellationToken);
                    }

                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        DecompressStats = new DecompressionStats
                        {
                            CompressedSize = (ulong)receivedBytes,
                            DecompressedSize = (ulong)decompressedSize,
                            DecompressedSha256 = ByteString.CopyFrom(decompressedHash)
                        }
                    });

                    // Stream from temp file
                    const int outChunkSize = 1024 * 1024;
                    uint seq = 0;
                    await using (var decFs = new FileStream(decompressedTempPath, FileMode.Open, FileAccess.Read,
                                     FileShare.Read, bufferSize: 1024 * 1024, useAsync: true))
                    {
                        byte[] buf = new byte[outChunkSize];
                        int read;
                        while ((read = await decFs.ReadAsync(buf, 0, outChunkSize, context.CancellationToken)) > 0)
                        {
                            await responseStream.WriteAsync(new FileUploadResponse
                            {
                                Chunk = new FileChunk
                                {
                                    Seq = seq++,
                                    Data = ByteString.CopyFrom(buf, 0, read),
                                    Eof = false
                                }
                            });
                        }
                    }

                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        Chunk = new FileChunk { Seq = seq, Data = ByteString.Empty, Eof = true }
                    });
                }
                finally
                {
                    try { if (File.Exists(decompressedTempPath)) File.Delete(decompressedTempPath); } catch { }
                }
            }
            else
            {
                // ═══════════════════════════════════════════════════════════
                //  MONOLITHIC v3.0.0/v2.x — original path, unchanged
                // ═══════════════════════════════════════════════════════════
                byte[] ccfBytes = await File.ReadAllBytesAsync(tempPath, context.CancellationToken);
                byte[] decompressedBytes = await crossService.DecompressFile(ccfBytes);

                Console.WriteLine($"[DecompressStream] Decompressed {ccfBytes.Length} → {decompressedBytes.Length} bytes");

                byte[] decompressedHash;
                using (var sha = SHA256.Create())
                    decompressedHash = sha.ComputeHash(decompressedBytes);

                await responseStream.WriteAsync(new FileUploadResponse
                {
                    DecompressStats = new DecompressionStats
                    {
                        CompressedSize = (ulong)ccfBytes.Length,
                        DecompressedSize = (ulong)decompressedBytes.Length,
                        DecompressedSha256 = ByteString.CopyFrom(decompressedHash)
                    }
                });

                const int outChunkSize = 1024 * 1024;
                uint seq = 0;
                for (int offset = 0; offset < decompressedBytes.Length; offset += outChunkSize, seq++)
                {
                    int len = Math.Min(outChunkSize, decompressedBytes.Length - offset);
                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        Chunk = new FileChunk
                        {
                            Seq = seq,
                            Data = ByteString.CopyFrom(decompressedBytes, offset, len),
                            Eof = false
                        }
                    });
                }

                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Chunk = new FileChunk { Seq = seq, Data = ByteString.Empty, Eof = true }
                });
            }
        }
        catch (RpcException)
        {
            throw;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DecompressStream] FAILED: {ex.Message}");
            throw new RpcException(new Status(StatusCode.Internal,
                $"Decompression stream failed: {ex.Message}"));
        }
        finally
        {
            try
            {
                if (File.Exists(tempPath))
                    File.Delete(tempPath);
            }
            catch { /* best-effort cleanup */ }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  STREAMING COMPRESSION
    // ═══════════════════════════════════════════════════════════════════
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

            var crossService = new Cross.Services.Cross.CrossService();
            long windowedThreshold = GetWindowedThreshold();
            bool useWindowed = receivedBytes > windowedThreshold;

            Console.WriteLine($"[ProcessFileStream] {receivedBytes} bytes, windowed={useWindowed} (threshold={windowedThreshold})");

            if (useWindowed)
            {
                // ═══════════════════════════════════════════════════════════
                //  WINDOWED v4.0.0 — O(windowSize) RAM, unlimited file size
                // ═══════════════════════════════════════════════════════════
                string compressedTempPath = Path.Combine(Path.GetTempPath(), $"cross-v4out-{Guid.NewGuid():N}.bin");
                try
                {
                    var (compressedSize, refsFound, chunks) =
                        await crossService.CompressFileWindowedAsync(tempPath, compressedTempPath, context.CancellationToken);

                    byte[] compressedHash;
                    using (var compressedSha = SHA256.Create())
                    await using (var hashFs = new FileStream(compressedTempPath, FileMode.Open, FileAccess.Read,
                                     FileShare.Read, bufferSize: 1024 * 1024, useAsync: true))
                    {
                        compressedHash = await compressedSha.ComputeHashAsync(hashFs, context.CancellationToken);
                    }

                    // Stats
                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        Stats = new CompressionStats
                        {
                            OriginalSize = (ulong)receivedBytes,
                            CompressedSize = (ulong)compressedSize,
                            ReferencesFound = (uint)Math.Max(0, refsFound),
                            TotalChunks = (uint)Math.Max(0, chunks),
                            CompressedSha256 = ByteString.CopyFrom(compressedHash)
                        }
                    });

                    // Stream compressed output from temp file
                    const int outChunkSize = 1024 * 1024;
                    uint seq = 0;
                    await using (var compressedFs = new FileStream(compressedTempPath, FileMode.Open, FileAccess.Read,
                                     FileShare.Read, bufferSize: 1024 * 1024, useAsync: true))
                    {
                        byte[] buf = new byte[outChunkSize];
                        int read;
                        while ((read = await compressedFs.ReadAsync(buf, 0, outChunkSize, context.CancellationToken)) > 0)
                        {
                            await responseStream.WriteAsync(new FileUploadResponse
                            {
                                Chunk = new FileChunk
                                {
                                    Seq = seq++,
                                    Data = ByteString.CopyFrom(buf, 0, read),
                                    Eof = false
                                }
                            });
                        }
                    }

                    // EOF
                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        Chunk = new FileChunk { Seq = seq, Data = ByteString.Empty, Eof = true }
                    });
                }
                finally
                {
                    try { if (File.Exists(compressedTempPath)) File.Delete(compressedTempPath); } catch { }
                }
            }
            else
            {
                // ═══════════════════════════════════════════════════════════
                //  MONOLITHIC v3.0.0 — original path, unchanged
                // ═══════════════════════════════════════════════════════════
                byte[] fileBytes = await File.ReadAllBytesAsync(tempPath, context.CancellationToken);

                var (compressedBytes, referencesFound, totalChunks) = await crossService.CompressFileWithStats(fileBytes);

                byte[] compressedHash;
                using (var compressedSha = SHA256.Create())
                {
                    compressedHash = compressedSha.ComputeHash(compressedBytes);
                }

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

                const int outChunkSize = 1024 * 1024;
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
                    Chunk = new FileChunk { Seq = seq, Data = ByteString.Empty, Eof = true }
                });
            }
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
