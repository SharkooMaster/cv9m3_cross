

using Cross.Services.Cross;
using Cross.Utilities;
using CrossService;
using Google.Protobuf;
using Grpc.Core;
using System.Security.Cryptography;

/// <summary>
/// Stream wrapper that writes to a gRPC response stream.
/// Used for true streaming compression/decompression.
/// </summary>
internal class GrpcResponseStream : Stream
{
    private readonly IServerStreamWriter<FileUploadResponse> _responseStream;
    private readonly CancellationToken _ct;
    private uint _seq = 0;
    private readonly int _chunkSize;

    public GrpcResponseStream(IServerStreamWriter<FileUploadResponse> responseStream, CancellationToken ct, int chunkSize = 1024 * 1024)
    {
        _responseStream = responseStream;
        _ct = ct;
        _chunkSize = chunkSize;
    }

    public override bool CanRead => false;
    public override bool CanSeek => false;
    public override bool CanWrite => true;
    public override long Length => throw new NotSupportedException();
    public override long Position { get => throw new NotSupportedException(); set => throw new NotSupportedException(); }

    public override void Flush() => FlushAsync(_ct).GetAwaiter().GetResult();
    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        // gRPC streams are automatically flushed on WriteAsync
        await Task.CompletedTask;
    }

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count)
        => WriteAsync(buffer, offset, count, _ct).GetAwaiter().GetResult();

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        int remaining = count;
        int pos = offset;

        while (remaining > 0)
        {
            int chunk = Math.Min(remaining, _chunkSize);
            await _responseStream.WriteAsync(new FileUploadResponse
            {
                Chunk = new FileChunk
                {
                    Seq = _seq++,
                    Data = ByteString.CopyFrom(buffer, pos, chunk),
                    Eof = false
                }
            }, cancellationToken);

            remaining -= chunk;
            pos += chunk;
        }
    }
}

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

    /// <summary>
    /// Returns the effective max upload size for a given file size.
    /// If file will use windowed compression, returns unlimited (1 PB).
    /// Otherwise, returns the configured monolithic limit.
    /// </summary>
    private static long GetEffectiveMaxUploadBytes(ulong declaredSize)
    {
        long windowedThreshold = GetWindowedThreshold();
        if (declaredSize > (ulong)windowedThreshold)
        {
            // Windowed path → unlimited (1 PB)
            return 1024L * 1024 * 1024 * 1024 * 1024; // 1 PB
        }
        // Monolithic path → use configured limit
        return GetMaxUploadBytes();
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

        try
        {
            // ── 1. Receive .ccf via stream → temp file ──
            // NOTE: No size limit for decompression. Format is detected after upload.
            // v4.0.0 windowed files can be unlimited size (windowed handles RAM).
            // v3.0.0 monolithic files are usually much smaller (compressed).
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
                //  WINDOWED v4.0.0 STREAMING DECOMPRESSION — O(windowSize) RAM
                //  Decompresses and streams simultaneously (no temp file for output)
                // ═══════════════════════════════════════════════════════════
                var grpcStream = new GrpcResponseStream(responseStream, context.CancellationToken);

                // Decompress and stream directly to client (hash computed and verified internally)
                var (decompressedSize, decompressedHash) = await crossService.DecompressFileWindowedStreamAsync(
                    tempPath, grpcStream, context.CancellationToken);

                // Stats (send after decompression completes)
                await responseStream.WriteAsync(new FileUploadResponse
                {
                    DecompressStats = new DecompressionStats
                    {
                        CompressedSize = (ulong)receivedBytes,
                        DecompressedSize = (ulong)decompressedSize,
                        DecompressedSha256 = ByteString.CopyFrom(decompressedHash)
                    }
                });

                // EOF
                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
                });
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
            Task? compressionTask = null;
            var crossService = new Cross.Services.Cross.CrossService();
            long windowedThreshold = GetWindowedThreshold();
            bool willUseWindowed = false; // Will be set when metadata arrives
            
            {
                // Use FileShare.Read to allow compression to read while we're still writing
                await using var fs = new FileStream(
                    tempPath,
                    FileMode.CreateNew,
                    FileAccess.Write,
                    FileShare.Read,  // Allow reading while writing for streaming compression
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

                        // CRITICAL: Update willUseWindowed now that we know the file size!
                        willUseWindowed = declaredSize > (ulong)windowedThreshold;

                        Console.WriteLine($"[ProcessFileStream] Received metadata: fileName={fileName}, size={declaredSize} bytes, willUseWindowed={willUseWindowed}");

                        // Use conditional limit: windowed files get unlimited, monolithic get the configured limit
                        long effectiveLimit = GetEffectiveMaxUploadBytes(declaredSize);
                        if (declaredSize > 0 && declaredSize > (ulong)effectiveLimit)
                            throw new RpcException(new Status(StatusCode.InvalidArgument, $"File too large. Declared {declaredSize} bytes, max allowed {effectiveLimit}."));
                        // Update maxUploadBytes for the streaming check below
                        maxUploadBytes = effectiveLimit;
                        Console.WriteLine($"[ProcessFileStream] Effective limit: {effectiveLimit} bytes, windowed={willUseWindowed}, starting upload...");
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
                    
                    // Flush frequently so compression can read the data
                    if (willUseWindowed && compressionTask != null && receivedBytes % (10 * 1024 * 1024) < data.Length)
                    {
                        await fs.FlushAsync(context.CancellationToken);
                    }
                    
                    // Start compression in parallel once we have first window (for windowed files)
                    if (willUseWindowed && compressionTask == null && receivedBytes >= windowedThreshold)
                    {
                        Console.WriteLine($"[ProcessFileStream] ✅ Starting parallel compression NOW (received {receivedBytes / (1024.0 * 1024.0):F1} MB, threshold={windowedThreshold / (1024.0 * 1024.0):F1} MB)...");
                        compressionTask = Task.Run(async () =>
                        {
                            try
                            {
                                Console.WriteLine($"[ProcessFileStream] Compression task started, waiting 500ms for file handle...");
                                // Wait a bit for file handle to be available, then start compressing
                                await Task.Delay(500, context.CancellationToken);
                                Console.WriteLine($"[ProcessFileStream] Compression task: Starting CompressFileWindowedStreamAsync (file={tempPath}, declaredSize={declaredSize})...");
                                
                                // Verify file exists and is readable
                                if (!File.Exists(tempPath))
                                {
                                    Console.WriteLine($"[ProcessFileStream] ERROR: File {tempPath} does not exist!");
                                    throw new FileNotFoundException($"Temp file not found: {tempPath}");
                                }
                                
                                long fileSize = new FileInfo(tempPath).Length;
                                Console.WriteLine($"[ProcessFileStream] Compression task: File exists, size={fileSize} bytes");
                                
                                using var compSha = SHA256.Create();
                                var grpcStream = new GrpcResponseStream(responseStream, context.CancellationToken);
                                using var hashStream = new CryptoStream(grpcStream, compSha, CryptoStreamMode.Write);

                                Console.WriteLine($"[ProcessFileStream] Compression task: Calling CompressFileWindowedStreamAsync...");
                                var (compressedSize, refsFound, chunks) =
                                    await crossService.CompressFileWindowedStreamAsync(tempPath, hashStream, declaredSize > 0 ? (long)declaredSize : fileSize, context.CancellationToken);
                                Console.WriteLine($"[ProcessFileStream] Compression task: CompressFileWindowedStreamAsync completed!");

                                await hashStream.FlushFinalBlockAsync(context.CancellationToken);
                                byte[] compressedHash = compSha.Hash!;

                                // Get actual file size (may have grown since task started)
                                long actualFileSize = new FileInfo(tempPath).Length;

                                await responseStream.WriteAsync(new FileUploadResponse
                                {
                                    Stats = new CompressionStats
                                    {
                                        OriginalSize = (ulong)actualFileSize,
                                        CompressedSize = (ulong)compressedSize,
                                        ReferencesFound = (uint)Math.Max(0, refsFound),
                                        TotalChunks = (uint)Math.Max(0, chunks),
                                        CompressedSha256 = ByteString.CopyFrom(compressedHash)
                                    }
                                });

                                await responseStream.WriteAsync(new FileUploadResponse
                                {
                                    Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
                                });
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"[ProcessFileStream] Compression error: {ex.Message}");
                                throw;
                            }
                        }, context.CancellationToken);
                    }
                    
                    // Progress logging every 100 MB
                    if (receivedBytes % (100 * 1024 * 1024) < data.Length)
                    {
                        double percent = declaredSize > 0 ? (receivedBytes * 100.0 / declaredSize) : 0;
                        Console.WriteLine($"[ProcessFileStream] Receiving: {receivedBytes / (1024.0 * 1024.0):F1} MB / {declaredSize / (1024.0 * 1024.0):F1} MB ({percent:F1}%)");
                    }
                    
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

            bool useWindowed = receivedBytes > windowedThreshold;

            Console.WriteLine($"[ProcessFileStream] {receivedBytes} bytes, windowed={useWindowed} (threshold={windowedThreshold})");

            // If compression started in parallel, wait for it and update stats with final size
            if (compressionTask != null)
            {
                Console.WriteLine($"[ProcessFileStream] Waiting for parallel compression to complete...");
                await compressionTask;
                // Stats were already sent by the compression task, but with potentially stale receivedBytes
                // The compression reads from file so it has the correct size - this is fine
            }
            else if (useWindowed)
            {
                // Compression didn't start in parallel (file was too small or finished too fast)
                // Start it now
                using var sha = SHA256.Create();
                var grpcStream = new GrpcResponseStream(responseStream, context.CancellationToken);
                using var hashStream = new CryptoStream(grpcStream, sha, CryptoStreamMode.Write);

                var (compressedSize, refsFound, chunks) =
                    await crossService.CompressFileWindowedStreamAsync(tempPath, hashStream, receivedBytes, context.CancellationToken);

                await hashStream.FlushFinalBlockAsync(context.CancellationToken);
                byte[] compressedHash = sha.Hash!;

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

                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
                });
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
