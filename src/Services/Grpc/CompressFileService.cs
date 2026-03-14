

using Cross.Utilities;
using CrossService;
using Google.Protobuf;
using Grpc.Core;
using System.Collections.Concurrent;
using System.Security.Cryptography;
using System.Text;
using System.Threading.Channels;

using MyCrossService = Cross.Services.Cross.CrossService;

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

    public override int Read(byte[] buffer, int offset, int count) => throw new NotSupportedException();
    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();
    public override void SetLength(long value) => throw new NotSupportedException();

    // Buffer for small synchronous writes (e.g. BinaryWriter header writes).
    // Avoids blocking threadpool on tiny gRPC messages (4-8 bytes each).
    private byte[]? _syncBuf;
    private int _syncBufLen;
    private const int SyncBufCapacity = 64 * 1024; // 64 KB buffer

    public override void Write(byte[] buffer, int offset, int count)
    {
        // Buffer small synchronous writes to avoid threadpool-blocking gRPC calls
        _syncBuf ??= new byte[SyncBufCapacity];

        if (_syncBufLen + count <= SyncBufCapacity)
        {
            Buffer.BlockCopy(buffer, offset, _syncBuf, _syncBufLen, count);
            _syncBufLen += count;
        }
        else
        {
            // Flush existing buffer + new data together
            FlushSyncBuffer();
            if (count <= SyncBufCapacity)
            {
                Buffer.BlockCopy(buffer, offset, _syncBuf!, 0, count);
                _syncBufLen = count;
            }
            else
            {
                // Larger than buffer — send directly (rare)
                WriteAsync(buffer, offset, count, _ct).GetAwaiter().GetResult();
            }
        }
    }

    private void FlushSyncBuffer()
    {
        if (_syncBufLen > 0 && _syncBuf != null)
        {
            WriteAsync(_syncBuf, 0, _syncBufLen, _ct).GetAwaiter().GetResult();
            _syncBufLen = 0;
        }
    }

    public override void Flush() => FlushSyncBuffer();
    public override async Task FlushAsync(CancellationToken cancellationToken)
    {
        if (_syncBufLen > 0 && _syncBuf != null)
        {
            await WriteAsync(_syncBuf, 0, _syncBufLen, cancellationToken);
            _syncBufLen = 0;
        }
    }

    public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        // Flush any buffered sync writes first
        if (_syncBufLen > 0 && _syncBuf != null)
        {
            byte[] pending = new byte[_syncBufLen];
            Buffer.BlockCopy(_syncBuf, 0, pending, 0, _syncBufLen);
            _syncBufLen = 0;

            int pendRemaining = pending.Length;
            int pendPos = 0;
            while (pendRemaining > 0)
            {
                int chunk = Math.Min(pendRemaining, _chunkSize);
                await _responseStream.WriteAsync(new FileUploadResponse
                {
                    Chunk = new FileChunk
                    {
                        Seq = _seq++,
                        Data = ByteString.CopyFrom(pending, pendPos, chunk),
                        Eof = false
                    }
                }, cancellationToken);
                pendRemaining -= chunk;
                pendPos += chunk;
            }
        }

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
    // ── Concurrency limiter: prevent OOM by limiting parallel compressions ──
    // Each compression can use 500 MB-2 GB RAM. With 16 GiB limit, max 4 concurrent is safe.
    private static readonly int MaxConcurrentCompressions = Math.Max(2,
        int.TryParse(Environment.GetEnvironmentVariable("CROSS_MAX_CONCURRENT"), out var mc) ? mc : 4);
    private static readonly SemaphoreSlim _compressionGate = new(MaxConcurrentCompressions, MaxConcurrentCompressions);

    private static long GetMaxUploadBytes()
    {
        var raw = Environment.GetEnvironmentVariable("CROSS_MAX_UPLOAD_BYTES");
        if (long.TryParse(raw, out var v) && v > 0) return v;
        return 1024L * 1024 * 1024 * 100; // 100 GiB default
    }

    private static long GetWindowedThreshold()
    {
        var raw = Environment.GetEnvironmentVariable("CROSS_WINDOWED_THRESHOLD_MB");
        if (long.TryParse(raw, out var mb) && mb > 0) return mb * 1024 * 1024;
        return 256L * 1024 * 1024; // 256 MiB default
    }

    private static int GetPipelineParallelism()
    {
        var raw = Environment.GetEnvironmentVariable("CROSS_PIPELINE_PARALLELISM");
        if (int.TryParse(raw, out var p) && p > 0) return p;
        return 4; // 4 concurrent window compressions
    }

    private static long GetEffectiveMaxUploadBytes(ulong declaredSize)
    {
        long windowedThreshold = GetWindowedThreshold();
        if (declaredSize > (ulong)windowedThreshold)
            return 1024L * 1024 * 1024 * 1024 * 1024; // 1 PB → unlimited
        return GetMaxUploadBytes();
    }

    public override Task<FileResponse> ProcessFile(FileRequest request, ServerCallContext context)
    {
        throw new RpcException(new Status(StatusCode.Unimplemented,
            "Unary ProcessFile is disabled to prevent OOM. Use ProcessFileStream instead."));
    }

    public override Task<FileResponse> DecompressFile(FileRequest request, ServerCallContext context)
    {
        throw new RpcException(new Status(StatusCode.Unimplemented,
            "Unary DecompressFile is disabled to prevent OOM. Use DecompressFileStream instead."));
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
            {
                await using var fs = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write,
                    FileShare.None, bufferSize: 1024 * 1024, useAsync: true);

                while (await requestStream.MoveNext(context.CancellationToken))
                {
                    var msg = requestStream.Current;
                    if (msg.PayloadCase == FileUploadRequest.PayloadOneofCase.Metadata) continue;
                    if (msg.PayloadCase != FileUploadRequest.PayloadOneofCase.Chunk) continue;

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

            Console.WriteLine($"[DecompressStream] Received {receivedBytes} bytes → temp file, concurrency={MaxConcurrentCompressions - _compressionGate.CurrentCount}/{MaxConcurrentCompressions}");

            // ── 2. Detect format ──
            byte[] magicBytes = new byte[4];
            {
                await using var peekFs = new FileStream(tempPath, FileMode.Open, FileAccess.Read,
                    FileShare.Read, bufferSize: 4096);
                _ = await peekFs.ReadAsync(magicBytes, 0, 4, context.CancellationToken);
            }

            bool isWindowed = MyCrossService.IsWindowedFormat(magicBytes);
            Console.WriteLine($"[DecompressStream] Format={(isWindowed ? "windowed (CV4/CV5)" : "monolithic")}");

            // ── Concurrency gate: prevent OOM from parallel decompressions ──
            await _compressionGate.WaitAsync(context.CancellationToken);
            try
            {
                var crossService = new MyCrossService();

                if (isWindowed)
                {
                    var grpcStream = new GrpcResponseStream(responseStream, context.CancellationToken);
                    (long decompressedSize, byte[] decompressedHash) = await crossService.DecompressFileWindowedStreamAsync(
                        tempPath, grpcStream, context.CancellationToken);

                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        DecompressStats = new DecompressionStats
                        {
                            CompressedSize = (ulong)receivedBytes,
                            DecompressedSize = (ulong)decompressedSize,
                            DecompressedSha256 = ByteString.CopyFrom(decompressedHash)
                        }
                    });
                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
                    });
                }
                else
                {
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
            finally
            {
                _compressionGate.Release();
            }
        }
        catch (RpcException) { throw; }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            Console.WriteLine($"[DecompressStream] Client disconnected. Cleaning up.");
        }
        catch (IOException ex) when (ex.Message.Contains("reset", StringComparison.OrdinalIgnoreCase)
                                  || ex.Message.Contains("aborted", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"[DecompressStream] Client reset stream: {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[DecompressStream] FAILED: {ex.GetType().Name}: {ex.Message}");
            throw new RpcException(new Status(StatusCode.Internal, $"Decompression stream failed: {ex.Message}"));
        }
        finally
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
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
        ByteString? declaredSha256 = null;
        long windowedThreshold = GetWindowedThreshold();

        // Temp file only used for monolithic path
        string tempPath = Path.Combine(Path.GetTempPath(), $"cross-upload-{Guid.NewGuid():N}.bin");

        try
        {
            // ── Read metadata from first message ──
            if (!await requestStream.MoveNext(context.CancellationToken))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Empty request stream."));

            var firstMsg = requestStream.Current;
            if (firstMsg.PayloadCase == FileUploadRequest.PayloadOneofCase.Metadata)
            {
                fileName = firstMsg.Metadata.FileName;
                declaredSize = firstMsg.Metadata.OriginalSize;
                declaredSha256 = firstMsg.Metadata.Sha256;
            }

            bool willUseWindowed = declaredSize > (ulong)windowedThreshold;
            long effectiveLimit = GetEffectiveMaxUploadBytes(declaredSize);
            if (declaredSize > 0 && declaredSize > (ulong)effectiveLimit)
                throw new RpcException(new Status(StatusCode.InvalidArgument,
                    $"File too large. Declared {declaredSize} bytes, max allowed {effectiveLimit}."));

            Console.WriteLine($"[ProcessFileStream] metadata: file={fileName}, size={declaredSize}, windowed={willUseWindowed}, concurrency={MaxConcurrentCompressions - _compressionGate.CurrentCount}/{MaxConcurrentCompressions}");

            // ── Concurrency gate: wait if too many concurrent compressions ──
            await _compressionGate.WaitAsync(context.CancellationToken);
            try
            {
                if (willUseWindowed)
                {
                    // ═══════════════════════════════════════════════════════════
                    //  WINDOWED IN-MEMORY PIPELINE — no temp file, parallel compression
                    //  RAM ≈ (parallelism + channelCapacity) × windowSize ≈ 512-768 MB
                    // ═══════════════════════════════════════════════════════════
                    await HandleWindowedPipeline(requestStream, responseStream, context,
                        (long)declaredSize, declaredSha256, effectiveLimit);
                }
                else
                {
                    // ═══════════════════════════════════════════════════════════
                    //  MONOLITHIC v3.0.0 — temp file, single-shot compression
                    // ═══════════════════════════════════════════════════════════
                    await HandleMonolithicCompression(requestStream, responseStream, context,
                        tempPath, (long)declaredSize, declaredSha256, effectiveLimit);
                }
            }
            finally
            {
                _compressionGate.Release();
            }
        }
        catch (RpcException) { throw; }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            Console.WriteLine($"[ProcessFileStream] Client disconnected (file={fileName ?? "?"}). Cleaning up.");
        }
        catch (IOException ex) when (ex.Message.Contains("reset", StringComparison.OrdinalIgnoreCase)
                                  || ex.Message.Contains("aborted", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"[ProcessFileStream] Client reset stream (file={fileName ?? "?"}): {ex.Message}");
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("request is complete", StringComparison.OrdinalIgnoreCase)
                                                 || ex.Message.Contains("Can't write", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"[ProcessFileStream] Client closed before response finished (file={fileName ?? "?"}): {ex.Message}");
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ProcessFileStream] FAILED (file={fileName ?? "?"}): {ex.GetType().Name}: {ex.Message}");
            throw new RpcException(new Status(StatusCode.Internal, $"Compression stream failed: {ex.Message}"));
        }
        finally
        {
            try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
        }
    }

    // ═══════════════════════════════════════════════════════════════════
    //  WINDOWED IN-MEMORY PIPELINE (Phase 1)
    //  No temp file. Receives gRPC chunks → accumulates 64 MB windows →
    //  bounded channel → N parallel compress workers → ordered output
    // ═══════════════════════════════════════════════════════════════════
    private async Task HandleWindowedPipeline(
        IAsyncStreamReader<FileUploadRequest> requestStream,
        IServerStreamWriter<FileUploadResponse> responseStream,
        ServerCallContext context,
        long declaredSize,
        ByteString? declaredSha256,
        long effectiveLimit)
    {
        int windowSize = MyCrossService.WindowSize;
        int parallelism = GetPipelineParallelism();
        int blockCount = (int)((declaredSize + windowSize - 1) / windowSize);
        if (blockCount == 0) blockCount = 1;

        Console.WriteLine($"[Pipeline] Starting: {declaredSize} bytes, windowSize={windowSize / (1024 * 1024)}MB, blocks={blockCount}, parallelism={parallelism}");

        // Bounded channel: capacity = parallelism + small buffer
        // This limits RAM to (capacity + parallelism) × windowSize
        var windowChannel = Channel.CreateBounded<(int Index, byte[] Data)>(
            new BoundedChannelOptions(parallelism + 4)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true
            });

        var grpcStream = new GrpcResponseStream(responseStream, context.CancellationToken);

        // ── Start compression pipeline (runs concurrently with receive) ──
        var pipelineTask = RunCompressionPipeline(
            windowChannel.Reader, grpcStream, declaredSize, blockCount, parallelism, context.CancellationToken);

        // ── Receive loop: accumulate gRPC chunks into window buffers ──
        byte[] currentBuf = new byte[windowSize];
        int bufOffset = 0;
        int windowIndex = 0;
        long receivedBytes = 0;
        using var sha = SHA256.Create();

        while (await requestStream.MoveNext(context.CancellationToken))
        {
            var msg = requestStream.Current;
            if (msg.PayloadCase == FileUploadRequest.PayloadOneofCase.Metadata)
                continue; // Already processed
            if (msg.PayloadCase != FileUploadRequest.PayloadOneofCase.Chunk)
                continue;

            var chunk = msg.Chunk;
            if (chunk.Data == null || chunk.Data.Length == 0)
            {
                if (chunk.Eof) break;
                continue;
            }

            byte[] data = chunk.Data.ToByteArray();
            sha.TransformBlock(data, 0, data.Length, null, 0);
            receivedBytes += data.Length;

            // Fill window buffer(s) — a single gRPC chunk may span window boundaries
            int srcOffset = 0;
            while (srcOffset < data.Length)
            {
                int toCopy = Math.Min(data.Length - srcOffset, windowSize - bufOffset);
                Buffer.BlockCopy(data, srcOffset, currentBuf, bufOffset, toCopy);
                bufOffset += toCopy;
                srcOffset += toCopy;

                if (bufOffset == windowSize)
                {
                    // Window complete → push to pipeline (may block if channel is full = backpressure)
                    await windowChannel.Writer.WriteAsync((windowIndex, currentBuf), context.CancellationToken);
                    windowIndex++;
                    currentBuf = new byte[windowSize]; // Old buffer is now owned by the channel
                    bufOffset = 0;
                }
            }

            if (receivedBytes % (500 * 1024 * 1024) < data.Length)
            {
                double pct = declaredSize > 0 ? (receivedBytes * 100.0 / declaredSize) : 0;
                Console.WriteLine($"[Pipeline] Receiving: {receivedBytes / (1024.0 * 1024.0):F0} MB / {declaredSize / (1024.0 * 1024.0):F0} MB ({pct:F1}%)");
            }

            if (receivedBytes > effectiveLimit)
                throw new RpcException(new Status(StatusCode.InvalidArgument,
                    $"File too large. Received {receivedBytes} bytes, max allowed {effectiveLimit}."));

            if (chunk.Eof) break;
        }

        // Push last partial window
        if (bufOffset > 0)
        {
            byte[] lastWindow = new byte[bufOffset];
            Buffer.BlockCopy(currentBuf, 0, lastWindow, 0, bufOffset);
            await windowChannel.Writer.WriteAsync((windowIndex, lastWindow), context.CancellationToken);
        }

        windowChannel.Writer.Complete();
        Console.WriteLine($"[Pipeline] Receive done: {receivedBytes} bytes, {windowIndex + (bufOffset > 0 ? 1 : 0)} windows pushed");

        // Finalize upload SHA256
        sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
        byte[] uploadedSha256 = sha.Hash!;

        // Validate upload
        if (declaredSize > 0 && (ulong)receivedBytes != (ulong)declaredSize)
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                $"Declared size {declaredSize} does not match received {receivedBytes}."));
        if (declaredSha256 != null && declaredSha256.Length == 32 && !declaredSha256.Span.SequenceEqual(uploadedSha256))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "SHA-256 mismatch for uploaded file."));

        // ── Wait for pipeline to finish writing all compressed blocks ──
        var (compressedSize, refsFound, totalChunks, dcBytesStored, serverProcessingMs) = await pipelineTask;

        // ── Write SHA256 trailer (hash of original file, computed in receive loop) ──
        await grpcStream.WriteAsync(uploadedSha256, 0, 32, context.CancellationToken);
        await grpcStream.FlushAsync(context.CancellationToken);
        compressedSize += 32;

        // ── Stats + EOF ──
        await responseStream.WriteAsync(new FileUploadResponse
        {
            Stats = new CompressionStats
            {
                OriginalSize = (ulong)receivedBytes,
                CompressedSize = (ulong)compressedSize,
                ReferencesFound = (uint)Math.Max(0, refsFound),
                TotalChunks = (uint)Math.Max(0, totalChunks),
                CompressedSha256 = ByteString.CopyFrom(uploadedSha256),
                DatacenterBytesStored = (ulong)Math.Max(0, dcBytesStored),
                ServerProcessingMs = serverProcessingMs
            }
        });
        await responseStream.WriteAsync(new FileUploadResponse
        {
            Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
        });

        Console.WriteLine($"[Pipeline] COMPLETE: {receivedBytes} → {compressedSize} bytes, {blockCount} blocks, refs={refsFound}");
    }

    /// <summary>
    /// Parallel compression pipeline: N workers compress windows, writer outputs in order.
    /// Writes v4.0.0 header + blocks (NOT trailer — caller writes that).
    /// </summary>
    private static async Task<(long CompressedSize, int TotalRefs, int TotalChunks, long DatacenterBytes, double ServerProcessingMs)> RunCompressionPipeline(
        ChannelReader<(int Index, byte[] Data)> reader,
        GrpcResponseStream grpcStream,
        long declaredFileSize,
        int blockCount,
        int parallelism,
        CancellationToken ct)
    {
        var crossService = new MyCrossService();

        // ── Write v5 header: magic(4) + fileSize(8) + blockCount(4) = 16 bytes ──
        byte[] header = new byte[4 + 8 + 4];
        byte[] magic = "CV5\0"u8.ToArray();
        Buffer.BlockCopy(magic, 0, header, 0, 4);
        BitConverter.TryWriteBytes(header.AsSpan(4), declaredFileSize);
        BitConverter.TryWriteBytes(header.AsSpan(12), blockCount);
        await grpcStream.WriteAsync(header, 0, header.Length, ct);

        long totalCompressedSize = header.Length;
        int totalRefs = 0, totalChunks = 0;
        long totalDatacenterBytes = 0;
        long totalCompressionTicks = 0;

        // Completed blocks waiting to be written in order
        var completedBlocks = new ConcurrentDictionary<int, (byte[] Compressed, int OriginalLen)>();
        int nextToWrite = 0;
        var blockReady = new SemaphoreSlim(0);

        // ── N compression workers ──
        var workers = new Task[parallelism];
        for (int w = 0; w < parallelism; w++)
        {
            workers[w] = Task.Run(async () =>
            {
                while (await reader.WaitToReadAsync(ct))
                {
                    while (reader.TryRead(out var item))
                    {
                        Console.WriteLine($"[Pipeline] Compressing block {item.Index + 1}/{blockCount} ({item.Data.Length} bytes)...");
                        var sw = System.Diagnostics.Stopwatch.StartNew();
                        (byte[] compressed, int refs, int chunks, long dcBytes) = await crossService.CompressFileWithStats(item.Data);
                        sw.Stop();
                        Console.WriteLine($"[Pipeline] Block {item.Index + 1}/{blockCount}: {item.Data.Length} → {compressed.Length} ({sw.ElapsedMilliseconds}ms)");

                        int blockIndex = item.Index;
                        int dataLen = item.Data.Length;
                        completedBlocks[blockIndex] = (compressed, dataLen);
                        Interlocked.Add(ref totalRefs, refs);
                        Interlocked.Add(ref totalChunks, chunks);
                        Interlocked.Add(ref totalDatacenterBytes, dcBytes);
                        Interlocked.Add(ref totalCompressionTicks, sw.ElapsedTicks);
                        blockReady.Release();
                        GC.Collect(2, GCCollectionMode.Optimized, false);
                    }
                }
            }, ct);
        }

        // ── Writer: outputs blocks in strict order ──
        while (nextToWrite < blockCount)
        {
            await blockReady.WaitAsync(ct);
            while (completedBlocks.TryRemove(nextToWrite, out var block))
            {
                // V5 block format: compressedLen(4) + originalLen(4) + compressed data
                byte[] blockHeader = new byte[8];
                BitConverter.TryWriteBytes(blockHeader.AsSpan(0), (int)block.Compressed.Length);
                BitConverter.TryWriteBytes(blockHeader.AsSpan(4), block.OriginalLen);
                await grpcStream.WriteAsync(blockHeader, 0, 8, ct);
                await grpcStream.WriteAsync(block.Compressed, 0, block.Compressed.Length, ct);

                totalCompressedSize += 8 + block.Compressed.Length;
                Console.WriteLine($"[Pipeline] ✅ Block {nextToWrite + 1}/{blockCount} streamed ({block.OriginalLen} → {block.Compressed.Length})");
                nextToWrite++;
            }
        }

        await Task.WhenAll(workers);
        double serverMs = totalCompressionTicks * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
        return (totalCompressedSize, totalRefs, totalChunks, totalDatacenterBytes, serverMs);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  MONOLITHIC COMPRESSION (small files, < windowedThreshold)
    // ═══════════════════════════════════════════════════════════════════
    private async Task HandleMonolithicCompression(
        IAsyncStreamReader<FileUploadRequest> requestStream,
        IServerStreamWriter<FileUploadResponse> responseStream,
        ServerCallContext context,
        string tempPath,
        long declaredSize,
        ByteString? declaredSha256,
        long effectiveLimit)
    {
        long receivedBytes = 0;
        byte[] uploadedSha256;

        // ── Receive to temp file ──
        {
            await using var fs = new FileStream(tempPath, FileMode.CreateNew, FileAccess.Write,
                FileShare.None, bufferSize: 1024 * 1024, useAsync: true);
            using var sha = SHA256.Create();

            while (await requestStream.MoveNext(context.CancellationToken))
            {
                var msg = requestStream.Current;
                if (msg.PayloadCase == FileUploadRequest.PayloadOneofCase.Metadata) continue;
                if (msg.PayloadCase != FileUploadRequest.PayloadOneofCase.Chunk) continue;

                var chunk = msg.Chunk;
                if (chunk.Data == null || chunk.Data.Length == 0)
                {
                    if (chunk.Eof) break;
                    continue;
                }

                byte[] data = chunk.Data.ToByteArray();
                await fs.WriteAsync(data, 0, data.Length, context.CancellationToken);
                sha.TransformBlock(data, 0, data.Length, null, 0);
                receivedBytes += data.Length;

                if (receivedBytes > effectiveLimit)
                    throw new RpcException(new Status(StatusCode.InvalidArgument,
                        $"File too large. Received {receivedBytes} bytes, max allowed {effectiveLimit}."));
                if (chunk.Eof) break;
            }

            sha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
            await fs.FlushAsync(context.CancellationToken);
            uploadedSha256 = sha.Hash ?? Array.Empty<byte>();
        }

        // Validate
        if (declaredSize > 0 && receivedBytes != declaredSize)
            throw new RpcException(new Status(StatusCode.InvalidArgument,
                $"Declared size {declaredSize} does not match received {receivedBytes}."));
        if (declaredSha256 != null && declaredSha256.Length == 32 && !declaredSha256.Span.SequenceEqual(uploadedSha256))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "SHA-256 mismatch for uploaded file."));

        Console.WriteLine($"[ProcessFileStream] Monolithic: {receivedBytes} bytes received");

        // ── Compress ──
        var crossService = new MyCrossService();
        byte[] fileBytes = await File.ReadAllBytesAsync(tempPath, context.CancellationToken);

        var compressSw = System.Diagnostics.Stopwatch.StartNew();
        (byte[] compressedBytes, int referencesFound, int totalChunks, long dcBytesStored) = await crossService.CompressFileWithStats(fileBytes);
        compressSw.Stop();
        fileBytes = null!;

        byte[] compressedHash;
        using (var compressedSha = SHA256.Create())
            compressedHash = compressedSha.ComputeHash(compressedBytes);

        // ── Stream response ──
        await responseStream.WriteAsync(new FileUploadResponse
        {
            Stats = new CompressionStats
            {
                OriginalSize = (ulong)receivedBytes,
                CompressedSize = (ulong)compressedBytes.Length,
                ReferencesFound = (uint)Math.Max(0, referencesFound),
                TotalChunks = (uint)Math.Max(0, totalChunks),
                CompressedSha256 = ByteString.CopyFrom(compressedHash),
                DatacenterBytesStored = (ulong)Math.Max(0, dcBytesStored),
                ServerProcessingMs = compressSw.Elapsed.TotalMilliseconds
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

        Console.WriteLine($"[ProcessFileStream] Monolithic DONE: {receivedBytes} → {compressedBytes.Length}");
        compressedBytes = null!;
        GC.Collect(2, GCCollectionMode.Optimized, false);
    }

    /// <summary>
    /// Aggregate storage statistics from all agents.
    /// Queries every known agent in parallel and sums the results.
    /// </summary>
    public override async Task<SystemStatsResponse> GetSystemStats(
        SystemStatsRequest request, ServerCallContext context)
    {
        var agentIps = RendezvousRouter.GetAllAgentIps();
        if (agentIps.Length == 0)
        {
            // Force a refresh
            RendezvousRouter.GetAgents();
            agentIps = RendezvousRouter.GetAllAgentIps();
        }

        ulong totalChunks = 0, totalBytes = 0, totalBuckets = 0, totalVectors = 0;
        uint reachable = 0;

        var tasks = agentIps.Select(async ip =>
        {
            try
            {
                var client = GrpcChannelFactory.GetClient(
                    target: ip,
                    ctor: chan => new StorageStats.StorageStatsClient(chan),
                    roundRobin: false,
                    port: 5000);

                var res = await client.GetStatsAsync(
                    new Google.Protobuf.WellKnownTypes.Empty(),
                    deadline: DateTime.UtcNow.AddSeconds(15),
                    cancellationToken: context.CancellationToken);

                return (res.TotalUniqueChunks, res.TotalChunkBytes, res.TotalBuckets, res.TotalVectors, true);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[GetSystemStats] Agent {ip} failed: {ex.Message}");
                return (0UL, 0UL, 0UL, 0UL, false);
            }
        });

        var results = await Task.WhenAll(tasks);
        foreach (var (chunks, bytes, buckets, vectors, ok) in results)
        {
            if (!ok) continue;
            totalChunks += chunks;
            totalBytes += bytes;
            totalBuckets += buckets;
            totalVectors += vectors;
            reachable++;
        }

        Console.WriteLine($"[GetSystemStats] {reachable}/{agentIps.Length} agents: {totalChunks:N0} chunks, {totalBytes / (1024.0 * 1024.0):F1} MB, {totalBuckets:N0} buckets, {totalVectors:N0} vectors");

        return new SystemStatsResponse
        {
            TotalUniqueChunks = totalChunks,
            TotalChunkBytes = totalBytes,
            TotalBuckets = totalBuckets,
            TotalVectors = totalVectors,
            AgentCount = reachable
        };
    }
}
