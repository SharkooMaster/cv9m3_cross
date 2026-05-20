

using Cross.Services.CcfStore;
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

    // ── Dedicated ArrayPool for windowed compression buffers ──
    // Why this is its own pool: ArrayPool<byte>.Shared has a hard ceiling of
    // 1 MiB on the pooled array size. For anything larger Rent() silently
    // falls back to `new byte[size]` (returning a non-pooled array) and
    // Return() discards it. Our windows are 4–64 MiB, so the Shared pool
    // gave us zero reuse — every window allocation hit the LOH, which is
    // exactly the problem we were trying to solve. ArrayPool<byte>.Create
    // returns a real pool with configurable size limits.
    //
    // Sizing:
    //   maxArrayLength: 128 MiB    — covers CROSS_WINDOW_SIZE_MB up to 128,
    //                                future-proofs without absurd overhead.
    //   maxArraysPerBucket: 8      — caps pinned memory at
    //                                ~8 × 128 MiB = 1 GiB worst case per
    //                                cross pod. In practice, with 2 files in
    //                                flight × parallelism 2, channel cap 6,
    //                                we keep ~16 buffers active. At the
    //                                benchmark's 4 MiB window size that's
    //                                ~64 MiB resident — tiny.
    //
    // Buckets are powers of 2 from 16 B up to maxArrayLength, so a 4 MiB
    // window goes to the 4 MiB bucket cleanly; a 64 MiB window goes to the
    // 64 MiB bucket. No internal fragmentation when window size is a power
    // of 2 (which it always is in our config).
    private static readonly System.Buffers.ArrayPool<byte> _windowBufPool =
        System.Buffers.ArrayPool<byte>.Create(
            maxArrayLength: 128 * 1024 * 1024,
            maxArraysPerBucket: 64);

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
                    
                    if (receivedBytes > 1024L * 1024 * 1024 * 100) // 100GB limit
                        throw new RpcException(new Status(StatusCode.InvalidArgument, "File too large."));
                        
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
                    if (receivedBytes > GetWindowedThreshold())
                        throw new RpcException(new Status(StatusCode.InvalidArgument, $"Monolithic file too large. Max {GetWindowedThreshold()} bytes."));

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

        // Job telemetry: server-issued correlation id for the dashboard. Created here
        // (not later) so failures during metadata parsing also have an id we can
        // attribute the FAILED event to. The JobScope is opened after we know the mode
        // (monolithic vs windowed) so STAGE_DONE events tag themselves correctly.
        string jobId = Guid.NewGuid().ToString("N");
        var jobWallSw = System.Diagnostics.Stopwatch.StartNew();
        Cross.Services.JobEvents.JobEventBus.JobScope? jobScope = null;
        bool jobCompleted = false;
        string? errorStage = null;

        try
        {
            // ── Read metadata from first message ──
            if (!await requestStream.MoveNext(context.CancellationToken))
                throw new RpcException(new Status(StatusCode.InvalidArgument, "Empty request stream."));

            var firstMsg = requestStream.Current;
            float maxErrorRate = 0f;
            bool storeOnCluster = false;
            string preferredEncoding = "";
            if (firstMsg.PayloadCase == FileUploadRequest.PayloadOneofCase.Metadata)
            {
                fileName = firstMsg.Metadata.FileName;
                declaredSize = firstMsg.Metadata.OriginalSize;
                declaredSha256 = firstMsg.Metadata.Sha256;
                maxErrorRate = firstMsg.Metadata.MaxErrorRate;
                storeOnCluster = firstMsg.Metadata.StoreOnCluster && Globals.EnableCcfStore;
                preferredEncoding = firstMsg.Metadata?.PreferredEncoding ?? "";
            }

            bool willUseWindowed = declaredSize > (ulong)windowedThreshold;
            long effectiveLimit = GetEffectiveMaxUploadBytes(declaredSize);
            if (declaredSize > 0 && declaredSize > (ulong)effectiveLimit)
                throw new RpcException(new Status(StatusCode.InvalidArgument,
                    $"File too large. Declared {declaredSize} bytes, max allowed {effectiveLimit}."));

            // Open the job scope now that we know the mode. AsyncLocal flows into the
            // pipeline tasks so STAGE_DONE events automatically carry this jobId.
            var mode = willUseWindowed
                ? Crossv9.Jobevents.JobMode.Windowed
                : Crossv9.Jobevents.JobMode.Monolithic;
            jobScope = Cross.Services.JobEvents.JobEventBus.BeginScope(jobId, mode);
            Cross.Services.JobEvents.JobEventBus.EmitStarted(jobId, mode, fileName ?? string.Empty, declaredSize);

            Console.WriteLine($"[ProcessFileStream] metadata: file={fileName}, size={declaredSize}, windowed={willUseWindowed}, concurrency={MaxConcurrentCompressions - _compressionGate.CurrentCount}/{MaxConcurrentCompressions}, jobId={jobId}");

            // ── Agent readiness gate: wait until at least one agent is available ──
            try
            {
                await AgentHealthWatcher.Instance.WaitForFirstAgentAsync(context.CancellationToken);
            }
            catch (InvalidOperationException)
            {
                Console.WriteLine("[ProcessFileStream] AgentHealthWatcher not yet started, proceeding with warmup discovery");
                RendezvousRouter.GetAgents();
            }

            // ── Concurrency gate: wait if too many concurrent compressions ──
            await _compressionGate.WaitAsync(context.CancellationToken);
            try
            {
                if (willUseWindowed)
                {
                    await HandleWindowedPipeline(requestStream, responseStream, context,
                        jobId, jobWallSw, (long)declaredSize, declaredSha256, effectiveLimit, maxErrorRate, storeOnCluster, preferredEncoding);
                }
                else
                {
                    await HandleMonolithicCompression(requestStream, responseStream, context,
                        jobId, jobWallSw, tempPath, (long)declaredSize, declaredSha256, effectiveLimit, maxErrorRate, storeOnCluster, preferredEncoding);
                }

                jobCompleted = true;
            }
            finally
            {
                _compressionGate.Release();
            }
        }
        catch (RpcException rex)
        {
            if (!jobCompleted)
                Cross.Services.JobEvents.JobEventBus.EmitFailed(jobId, "RpcException", rex.Status.Detail, errorStage);
            throw;
        }
        catch (OperationCanceledException) when (context.CancellationToken.IsCancellationRequested)
        {
            Console.WriteLine($"[ProcessFileStream] Client disconnected (file={fileName ?? "?"}). Cleaning up.");
            if (!jobCompleted)
                Cross.Services.JobEvents.JobEventBus.EmitFailed(jobId, "ClientDisconnected", null, errorStage);
        }
        catch (IOException ex) when (ex.Message.Contains("reset", StringComparison.OrdinalIgnoreCase)
                                  || ex.Message.Contains("aborted", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"[ProcessFileStream] Client reset stream (file={fileName ?? "?"}): {ex.Message}");
            if (!jobCompleted)
                Cross.Services.JobEvents.JobEventBus.EmitFailed(jobId, "ClientReset", ex.Message, errorStage);
        }
        catch (InvalidOperationException ex) when (ex.Message.Contains("request is complete", StringComparison.OrdinalIgnoreCase)
                                                 || ex.Message.Contains("Can't write", StringComparison.OrdinalIgnoreCase))
        {
            Console.WriteLine($"[ProcessFileStream] Client closed before response finished (file={fileName ?? "?"}): {ex.Message}");
            if (!jobCompleted)
                Cross.Services.JobEvents.JobEventBus.EmitFailed(jobId, "ClientClosed", ex.Message, errorStage);
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ProcessFileStream] FAILED (file={fileName ?? "?"}): {ex.GetType().Name}: {ex.Message}");
            if (!jobCompleted)
                Cross.Services.JobEvents.JobEventBus.EmitFailed(jobId, ex.GetType().Name, ex.Message, errorStage);
            throw new RpcException(new Status(StatusCode.Internal, $"Compression stream failed: {ex.Message}"));
        }
        finally
        {
            jobScope?.Dispose();
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
        string jobId,
        System.Diagnostics.Stopwatch jobWallSw,
        long declaredSize,
        ByteString? declaredSha256,
        long effectiveLimit,
        float maxErrorRate = 0f,
        bool storeOnCluster = false,
        string preferredEncoding = "")
    {
        int windowSize = MyCrossService.WindowSize;
        int parallelism = GetPipelineParallelism();

        if (declaredSize < 0 || declaredSize > 1024L * 1024 * 1024 * 1024 * 1024) // 1 PB limit
            throw new RpcException(new Status(StatusCode.InvalidArgument, $"Invalid declared size: {declaredSize}"));

        int blockCount = (int)((declaredSize + windowSize - 1) / windowSize);
        if (blockCount == 0) blockCount = 1;

        Console.WriteLine($"[Pipeline] Starting: {declaredSize} bytes, windowSize={windowSize / (1024 * 1024)}MB, blocks={blockCount}, parallelism={parallelism}, clusterStore={storeOnCluster}");

        // Channel carries (index, pooled buffer, logical length). The buffer is
        // rented from the dedicated window-buffer pool on the producer side and
        // MUST be returned by the consumer (RunCompressionPipeline) regardless
        // of how its work item completes. Pre-pool every window allocation hit
        // the LOH directly (~64 MiB/s allocation rate at peak), driving frag
        // to 50%+ between forced compactions. With the dedicated pool below,
        // we recycle the same ~16 buffers across all in-flight windows and
        // LOH churn for this path drops to zero.
        var windowChannel = Channel.CreateBounded<(int Index, byte[] Data, int Length)>(
            new BoundedChannelOptions(parallelism + 4)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleWriter = true
            });

        var windowBufPool = _windowBufPool;

        // When storing on cluster, capture compressed output to a temp file instead of streaming
        string? clusterTempPath = storeOnCluster
            ? Path.Combine(Path.GetTempPath(), $"cross-cluster-{Guid.NewGuid():N}.bin")
            : null;
        FileStream? clusterTempFs = clusterTempPath != null
            ? new FileStream(clusterTempPath, FileMode.CreateNew, FileAccess.Write, FileShare.None, 1024 * 1024)
            : null;

        var grpcStream = storeOnCluster ? null : new GrpcResponseStream(responseStream, context.CancellationToken);

        Stream outputStream = storeOnCluster ? (Stream)clusterTempFs! : grpcStream!;
        var pipelineTask = RunCompressionPipeline(
            windowChannel.Reader, outputStream, declaredSize, blockCount, parallelism, context.CancellationToken, maxErrorRate, preferredEncoding);

        // ── Receive loop: accumulate gRPC chunks into pooled window buffers ──
        byte[] currentBuf = windowBufPool.Rent(windowSize);
        int bufOffset = 0;
        int windowIndex = 0;
        long receivedBytes = 0;
        using var sha = SHA256.Create();

        // If the receive loop throws before transferring `currentBuf` ownership
        // to the channel, we must return it ourselves. The channel consumer is
        // responsible for returning anything that successfully made it through.
        bool currentBufTransferred = false;

        try
        {
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
                        // Window complete → hand ownership to pipeline (may block if channel is full = backpressure)
                        currentBufTransferred = true;
                        await windowChannel.Writer.WriteAsync((windowIndex, currentBuf, windowSize), context.CancellationToken);
                        windowIndex++;
                        currentBuf = windowBufPool.Rent(windowSize);
                        currentBufTransferred = false;
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

            // Push last partial window — reuse the same pooled buffer; logical length carries the
            // truth so we don't have to copy into a smaller buffer.
            if (bufOffset > 0)
            {
                currentBufTransferred = true;
                await windowChannel.Writer.WriteAsync((windowIndex, currentBuf, bufOffset), context.CancellationToken);
            }
        }
        finally
        {
            // If we never transferred ownership (loop threw, or last window was empty),
            // return the buffer to the pool here. Channel consumer handles all other cases.
            if (!currentBufTransferred && currentBuf != null)
            {
                windowBufPool.Return(currentBuf);
            }
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

        var (compressedSize, refsFound, totalChunks, dcBytesStored, serverProcessingMs, avgErrorRate, errorPayloadBytes) = await pipelineTask;

        // Write SHA256 trailer
        await outputStream.WriteAsync(uploadedSha256, 0, 32, context.CancellationToken);
        await outputStream.FlushAsync(context.CancellationToken);
        compressedSize += 32;

        string fileIdHex = Convert.ToHexString(uploadedSha256).ToLowerInvariant();

        if (storeOnCluster && clusterTempFs != null && clusterTempPath != null)
        {
            await clusterTempFs.DisposeAsync();
            await CcfStoreService.Instance.StoreCcfStreamAsync(fileIdHex, clusterTempPath, context.CancellationToken);
            try { File.Delete(clusterTempPath); } catch { }
            Console.WriteLine($"[Pipeline] Cluster-stored CCF: fileId={fileIdHex}, {compressedSize} bytes");

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
                    ServerProcessingMs = serverProcessingMs,
                    AverageErrorRate = avgErrorRate,
                    ErrorPayloadBytes = (ulong)Math.Max(0, errorPayloadBytes),
                    FileId = fileIdHex
                }
            });
            await responseStream.WriteAsync(new FileUploadResponse
            {
                Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
            });
        }
        else
        {
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
                    ServerProcessingMs = serverProcessingMs,
                    AverageErrorRate = avgErrorRate,
                    ErrorPayloadBytes = (ulong)Math.Max(0, errorPayloadBytes)
                }
            });
            await responseStream.WriteAsync(new FileUploadResponse
            {
                Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
            });
        }

        Console.WriteLine($"[Pipeline] COMPLETE: {receivedBytes} → {compressedSize} bytes, {blockCount} blocks, refs={refsFound}");

        jobWallSw.Stop();
        Cross.Services.JobEvents.JobEventBus.EmitCompleted(
            jobId,
            (ulong)Math.Max(0, compressedSize),
            Math.Max(0, refsFound),
            Math.Max(0, totalChunks),
            Math.Max(0, dcBytesStored),
            serverProcessingMs,
            jobWallSw.Elapsed.TotalMilliseconds,
            avgErrorRate,
            Math.Max(0, errorPayloadBytes),
            fileIdHex);
    }

    /// <summary>
    /// Parallel compression pipeline: N workers compress windows, writer outputs in order.
    /// Writes v4.0.0 header + blocks (NOT trailer — caller writes that).
    /// </summary>
    private static async Task<(long CompressedSize, int TotalRefs, int TotalChunks, long DatacenterBytes, double ServerProcessingMs, float AverageErrorRate, long ErrorPayloadBytes)> RunCompressionPipeline(
        ChannelReader<(int Index, byte[] Data, int Length)> reader,
        Stream outputStream,
        long declaredFileSize,
        int blockCount,
        int parallelism,
        CancellationToken ct,
        float maxErrorRate = 0f,
        string preferredEncoding = "")
    {
        var crossService = new MyCrossService();
        // Must be the same dedicated pool the producer rented from (see
        // CompressFileService._windowBufPool) — Returning to the Shared pool
        // would silently drop large arrays and corrupt accounting.
        var windowBufPool = _windowBufPool;

        byte[] header = new byte[4 + 8 + 4];
        byte[] magic = "CV5\0"u8.ToArray();
        Buffer.BlockCopy(magic, 0, header, 0, 4);
        BitConverter.TryWriteBytes(header.AsSpan(4), declaredFileSize);
        BitConverter.TryWriteBytes(header.AsSpan(12), blockCount);
        await outputStream.WriteAsync(header, 0, header.Length, ct);

        long totalCompressedSize = header.Length;
        int totalRefs = 0, totalChunks = 0;
        long totalDatacenterBytes = 0;
        long totalCompressionTicks = 0;
        long totalErrorPayloadBytes = 0;
        double weightedErrorRateSum = 0;
        long weightedErrorRateDenom = 0;

        // Completed blocks waiting to be written in order
        var completedBlocks = new ConcurrentDictionary<int, (byte[] Compressed, int OriginalLen)>();
        int nextToWrite = 0;
        var blockReady = new SemaphoreSlim(0);

        // ── N compression workers ──
        // Capture parent jobId from AsyncLocal so per-block events attribute correctly.
        // Task.Run captures ExecutionContext, which carries the AsyncLocal value into the
        // worker continuation, but we read it once here to skip per-block lookups.
        string? parentJobId = Cross.Services.JobEvents.JobEventBus.CurrentJobId;
        var workers = new Task[parallelism];
        for (int w = 0; w < parallelism; w++)
        {
            workers[w] = Task.Run(async () =>
            {
                while (await reader.WaitToReadAsync(ct))
                {
                    while (reader.TryRead(out var item))
                    {
                        // item.Data is a pooled buffer owned by us from here on —
                        // we must return it to the pool regardless of outcome.
                        try
                        {
                            Console.WriteLine($"[Pipeline] Compressing block {item.Index + 1}/{blockCount} ({item.Length} bytes)...");
                            var sw = System.Diagnostics.Stopwatch.StartNew();
                            (byte[] compressed, int refs, int chunks, long dcBytes, float blockAvgErr, long blockErrPayload) = await crossService.CompressFileWithStats(item.Data, item.Length, maxErrorRate, preferredEncoding);
                            sw.Stop();
                            Console.WriteLine($"[Pipeline] Block {item.Index + 1}/{blockCount}: {item.Length} → {compressed.Length} ({sw.ElapsedMilliseconds}ms)");

                            int blockIndex = item.Index;
                            int dataLen = item.Length;
                            completedBlocks[blockIndex] = (compressed, dataLen);
                            Interlocked.Add(ref totalRefs, refs);
                            Interlocked.Add(ref totalChunks, chunks);
                            Interlocked.Add(ref totalDatacenterBytes, dcBytes);
                            Interlocked.Add(ref totalCompressionTicks, sw.ElapsedTicks);
                            Interlocked.Add(ref totalErrorPayloadBytes, blockErrPayload);

                            if (!string.IsNullOrEmpty(parentJobId))
                            {
                                Cross.Services.JobEvents.JobEventBus.EmitBlockDone(
                                    parentJobId, blockIndex, blockCount,
                                    dataLen, compressed.Length, refs, chunks, dcBytes,
                                    sw.Elapsed.TotalMilliseconds);
                            }
                            if (blockAvgErr > 0 && refs > 0)
                            {
                                long blockWeight = (long)refs;
                                lock (completedBlocks)
                                {
                                    weightedErrorRateSum += blockAvgErr * blockWeight;
                                    weightedErrorRateDenom += blockWeight;
                                }
                            }
                            blockReady.Release();
                        }
                        finally
                        {
                            windowBufPool.Return(item.Data);
                        }
                    }
                }
            }, ct);
        }

        try
        {
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
                    await outputStream.WriteAsync(blockHeader, 0, 8, ct);
                    await outputStream.WriteAsync(block.Compressed, 0, block.Compressed.Length, ct);

                    totalCompressedSize += 8 + block.Compressed.Length;
                    Console.WriteLine($"[Pipeline] ✅ Block {nextToWrite + 1}/{blockCount} streamed ({block.OriginalLen} → {block.Compressed.Length})");
                    nextToWrite++;
                }
            }

            await Task.WhenAll(workers);
        }
        finally
        {
            // Safety net: if cancellation or an error short-circuited the workers,
            // drain any pooled buffers still parked in the channel so they
            // don't escape the pool's accounting.
            while (reader.TryRead(out var leftover))
            {
                try { windowBufPool.Return(leftover.Data); } catch { }
            }
        }

        double serverMs = totalCompressionTicks * 1000.0 / System.Diagnostics.Stopwatch.Frequency;
        float avgErrorRate = weightedErrorRateDenom > 0 ? (float)(weightedErrorRateSum / weightedErrorRateDenom) : 0f;
        return (totalCompressedSize, totalRefs, totalChunks, totalDatacenterBytes, serverMs, avgErrorRate, totalErrorPayloadBytes);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  MONOLITHIC COMPRESSION (small files, < windowedThreshold)
    // ═══════════════════════════════════════════════════════════════════
    private async Task HandleMonolithicCompression(
        IAsyncStreamReader<FileUploadRequest> requestStream,
        IServerStreamWriter<FileUploadResponse> responseStream,
        ServerCallContext context,
        string jobId,
        System.Diagnostics.Stopwatch jobWallSw,
        string tempPath,
        long declaredSize,
        ByteString? declaredSha256,
        long effectiveLimit,
        float maxErrorRate = 0f,
        bool storeOnCluster = false,
        string preferredEncoding = "")
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
        long windowedThreshold = GetWindowedThreshold();

        if (receivedBytes > windowedThreshold)
        {
            Console.WriteLine($"[ProcessFileStream] File grew to {receivedBytes} bytes (exceeds {windowedThreshold} threshold), switching to windowed compression.");
            
            string outTempPath = tempPath + ".out";
            var (winCompressedSize, winReferencesFound, winTotalChunks) = await crossService.CompressFileWindowedAsync(tempPath, outTempPath, context.CancellationToken);
            
            byte[] winCompressedHash;
            using (var compressedSha = SHA256.Create())
            await using (var hashFs = new FileStream(outTempPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 1024, true))
            {
                byte[] buf = System.Buffers.ArrayPool<byte>.Shared.Rent(1024 * 1024);
                try
                {
                    int read;
                    while ((read = await hashFs.ReadAsync(buf, 0, buf.Length, context.CancellationToken)) > 0)
                        compressedSha.TransformBlock(buf, 0, read, null, 0);
                    compressedSha.TransformFinalBlock(Array.Empty<byte>(), 0, 0);
                    winCompressedHash = compressedSha.Hash!;
                }
                finally { System.Buffers.ArrayPool<byte>.Shared.Return(buf); }
            }

            string winFileIdHex = Convert.ToHexString(uploadedSha256).ToLowerInvariant();

            if (storeOnCluster)
            {
                await CcfStoreService.Instance.StoreCcfStreamAsync(winFileIdHex, outTempPath, context.CancellationToken);
                Console.WriteLine($"[ProcessFileStream] Cluster-stored CCF: fileId={winFileIdHex}, {winCompressedSize} bytes");

                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Stats = new CompressionStats
                    {
                        OriginalSize = (ulong)receivedBytes,
                        CompressedSize = (ulong)winCompressedSize,
                        ReferencesFound = (uint)Math.Max(0, winReferencesFound),
                        TotalChunks = (uint)Math.Max(0, winTotalChunks),
                        CompressedSha256 = ByteString.CopyFrom(winCompressedHash),
                        DatacenterBytesStored = 0,
                        ServerProcessingMs = 0,
                        AverageErrorRate = 0,
                        ErrorPayloadBytes = 0,
                        FileId = winFileIdHex
                    }
                });
                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
                });
            }
            else
            {
                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Stats = new CompressionStats
                    {
                        OriginalSize = (ulong)receivedBytes,
                        CompressedSize = (ulong)winCompressedSize,
                        ReferencesFound = (uint)Math.Max(0, winReferencesFound),
                        TotalChunks = (uint)Math.Max(0, winTotalChunks),
                        CompressedSha256 = ByteString.CopyFrom(winCompressedHash),
                        DatacenterBytesStored = 0,
                        ServerProcessingMs = 0,
                        AverageErrorRate = 0,
                        ErrorPayloadBytes = 0
                    }
                });

                await using var outFs = new FileStream(outTempPath, FileMode.Open, FileAccess.Read, FileShare.Read, 1024 * 1024, true);
                const int outChunkSize = 1024 * 1024;
                byte[] chunkBuf = new byte[outChunkSize];
                uint seq = 0;
                int read;
                while ((read = await outFs.ReadAsync(chunkBuf, 0, outChunkSize, context.CancellationToken)) > 0)
                {
                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        Chunk = new FileChunk
                        {
                            Seq = seq++,
                            Data = ByteString.CopyFrom(chunkBuf, 0, read),
                            Eof = false
                        }
                    });
                }
                await responseStream.WriteAsync(new FileUploadResponse
                {
                    Chunk = new FileChunk { Seq = seq, Data = ByteString.Empty, Eof = true }
                });
            }

            try { File.Delete(outTempPath); } catch { }

            Console.WriteLine($"[ProcessFileStream] Monolithic (switched to windowed) DONE: {receivedBytes} → {winCompressedSize}");

            jobWallSw.Stop();
            Cross.Services.JobEvents.JobEventBus.EmitCompleted(
                jobId,
                (ulong)winCompressedSize,
                Math.Max(0, winReferencesFound),
                Math.Max(0, winTotalChunks),
                0,
                0,
                jobWallSw.Elapsed.TotalMilliseconds,
                0,
                0,
                winFileIdHex);
            
            return;
        }

        byte[] fileBytes = await File.ReadAllBytesAsync(tempPath, context.CancellationToken);

        var compressSw = System.Diagnostics.Stopwatch.StartNew();
        (byte[] compressedBytes, int referencesFound, int totalChunks, long dcBytesStored, float avgErrorRate, long errorPayloadBytes) = await crossService.CompressFileWithStats(fileBytes, maxErrorRate, preferredEncoding);
        compressSw.Stop();
        fileBytes = null!;

        byte[] compressedHash;
        using (var compressedSha = SHA256.Create())
            compressedHash = compressedSha.ComputeHash(compressedBytes);

        string fileIdHex = Convert.ToHexString(uploadedSha256).ToLowerInvariant();

        if (storeOnCluster)
        {
            await CcfStoreService.Instance.StoreCcfAsync(fileIdHex, compressedBytes, context.CancellationToken);
            Console.WriteLine($"[ProcessFileStream] Cluster-stored CCF: fileId={fileIdHex}, {compressedBytes.Length} bytes");

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
                    ServerProcessingMs = compressSw.Elapsed.TotalMilliseconds,
                    AverageErrorRate = avgErrorRate,
                    ErrorPayloadBytes = (ulong)Math.Max(0, errorPayloadBytes),
                    FileId = fileIdHex
                }
            });
            await responseStream.WriteAsync(new FileUploadResponse
            {
                Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
            });
        }
        else
        {
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
                    ServerProcessingMs = compressSw.Elapsed.TotalMilliseconds,
                    AverageErrorRate = avgErrorRate,
                    ErrorPayloadBytes = (ulong)Math.Max(0, errorPayloadBytes)
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

        Console.WriteLine($"[ProcessFileStream] Monolithic DONE: {receivedBytes} → {compressedBytes.Length}");

        jobWallSw.Stop();
        Cross.Services.JobEvents.JobEventBus.EmitCompleted(
            jobId,
            (ulong)compressedBytes.Length,
            Math.Max(0, referencesFound),
            Math.Max(0, totalChunks),
            Math.Max(0, dcBytesStored),
            compressSw.Elapsed.TotalMilliseconds,
            jobWallSw.Elapsed.TotalMilliseconds,
            avgErrorRate,
            Math.Max(0, errorPayloadBytes),
            fileIdHex);

        compressedBytes = null!;
        GC.Collect(2, GCCollectionMode.Optimized, false);
    }

    // ═══════════════════════════════════════════════════════════════════
    //  CLUSTER-STORED MODE: DECOMPRESS BY FILE ID
    // ═══════════════════════════════════════════════════════════════════
    public override async Task DecompressById(
        FileIdRequest request,
        IServerStreamWriter<FileUploadResponse> responseStream,
        ServerCallContext context)
    {
        if (!Globals.EnableCcfStore)
            throw new RpcException(new Status(StatusCode.FailedPrecondition,
                "Cluster-stored CCF mode is not enabled. Set ENABLE_CCF_STORE=true."));

        string fileId = request.FileId;
        if (string.IsNullOrWhiteSpace(fileId))
            throw new RpcException(new Status(StatusCode.InvalidArgument, "file_id is required."));

        Console.WriteLine($"[DecompressById] Fetching CCF for fileId={fileId}");

        var (ccfPath, ccfBytes) = await CcfStoreService.Instance.GetCcfLocationAsync(fileId, context.CancellationToken);
        if (ccfPath == null && ccfBytes == null)
            throw new RpcException(new Status(StatusCode.NotFound,
                $"CCF not found for fileId={fileId}. File may not have been compressed in cluster-stored mode."));

        long ccfLength = ccfPath != null ? new FileInfo(ccfPath).Length : ccfBytes!.Length;
        Console.WriteLine($"[DecompressById] Got CCF (length={ccfLength}), decompressing...");

        await _compressionGate.WaitAsync(context.CancellationToken);
        try
        {
            byte[] magicBytes = new byte[4];
            if (ccfPath != null)
            {
                using var fs = new FileStream(ccfPath, FileMode.Open, FileAccess.Read);
                fs.Read(magicBytes, 0, 4);
            }
            else
            {
                Buffer.BlockCopy(ccfBytes!, 0, magicBytes, 0, Math.Min(4, ccfBytes!.Length));
            }

            bool isWindowed = MyCrossService.IsWindowedFormat(magicBytes);

            var crossService = new MyCrossService();

            if (isWindowed)
            {
                string tempPath = ccfPath ?? Path.Combine(Path.GetTempPath(), $"cross-dbi-{Guid.NewGuid():N}.bin");
                bool deleteTemp = ccfPath == null;
                try
                {
                    if (deleteTemp)
                    {
                        await File.WriteAllBytesAsync(tempPath, ccfBytes!, context.CancellationToken);
                    }
                    var grpcStream = new GrpcResponseStream(responseStream, context.CancellationToken);
                    (long decompressedSize, byte[] decompressedHash) =
                        await crossService.DecompressFileWindowedStreamAsync(tempPath, grpcStream, context.CancellationToken);

                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        DecompressStats = new DecompressionStats
                        {
                            CompressedSize = (ulong)ccfLength,
                            DecompressedSize = (ulong)decompressedSize,
                            DecompressedSha256 = ByteString.CopyFrom(decompressedHash)
                        }
                    });
                    await responseStream.WriteAsync(new FileUploadResponse
                    {
                        Chunk = new FileChunk { Seq = 0, Data = ByteString.Empty, Eof = true }
                    });
                }
                finally
                {
                    if (deleteTemp)
                    {
                        try { if (File.Exists(tempPath)) File.Delete(tempPath); } catch { }
                    }
                }
            }
            else
            {
                byte[] dataToDecompress = ccfBytes ?? await File.ReadAllBytesAsync(ccfPath!, context.CancellationToken);
                byte[] decompressedBytes = await crossService.DecompressFile(dataToDecompress);
                byte[] decompressedHash;
                using (var sha = SHA256.Create())
                    decompressedHash = sha.ComputeHash(decompressedBytes);

                await responseStream.WriteAsync(new FileUploadResponse
                {
                    DecompressStats = new DecompressionStats
                    {
                        CompressedSize = (ulong)ccfLength,
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

            Console.WriteLine($"[DecompressById] DONE for fileId={fileId}");
        }
        finally
        {
            _compressionGate.Release();
        }
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

    public override Task<CcfStoreStatsResponse> GetCcfStoreStats(
        CcfStoreStatsRequest request, ServerCallContext context)
    {
        var resp = new CcfStoreStatsResponse { Enabled = Globals.EnableCcfStore };

        if (Globals.EnableCcfStore)
        {
            var fs = CcfStoreService.Instance.GetStoreStats();
            resp.OptimizerRunning = CcfPackOptimizerService.IsRunning;
            resp.LastRunUnix = CcfPackOptimizerService.LastRunUtc.HasValue
                ? new DateTimeOffset(CcfPackOptimizerService.LastRunUtc.Value).ToUnixTimeSeconds()
                : 0;
            resp.UnpackedCount = fs.UnpackedCount;
            resp.UnpackedBytes = fs.UnpackedBytes;
            resp.PackCount = fs.PackCount;
            resp.PackBytes = fs.PackBytes;
            resp.TotalBytes = fs.TotalBytes;
            resp.HasDictionary = fs.HasDictionary;
            resp.DictBytes = fs.DictBytes;
            resp.LastPackedCount = CcfPackOptimizerService.LastPackedCount;
            resp.LastPackSavedBytes = CcfPackOptimizerService.LastPackSavedBytes;
            resp.TotalPackedAllTime = CcfPackOptimizerService.TotalPackedAllTime;
            resp.PackRawBytes = CcfPackOptimizerService.TotalPackRawBytes;
            resp.PackCompressedBytes = CcfPackOptimizerService.TotalPackCompressedBytes;
            resp.PackCompressionRatio = CcfPackOptimizerService.TotalPackRawBytes > 0
                ? 1.0 - (double)CcfPackOptimizerService.TotalPackCompressedBytes / CcfPackOptimizerService.TotalPackRawBytes
                : 0;
            resp.EncodingVersion = Globals.CcfEncodingV6 ? "v6.0.0" : "v5.6.0";
            resp.PframeGroups = CcfPackOptimizerService.PframeGroupsFound;
            resp.PframeDeltaCount = CcfPackOptimizerService.PframeDeltaCount;
            resp.PframeSavedBytes = CcfPackOptimizerService.PframeSavedBytes;
            resp.PframeSavingRatio = CcfPackOptimizerService.TotalPackRawBytes > 0
                ? (double)CcfPackOptimizerService.PframeSavedBytes / CcfPackOptimizerService.TotalPackRawBytes
                : 0;
            resp.LastConsolidationSavedBytes = ChunkConsolidationService.LastConsolidationSavedBytes;
            resp.TotalConsolidationSavedBytes = ChunkConsolidationService.TotalConsolidationSavedBytes;
            resp.LastConsolidatedEntries = ChunkConsolidationService.LastRepackedEntries;
            resp.LastConsolidationUnix = ChunkConsolidationService.LastConsolidationUtc.HasValue
                ? new DateTimeOffset(ChunkConsolidationService.LastConsolidationUtc.Value).ToUnixTimeSeconds()
                : 0;
        }

        return Task.FromResult(resp);
    }

    public override Task<ClearCcfStoreResponse> ClearCcfStore(
        ClearCcfStoreRequest request, ServerCallContext context)
    {
        var resp = new ClearCcfStoreResponse();

        if (!Globals.EnableCcfStore)
        {
            resp.Success = false;
            return Task.FromResult(resp);
        }

        try
        {
            var (bytesFreed, filesDeleted) = CcfStoreService.Instance.ClearAll();
            CcfPackOptimizerService.ResetStats();
            ChunkConsolidationService.ResetStats();

            resp.Success = true;
            resp.BytesFreed = bytesFreed;
            resp.FilesDeleted = filesDeleted;
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[ClearCcfStore] Error: {ex.Message}");
            resp.Success = false;
        }

        return Task.FromResult(resp);
    }
}
