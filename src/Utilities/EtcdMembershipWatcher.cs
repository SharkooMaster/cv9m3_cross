using System.Text.Json;
using dotnet_etcd;
using Etcdserverpb;
using Google.Protobuf;
using Mvccpb;

namespace Cross.Utilities;

/// <summary>
/// Streams the /agents/ prefix from etcd and pushes membership snapshots
/// into <see cref="RendezvousRouter"/>. This is the single source of truth
/// for cross-pod agent membership.
///
/// Background:
///   Previously cross derived membership from independent DNS + GetNodeInfo
///   per pod, with a 15-second cache. Two cross pods could observe slightly
///   different member sets and route the same (BucketId, BucketKey) to
///   different agents, leading to wrong-chunk reads at decompress time
///   (INTEGRITY_DECOMPRESS).
///
///   Now: every agent self-registers in etcd under /agents/{podName} with
///   a 10s lease + heartbeat. All cross pods watch the same etcd prefix
///   and receive the same linearised event stream → identical membership
///   views across cross pods within milliseconds of a change.
///
/// Failure modes handled:
///   - etcd unreachable at startup: keep retrying with backoff. Cross
///     continues to serve using the legacy DNS+GetNodeInfo path until the
///     watcher publishes (RendezvousRouter.IsEtcdFresh returns false).
///   - etcd connection drops mid-stream: rebuild the snapshot from a fresh
///     RangeRequest, then re-establish the watch.
///   - Malformed key/value: skipped with a log, doesn't break the stream.
/// </summary>
public class EtcdMembershipWatcher : IHostedService
{
    private readonly string? _endpoint;
    private CancellationTokenSource? _cts;
    private Task? _runner;
    private EtcdClient? _etcd;

    // Single-writer state owned by the runner loop; published snapshots are
    // immutable handed off to RendezvousRouter so concurrent readers can't
    // observe a partially-mutated state.
    private readonly Dictionary<string, string> _members = new();

    public EtcdMembershipWatcher()
    {
        _endpoint = Environment.GetEnvironmentVariable("ETCD_ENDPOINT");
    }

    public async Task StartAsync(CancellationToken cancellationToken)
    {
        if (string.IsNullOrWhiteSpace(_endpoint))
        {
            Console.WriteLine(
                "[EtcdMembershipWatcher] ETCD_ENDPOINT unset — etcd-driven membership disabled. " +
                "Cross will fall back to legacy DNS+GetNodeInfo routing.");
            return;
        }

        Console.WriteLine($"[EtcdMembershipWatcher] starting; etcd endpoint = {_endpoint}");
        _cts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);

        // ── Bounded wait for the first snapshot ──
        // If we let the watcher run purely in the background, every cross pod
        // has a few-second window after boot where it's still using the DNS
        // path while peer cross pods may already be using etcd. Two cross
        // pods in different stages → routing drift → exactly the bug we're
        // trying to fix.
        //
        // Synchronously waiting for the first snapshot inside StartAsync
        // closes that window: by the time cross becomes "started", its view
        // is consistent with every other cross pod's view.
        //
        // We cap the wait at 30 s so a permanently-down etcd doesn't block
        // cross from booting at all — after the cap we proceed and the
        // background loop keeps retrying.
        var firstSnapshotCts = CancellationTokenSource.CreateLinkedTokenSource(_cts.Token);
        firstSnapshotCts.CancelAfter(TimeSpan.FromSeconds(30));

        try
        {
            _etcd = new EtcdClient(
                connectionString: _endpoint!,
                configureChannelOptions: opts =>
                {
                    opts.Credentials = Grpc.Core.ChannelCredentials.Insecure;
                });

            long startRevision = await SnapshotAndPublishAsync(firstSnapshotCts.Token);
            Console.WriteLine(
                $"[EtcdMembershipWatcher] initial snapshot complete at revision {startRevision} " +
                $"({_members.Count} members)");

            // Hand off the watch to the background loop, starting from the
            // revision we just snapshotted so we don't lose events between
            // snapshot and watch.
            _runner = Task.Run(() => RunWatchAsync(startRevision + 1, _cts.Token));
        }
        catch (OperationCanceledException) when (firstSnapshotCts.IsCancellationRequested && !cancellationToken.IsCancellationRequested)
        {
            Console.WriteLine(
                "[EtcdMembershipWatcher] initial snapshot timed out after 30s — " +
                "cross will start with the legacy DNS path while the background watcher keeps retrying.");
            _runner = Task.Run(() => RunAsync(_cts.Token));
        }
        catch (Exception ex)
        {
            Console.WriteLine(
                $"[EtcdMembershipWatcher] initial snapshot failed ({ex.GetType().Name}: {ex.Message}) — " +
                "cross will start with the legacy DNS path while the background watcher keeps retrying.");
            _runner = Task.Run(() => RunAsync(_cts.Token));
        }
    }

    private async Task RunWatchAsync(long fromRevision, CancellationToken token)
    {
        // Continuation of the in-StartAsync snapshot: start the watch from
        // the revision after the snapshot. On stream end / error fall into
        // the full RunAsync loop which re-snapshots + re-watches with
        // exponential backoff.
        try
        {
            if (_etcd == null) throw new InvalidOperationException("etcd client not initialised");
            var watchReq = new WatchRequest
            {
                CreateRequest = new WatchCreateRequest
                {
                    Key = ByteString.CopyFromUtf8("/agents/"),
                    RangeEnd = ByteString.CopyFromUtf8(PrefixEnd("/agents/")),
                    StartRevision = fromRevision,
                }
            };
            await _etcd.WatchAsync(watchReq, OnWatchEvent, cancellationToken: token);
            Console.WriteLine("[EtcdMembershipWatcher] initial watch stream ended; switching to reconnect loop");
        }
        catch (OperationCanceledException) when (token.IsCancellationRequested) { return; }
        catch (Exception ex)
        {
            Console.WriteLine($"[EtcdMembershipWatcher] initial watch error: {ex.GetType().Name}: {ex.Message}");
        }

        if (!token.IsCancellationRequested)
            await RunAsync(token);
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        try
        {
            _cts?.Cancel();
            if (_runner != null)
            {
                await Task.WhenAny(_runner, Task.Delay(TimeSpan.FromSeconds(3), cancellationToken));
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[EtcdMembershipWatcher] StopAsync error: {ex.Message}");
        }
        finally
        {
            try { _etcd?.Dispose(); } catch { /* best effort */ }
        }
    }

    private async Task RunAsync(CancellationToken token)
    {
        var backoff = TimeSpan.FromSeconds(1);
        var maxBackoff = TimeSpan.FromSeconds(15);

        while (!token.IsCancellationRequested)
        {
            try
            {
                _etcd?.Dispose();
                _etcd = new EtcdClient(
                    connectionString: _endpoint!,
                    configureChannelOptions: opts =>
                    {
                        opts.Credentials = Grpc.Core.ChannelCredentials.Insecure;
                    });

                // ── 1. Snapshot ──
                // Read everything currently under /agents/ once, publish to
                // RendezvousRouter, then start a watch from the same revision
                // so we don't miss any events between snapshot and watch.
                long startRevision = await SnapshotAndPublishAsync(token);
                Console.WriteLine($"[EtcdMembershipWatcher] initial snapshot complete at revision {startRevision}");

                // Reset backoff on a successful snapshot+watch attach.
                backoff = TimeSpan.FromSeconds(1);

                // ── 2. Watch ──
                // dotnet-etcd's WatchAsync runs until the stream closes or
                // throws. On normal exit (no exception) we still need to
                // re-snapshot because in-flight events may have been lost.
                var watchReq = new WatchRequest
                {
                    CreateRequest = new WatchCreateRequest
                    {
                        Key = ByteString.CopyFromUtf8("/agents/"),
                        RangeEnd = ByteString.CopyFromUtf8(PrefixEnd("/agents/")),
                        StartRevision = startRevision + 1,
                    }
                };

                await _etcd.WatchAsync(
                    watchReq,
                    OnWatchEvent,
                    cancellationToken: token);

                Console.WriteLine("[EtcdMembershipWatcher] watch stream ended cleanly; restarting");
            }
            catch (OperationCanceledException) when (token.IsCancellationRequested)
            {
                return;
            }
            catch (Exception ex)
            {
                Console.WriteLine($"[EtcdMembershipWatcher] watch error: {ex.GetType().Name}: {ex.Message}; retrying in {backoff.TotalSeconds}s");
            }

            try { await Task.Delay(backoff, token); }
            catch (OperationCanceledException) { return; }

            backoff = TimeSpan.FromTicks(Math.Min(maxBackoff.Ticks, backoff.Ticks * 2));
        }
    }

    private async Task<long> SnapshotAndPublishAsync(CancellationToken token)
    {
        if (_etcd == null) throw new InvalidOperationException("etcd client not initialised");

        var rangeReq = new RangeRequest
        {
            Key = ByteString.CopyFromUtf8("/agents/"),
            RangeEnd = ByteString.CopyFromUtf8(PrefixEnd("/agents/")),
        };

        var resp = await _etcd.GetAsync(rangeReq, cancellationToken: token);

        _members.Clear();
        foreach (var kv in resp.Kvs)
        {
            if (TryParseEntry(kv, out var name, out var ip))
            {
                _members[name] = ip;
            }
        }

        PublishToRouter();
        return resp.Header?.Revision ?? 0;
    }

    private void OnWatchEvent(WatchResponse response)
    {
        bool changed = false;
        foreach (var ev in response.Events)
        {
            switch (ev.Type)
            {
                case Event.Types.EventType.Put:
                    if (TryParseEntry(ev.Kv, out var name, out var ip))
                    {
                        if (!_members.TryGetValue(name, out var prev) || prev != ip)
                        {
                            _members[name] = ip;
                            changed = true;
                        }
                    }
                    break;

                case Event.Types.EventType.Delete:
                    if (TryGetKeyName(ev.Kv, out var delName))
                    {
                        if (_members.Remove(delName))
                            changed = true;
                    }
                    break;
            }
        }

        if (changed)
            PublishToRouter();
    }

    private void PublishToRouter()
    {
        // Hand off an immutable copy — the watcher's internal dict keeps
        // mutating as new events arrive, RendezvousRouter must see a frozen
        // snapshot.
        var snapshot = new Dictionary<string, string>(_members);
        RendezvousRouter.SetMembershipFromEtcd(snapshot);
    }

    private static bool TryParseEntry(KeyValue kv, out string name, out string ip)
    {
        name = string.Empty;
        ip = string.Empty;
        if (!TryGetKeyName(kv, out name)) return false;

        try
        {
            var json = kv.Value.ToStringUtf8();
            using var doc = JsonDocument.Parse(json);
            var root = doc.RootElement;
            if (root.TryGetProperty("ip", out var ipEl))
            {
                var parsed = ipEl.GetString();
                if (!string.IsNullOrWhiteSpace(parsed))
                {
                    ip = parsed!;
                    return true;
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"[EtcdMembershipWatcher] failed to parse /agents/{name}: {ex.Message}");
        }

        return false;
    }

    private static bool TryGetKeyName(KeyValue kv, out string name)
    {
        name = string.Empty;
        var k = kv.Key.ToStringUtf8();
        const string prefix = "/agents/";
        if (!k.StartsWith(prefix, StringComparison.Ordinal)) return false;
        var rest = k.Substring(prefix.Length);
        if (string.IsNullOrWhiteSpace(rest)) return false;
        name = rest;
        return true;
    }

    private static string PrefixEnd(string prefix)
    {
        // etcd convention: RangeEnd is the prefix with the last byte incremented.
        var bytes = System.Text.Encoding.UTF8.GetBytes(prefix);
        if (bytes.Length == 0) return "\0";
        for (int i = bytes.Length - 1; i >= 0; i--)
        {
            if (bytes[i] < 0xFF)
            {
                bytes[i]++;
                return System.Text.Encoding.UTF8.GetString(bytes, 0, i + 1);
            }
        }
        // All 0xFF — fall back to a clearly larger key.
        return prefix + "\uFFFF";
    }
}
