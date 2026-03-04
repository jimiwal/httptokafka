
using System.Collections.Concurrent;
using System.Threading.Channels;
using EcsToAzureBlobCopier.Abstractions;
using Microsoft.Extensions.Logging;

namespace EcsToAzureBlobCopier.Parallel;

public sealed class ParallelCopyOptions
{
    public int MaxDegreeOfParallelism { get; init; } = 16;

    // ile elementów maksymalnie "w locie" (producer -> channel -> workers)
    public int BoundedCapacity { get; init; } = 512;

    // co ile domkniętych commitów zapisujemy checkpoint (oprócz timera)
    public int CheckpointEveryCommits { get; init; } = 25;

    // minimalny odstęp czasu między zapisami checkpointu
    public TimeSpan CheckpointMinInterval { get; init; } = TimeSpan.FromSeconds(5);

    // replay window: cofamy effectiveAfter o ten czas, by złapać spóźnione modyfikacje
    public TimeSpan ReplayWindow { get; init; } = TimeSpan.Zero;
}

public sealed record ParallelCopyResult(
    int CopiedCount,
    int FailedCount,
    int TotalProcessed,
    string? WatermarkKey,
    DateTimeOffset? WatermarkLastModifiedUtc
);

internal sealed record SequencedObject(long Seq, EcsObjectDescriptor Obj);

public sealed class ParallelCopyRunner
{
    private readonly IEcsObjectSource _source;
    private readonly IBlobObjectSink _sink;
    private readonly ICheckpointStore _checkpointStore;
    private readonly ICopyManifestWriter _manifest;
    private readonly ILogger _log;

    public ParallelCopyRunner(
        IEcsObjectSource source,
        IBlobObjectSink sink,
        ICheckpointStore checkpointStore,
        ICopyManifestWriter manifest,
        ILogger<ParallelCopyRunner> log)
    {
        _source = source;
        _sink = sink;
        _checkpointStore = checkpointStore;
        _manifest = manifest;
        _log = log;
    }

    public async Task<ParallelCopyResult> RunAsync(
        CopyRequest req,
        ParallelCopyOptions opt,
        CancellationToken ct)
    {
        // 1) Load checkpoint (watermark)
        var cp = await _checkpointStore.TryLoadAsync(req.CheckpointName, ct).ConfigureAwait(false);

        if (cp is not null)
        {
            if (!string.Equals(cp.Bucket, req.SourceBucket, StringComparison.Ordinal) ||
                !string.Equals(cp.Prefix ?? "", req.SourcePrefix ?? "", StringComparison.Ordinal) ||
                !string.Equals(cp.Container, req.TargetContainer, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "Checkpoint nie pasuje do requestu (bucket/prefix/container). Zmień CheckpointName lub wyczyść checkpoint.");
            }
        }

        // 2) Effective watermark
        //    - start z checkpointu jeśli istnieje,
        //    - inaczej z req.ModifiedAfterUtc,
        //    - replay window cofa effectiveAfter (tylko jeśli jest ustawione)
        var baseAfter = cp?.LastProcessedLastModifiedUtc ?? req.ModifiedAfterUtc;
        var effectiveAfter = baseAfter is null
            ? (DateTimeOffset?)null
            : baseAfter.Value - opt.ReplayWindow;

        // Tie-breaker key tylko gdy bazujemy na checkpoint (bo tylko wtedy ma sens "Key > watermarkKey" na remisie)
        var watermarkKey = cp?.LastProcessedKey;
        var watermarkLm = cp?.LastProcessedLastModifiedUtc;

        _log.LogInformation(
            "Parallel copy start. Bucket={Bucket}, Prefix={Prefix}, TargetContainer={Container}, DOP={Dop}, Capacity={Cap}, EffectiveAfter={After}, Watermark=({WmLm},{WmKey}), ReplayWindow={Replay}",
            req.SourceBucket, req.SourcePrefix, req.TargetContainer, opt.MaxDegreeOfParallelism, opt.BoundedCapacity,
            effectiveAfter, watermarkLm, watermarkKey, opt.ReplayWindow);

        // 3) Channel
        var channel = Channel.CreateBounded<SequencedObject>(new BoundedChannelOptions(opt.BoundedCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = linkedCts.Token;

        var committer = new OrderedCheckpointCommitter(
            checkpointName: req.CheckpointName,
            baseCheckpoint: cp,
            checkpointStore: _checkpointStore,
            req: req,
            log: _log,
            everyCommits: opt.CheckpointEveryCommits,
            minInterval: opt.CheckpointMinInterval);

        int copied = 0;
        int failed = 0;
        int processed = 0;

        // 4) Producer
        var producer = Task.Run(async () =>
        {
            try
            {
                long seq = 0;

                await foreach (var obj in _source.ListAsync(
                    bucket: req.SourceBucket,
                    prefix: req.SourcePrefix,
                    modifiedAfterUtc: effectiveAfter,
                    startAfterKey: null,
                    ct: token).ConfigureAwait(false))
                {
                    token.ThrowIfCancellationRequested();

                    // dodatkowy filtr na remis watermarku (tylko jeśli checkpoint istnieje)
                    // - jeśli obj.LM == watermarkLm, to musimy brać tylko Key > watermarkKey
                    if (watermarkLm is not null && watermarkKey is not null &&
                        obj.LastModifiedUtc == watermarkLm.Value &&
                        string.CompareOrdinal(obj.Key, watermarkKey) <= 0)
                    {
                        continue;
                    }

                    // Uwaga: listowanie już filtruje po effectiveAfter, więc zwykle tu nic nie trzeba.
                    await channel.Writer.WriteAsync(new SequencedObject(seq++, obj), token).ConfigureAwait(false);
                }

                channel.Writer.TryComplete();
            }
            catch (Exception ex)
            {
                channel.Writer.TryComplete(ex);
                throw;
            }
        }, token);

        // 5) Workers
        var workers = Enumerable.Range(0, opt.MaxDegreeOfParallelism)
            .Select(i => Task.Run(async () =>
            {
                try
                {
                    await foreach (var item in channel.Reader.ReadAllAsync(token).ConfigureAwait(false))
                    {
                        token.ThrowIfCancellationRequested();
                        var obj = item.Obj;

                        var blobName = BuildTargetBlobName(req.TargetPrefix, obj.Key);

                        try
                        {
                            _log.LogInformation("[W{Worker}] Copy {Key} (LM={LM}, Size={Size}) -> {Blob}",
                                i, obj.Key, obj.LastModifiedUtc, obj.Size, blobName);

                            await using var stream = await _source.OpenReadAsync(obj.Bucket, obj.Key, token).ConfigureAwait(false);

                            var metadata = new Dictionary<string, string>
                            {
                                ["sourceBucket"] = obj.Bucket,
                                ["sourceKey"] = obj.Key,
                                ["sourceLastModifiedUtc"] = obj.LastModifiedUtc.ToString("O"),
                                ["sourceSize"] = obj.Size.ToString(System.Globalization.CultureInfo.InvariantCulture),
                            };
                            if (!string.IsNullOrWhiteSpace(obj.ETag))
                                metadata["sourceETag"] = obj.ETag!;

                            // Tryb A: overwrite ALWAYS (sink musi to zapewnić)
                            await _sink.UploadAsync(req.TargetContainer, blobName, stream, metadata, token).ConfigureAwait(false);

                            await _manifest.AppendSuccessAsync(req.ManifestName, obj, blobName, token).ConfigureAwait(false);

                            Interlocked.Increment(ref copied);
                            Interlocked.Increment(ref processed);

                            await committer.MarkDoneAsync(item.Seq, obj, token).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref failed);
                            Interlocked.Increment(ref processed);

                            _log.LogError(ex, "Copy failed for {Key} (seq={Seq}). Cancelling run.", obj.Key, item.Seq);

                            // fail-fast: przerywamy całość (żeby nie mieć "dziur" + niepewnego watermarku)
                            linkedCts.Cancel();
                            throw;
                        }
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // normalne przy fail-fast
                }
            }, token))
            .ToArray();

        // 6) Join + final flush checkpoint
        try
        {
            await Task.WhenAll(workers.Append(producer)).ConfigureAwait(false);
            await committer.FlushAsync(token).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (ct.IsCancellationRequested)
        {
            throw;
        }
        catch
        {
            // Jeżeli coś padło, spróbujmy jeszcze dopisać checkpoint do ostatniego domkniętego watermarku
            try { await committer.FlushAsync(CancellationToken.None).ConfigureAwait(false); } catch { /* best-effort */ }
            throw;
        }

        var finalWm = committer.CurrentWatermark;

        _log.LogInformation(
            "Parallel copy finished. Copied={Copied}, Failed={Failed}, Processed={Processed}, Watermark=({WmLm},{WmKey})",
            copied, failed, processed, finalWm.LastModifiedUtc, finalWm.Key);

        return new ParallelCopyResult(
            CopiedCount: copied,
            FailedCount: failed,
            TotalProcessed: processed,
            WatermarkKey: finalWm.Key,
            WatermarkLastModifiedUtc: finalWm.LastModifiedUtc);
    }

    private static string BuildTargetBlobName(string? targetPrefix, string sourceKey)
    {
        var p = (targetPrefix ?? "").Trim();
        if (string.IsNullOrEmpty(p)) return sourceKey;
        p = p.TrimEnd('/') + "/";
        return p + sourceKey.TrimStart('/');
    }

    private sealed class OrderedCheckpointCommitter
    {
        private readonly string _checkpointName;
        private readonly ICheckpointStore _store;
        private readonly CopyRequest _req;
        private readonly ILogger _log;

        private readonly int _everyCommits;
        private readonly TimeSpan _minInterval;

        private readonly object _lock = new();

        private long _nextToCommit;
        private readonly SortedDictionary<long, EcsObjectDescriptor> _done = new();

        private int _commitCountSinceSave;
        private DateTimeOffset _lastSaveUtc = DateTimeOffset.MinValue;

        public (DateTimeOffset? LastModifiedUtc, string? Key) CurrentWatermark { get; private set; }

        public OrderedCheckpointCommitter(
            string checkpointName,
            CopyCheckpoint? baseCheckpoint,
            ICheckpointStore checkpointStore,
            CopyRequest req,
            ILogger log,
            int everyCommits,
            TimeSpan minInterval)
        {
            _checkpointName = checkpointName;
            _store = checkpointStore;
            _req = req;
            _log = log;
            _everyCommits = Math.Max(1, everyCommits);
            _minInterval = minInterval;

            _nextToCommit = 0;
            CurrentWatermark = (baseCheckpoint?.LastProcessedLastModifiedUtc, baseCheckpoint?.LastProcessedKey);
        }

        public Task MarkDoneAsync(long seq, EcsObjectDescriptor obj, CancellationToken ct)
        {
            CopyCheckpoint? checkpointToSave = null;

            lock (_lock)
            {
                // zapisz sukces do mapy
                _done[seq] = obj;

                // commituj "ciągły prefiks"
                while (_done.TryGetValue(_nextToCommit, out var nextObj))
                {
                    _done.Remove(_nextToCommit);
                    _nextToCommit++;

                    // watermark idzie na ostatnio domknięty obiekt w porządku listowania
                    CurrentWatermark = (nextObj.LastModifiedUtc, nextObj.Key);
                    _commitCountSinceSave++;

                    // przygotuj checkpoint do zapisu (best effort)
                    if (ShouldSaveNow())
                    {
                        checkpointToSave = BuildCheckpoint(CurrentWatermark);
                        _commitCountSinceSave = 0;
                        _lastSaveUtc = DateTimeOffset.UtcNow;
                    }
                }
            }

            if (checkpointToSave is null) return Task.CompletedTask;
            return SaveAsync(checkpointToSave, ct);
        }

        public async Task FlushAsync(CancellationToken ct)
        {
            CopyCheckpoint cp;
            lock (_lock)
            {
                cp = BuildCheckpoint(CurrentWatermark);
                _commitCountSinceSave = 0;
                _lastSaveUtc = DateTimeOffset.UtcNow;
            }

            await SaveAsync(cp, ct).ConfigureAwait(false);
        }

        private bool ShouldSaveNow()
        {
            if (_commitCountSinceSave >= _everyCommits) return true;
            if (DateTimeOffset.UtcNow - _lastSaveUtc >= _minInterval) return true;
            return false;
        }

        private CopyCheckpoint BuildCheckpoint((DateTimeOffset? LastModifiedUtc, string? Key) wm)
            => new CopyCheckpoint(
                Bucket: _req.SourceBucket,
                Prefix: _req.SourcePrefix,
                Container: _req.TargetContainer,
                ModifiedAfterUtc: _req.ModifiedAfterUtc,
                LastProcessedKey: wm.Key,
                LastProcessedLastModifiedUtc: wm.LastModifiedUtc,
                UpdatedUtc: DateTimeOffset.UtcNow
            );

        private async Task SaveAsync(CopyCheckpoint cp, CancellationToken ct)
        {
            await _store.SaveAsync(_checkpointName, cp, ct).ConfigureAwait(false);
            _log.LogInformation("Checkpoint saved. Watermark=({WmLm},{WmKey}) UpdatedUtc={Updated}",
                cp.LastProcessedLastModifiedUtc, cp.LastProcessedKey, cp.UpdatedUtc);
        }
    }
}
