using System.Collections.Concurrent;
using System.Diagnostics;
using System.Threading.Channels;
using EcsToAzureBlobCopier.Abstractions;
using Microsoft.Extensions.Logging;

namespace EcsToAzureBlobCopier.Parallel;

public sealed class ParallelCopyOptions
{
    public int MaxDegreeOfParallelism { get; init; } = 16;

    // Maximum number of items in-flight (producer -> channel -> workers)
    public int BoundedCapacity { get; init; } = 512;

    // How many committed items before checkpoint is saved (besides time-based trigger)
    public int CheckpointEveryCommits { get; init; } = 25;

    // Minimum time between checkpoint saves
    public TimeSpan CheckpointMinInterval { get; init; } = TimeSpan.FromSeconds(5);

    // Replay window: shifts effectiveAfter backward to catch late-arriving updates
    public TimeSpan ReplayWindow { get; init; } = TimeSpan.Zero;

    // Minimum interval between progress logs
    public TimeSpan ProgressLogInterval { get; init; } = TimeSpan.FromMinutes(10);
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

    private DateTimeOffset _lastProgressLogUtc = DateTimeOffset.MinValue;
    private readonly object _progressLogLock = new();
    private long _progressCounter;
    private long _totalBytes;
    private Stopwatch _globalSw = new();

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
        // Reset per-run progress counters
        _lastProgressLogUtc = DateTimeOffset.MinValue;
        Interlocked.Exchange(ref _progressCounter, 0);
        Interlocked.Exchange(ref _totalBytes, 0);
        _globalSw = Stopwatch.StartNew();

        // 1) Load checkpoint (watermark)
        var cp = await _checkpointStore.TryLoadAsync(req.CheckpointName, ct).ConfigureAwait(false);

        if (cp is not null)
        {
            if (!string.Equals(cp.Bucket, req.SourceBucket, StringComparison.Ordinal) ||
                !string.Equals(cp.Prefix ?? "", req.SourcePrefix ?? "", StringComparison.Ordinal) ||
                !string.Equals(cp.Container, req.TargetContainer, StringComparison.Ordinal))
            {
                throw new InvalidOperationException(
                    "Checkpoint does not match the current request (bucket/prefix/container). Use a different CheckpointName or clear the checkpoint.");
            }
        }

        // 2) Compute effective watermark
        //    - start from checkpoint if it exists
        //    - otherwise use req.ModifiedAfterUtc
        //    - replay window shifts effectiveAfter backward when configured
        var baseAfter = cp?.LastProcessedLastModifiedUtc ?? req.ModifiedAfterUtc;
        var effectiveAfter = baseAfter is null
            ? (DateTimeOffset?)null
            : baseAfter.Value - opt.ReplayWindow;

        // Tie-breaker key is only relevant when resuming from a checkpoint
        var watermarkKey = cp?.LastProcessedKey;
        var watermarkLm = cp?.LastProcessedLastModifiedUtc;

        _log.LogInformation(
            "Parallel copy start. Bucket={Bucket}, Prefix={Prefix}, TargetContainer={Container}, DOP={Dop}, Capacity={Cap}, EffectiveAfter={After}, Watermark=({WmLm},{WmKey}), ReplayWindow={Replay}, ProgressLogInterval={ProgressLogInterval}",
            req.SourceBucket, req.SourcePrefix, req.TargetContainer, opt.MaxDegreeOfParallelism, opt.BoundedCapacity,
            effectiveAfter, watermarkLm, watermarkKey, opt.ReplayWindow, opt.ProgressLogInterval);

        // 3) Create channel
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

                    // Additional filter for watermark tie-break (same LastModified timestamp)
                    // If obj.LastModifiedUtc == watermarkLm, only keys > watermarkKey should be processed
                    if (watermarkLm is not null && watermarkKey is not null &&
                        obj.LastModifiedUtc == watermarkLm.Value &&
                        string.CompareOrdinal(obj.Key, watermarkKey) <= 0)
                    {
                        continue;
                    }

                    // Listing is already filtered by effectiveAfter, so no extra filter is usually needed here
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

                            // Mode A: always overwrite (sink implementation must support that behavior)
                            await _sink.UploadAsync(req.TargetContainer, blobName, stream, metadata, token).ConfigureAwait(false);

                            await _manifest.AppendSuccessAsync(req.ManifestName, obj, blobName, token).ConfigureAwait(false);

                            var currentIndex = Interlocked.Increment(ref _progressCounter);
                            Interlocked.Add(ref _totalBytes, obj.Size);

                            Interlocked.Increment(ref copied);
                            Interlocked.Increment(ref processed);

                            MaybeLogProgress(opt.ProgressLogInterval, currentIndex, copied, failed, processed, obj, blobName, i);

                            await committer.MarkDoneAsync(item.Seq, obj, token).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref failed);
                            Interlocked.Increment(ref processed);

                            _log.LogError(ex, "Copy failed for {Key} (seq={Seq}). Cancelling run.", obj.Key, item.Seq);

                            // Fail-fast: cancel entire run to avoid gaps and uncertain watermark state
                            linkedCts.Cancel();
                            throw;
                        }
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    // Expected during fail-fast cancellation
                }
            }, token))
            .ToArray();

        // 6) Join + final checkpoint flush
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
            // If something fails, try to persist the last fully committed watermark
            try { await committer.FlushAsync(CancellationToken.None).ConfigureAwait(false); } catch { /* best effort */ }
            throw;
        }

        _globalSw.Stop();

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

    private void MaybeLogProgress(
        TimeSpan progressLogInterval,
        long currentIndex,
        int copied,
        int failed,
        int processed,
        EcsObjectDescriptor obj,
        string blobName,
        int workerId)
    {
        bool shouldLog = false;

        lock (_progressLogLock)
        {
            if (DateTimeOffset.UtcNow - _lastProgressLogUtc >= progressLogInterval)
            {
                _lastProgressLogUtc = DateTimeOffset.UtcNow;
                shouldLog = true;
            }
        }

        if (!shouldLog)
            return;

        var elapsed = _globalSw.Elapsed.TotalSeconds;
        var objectsPerSecond = currentIndex / Math.Max(1d, elapsed);
        var mbPerSecond = (Interlocked.Read(ref _totalBytes) / 1024d / 1024d) / Math.Max(1d, elapsed);

        _log.LogInformation(
            "[Progress] Index={Index}, Copied={Copied}, Failed={Failed}, Processed={Processed}, ObjectsPerSecond={ObjectsPerSecond:F2}, MBPerSecond={MBPerSecond:F2}, CurrentKey={Key}, TargetBlob={Blob}, Worker={Worker}",
            currentIndex,
            copied,
            failed,
            processed,
            objectsPerSecond,
            mbPerSecond,
            obj.Key,
            blobName,
            workerId);
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
                // Store successful item in completion map
                _done[seq] = obj;

                // Commit continuous prefix (ordered completion)
                while (_done.TryGetValue(_nextToCommit, out var nextObj))
                {
                    _done.Remove(_nextToCommit);
                    _nextToCommit++;

                    // Watermark moves to the last fully committed object
                    CurrentWatermark = (nextObj.LastModifiedUtc, nextObj.Key);
                    _commitCountSinceSave++;

                    // Prepare checkpoint save when threshold is reached
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
