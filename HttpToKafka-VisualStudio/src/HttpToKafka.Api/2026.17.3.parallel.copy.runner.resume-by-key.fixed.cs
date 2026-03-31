using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Azure.Identity;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Extensions.Logging;

namespace EcsToAzureBlobCopier.SingleFile;

public sealed record EcsObjectDescriptor(
    string Bucket,
    string Key,
    long Size,
    DateTimeOffset LastModifiedUtc,
    string? ETag
);

public interface IEcsObjectSource
{
    IAsyncEnumerable<EcsObjectDescriptor> ListAsync(
        string bucket,
        string? prefix,
        DateTimeOffset? modifiedAfterUtc,
        string? startAfterKey,
        CancellationToken ct);

    Task<Stream> OpenReadAsync(string bucket, string key, CancellationToken ct);
}

public interface IBlobObjectSink
{
    Task UploadAsync(
        string container,
        string blobName,
        Stream content,
        IDictionary<string, string>? metadata,
        CancellationToken ct);
}

public sealed record CopyCheckpoint(
    string Bucket,
    string? Prefix,
    string Container,
    DateTimeOffset? ModifiedAfterUtc,
    string? LastProcessedKey,
    DateTimeOffset? LastProcessedLastModifiedUtc,
    DateTimeOffset UpdatedUtc
);

public interface ICheckpointStore
{
    Task<CopyCheckpoint?> TryLoadAsync(string checkpointName, CancellationToken ct);
    Task SaveAsync(string checkpointName, CopyCheckpoint checkpoint, CancellationToken ct);
}

public interface ICopyManifestWriter
{
    Task AppendSuccessAsync(string manifestName, EcsObjectDescriptor obj, string targetBlobName, CancellationToken ct);
}

public sealed class EcsS3Options
{
    public Uri ServiceUrl { get; init; }
    public string AccessKeyId { get; init; }
    public string SecretAccessKey { get; init; }
    public bool ForcePathStyle { get; init; } = true;
    public string Region { get; init; } = "us-east-1";
}

public sealed class AzureBlobOptions
{
    public Uri BlobServiceUri { get; init; }
}

public sealed class BlobCheckpointStoreOptions
{
    public Uri BlobServiceUri { get; init; }
    public string Container { get; init; }
}

public sealed class BlobManifestWriterOptions
{
    public Uri BlobServiceUri { get; init; }
    public string Container { get; init; }
}

public sealed class CopyRequest
{
    public string SourceBucket { get; init; }
    public string? SourcePrefix { get; init; }
    public string TargetContainer { get; init; }
    public string? TargetPrefix { get; init; }
    public DateTimeOffset? ModifiedAfterUtc { get; init; }
    public string CheckpointName { get; init; }
    public string ManifestName { get; init; }
}

public sealed class ParallelCopyOptions
{
    public int MaxDegreeOfParallelism { get; init; } = 16;
    public int BoundedCapacity { get; init; } = 512;
    public int CheckpointEveryCommits { get; init; } = 25;
    public TimeSpan CheckpointMinInterval { get; init; } = TimeSpan.FromSeconds(5);
    public TimeSpan ReplayWindow { get; init; } = TimeSpan.Zero; // Currently unused for resume-by-key mode
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

public sealed class EcsS3ObjectSource : IEcsObjectSource, IDisposable
{
    private readonly IAmazonS3 _s3;

    public EcsS3ObjectSource(EcsS3Options opt)
    {
        var creds = new BasicAWSCredentials(opt.AccessKeyId, opt.SecretAccessKey);
        var cfg = new AmazonS3Config
        {
            ServiceURL = opt.ServiceUrl.ToString(),
            ForcePathStyle = opt.ForcePathStyle,
            AuthenticationRegion = opt.Region,
            SignatureVersion = "4"
        };
        _s3 = new AmazonS3Client(creds, cfg);
    }

    public async IAsyncEnumerable<EcsObjectDescriptor> ListAsync(
        string bucket,
        string? prefix,
        DateTimeOffset? modifiedAfterUtc,
        string? startAfterKey,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken ct)
    {
        string? continuation = null;

        do
        {
            var req = new ListObjectsV2Request
            {
                BucketName = bucket,
                Prefix = prefix,
                ContinuationToken = continuation,
                StartAfter = startAfterKey
            };

            var resp = await _s3.ListObjectsV2Async(req, ct).ConfigureAwait(false);

            foreach (var o in resp.S3Objects.OrderBy(x => x.Key, StringComparer.Ordinal))
            {
                var lm = new DateTimeOffset(o.LastModified, TimeSpan.Zero);
                // Date filtering is only used for a fresh run when ModifiedAfterUtc is explicitly provided.
                // Resume logic is handled by StartAfter / LastProcessedKey, not by LastModified.
                if (modifiedAfterUtc is not null && lm <= modifiedAfterUtc.Value)
                    continue;

                yield return new EcsObjectDescriptor(bucket, o.Key, o.Size, lm, o.ETag);
            }

            continuation = resp.IsTruncated ? resp.NextContinuationToken : null;
            startAfterKey = null;
        }
        while (continuation is not null);
    }

    public async Task<Stream> OpenReadAsync(string bucket, string key, CancellationToken ct)
    {
        var resp = await _s3.GetObjectAsync(bucket, key, ct).ConfigureAwait(false);
        return new S3ResponseStream(resp);
    }

    public void Dispose() => _s3.Dispose();

    private sealed class S3ResponseStream : Stream
    {
        private readonly GetObjectResponse _response;
        private readonly Stream _inner;

        public S3ResponseStream(GetObjectResponse response)
        {
            _response = response;
            _inner = response.ResponseStream;
        }

        public override bool CanRead => _inner.CanRead;
        public override bool CanSeek => _inner.CanSeek;
        public override bool CanWrite => _inner.CanWrite;
        public override long Length => _inner.Length;
        public override long Position { get => _inner.Position; set => _inner.Position = value; }
        public override void Flush() => _inner.Flush();
        public override int Read(byte[] buffer, int offset, int count) => _inner.Read(buffer, offset, count);
        public override long Seek(long offset, SeekOrigin origin) => _inner.Seek(offset, origin);
        public override void SetLength(long value) => _inner.SetLength(value);
        public override void Write(byte[] buffer, int offset, int count) => _inner.Write(buffer, offset, count);
        public override ValueTask DisposeAsync()
        {
            _response.Dispose();
            return ValueTask.CompletedTask;
        }
        protected override void Dispose(bool disposing)
        {
            if (disposing) _response.Dispose();
            base.Dispose(disposing);
        }
        public override async Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => await _inner.ReadAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        public override async ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
            => await _inner.ReadAsync(buffer, cancellationToken).ConfigureAwait(false);
        public override async Task CopyToAsync(Stream destination, int bufferSize, CancellationToken cancellationToken)
            => await _inner.CopyToAsync(destination, bufferSize, cancellationToken).ConfigureAwait(false);
        public override async Task FlushAsync(CancellationToken cancellationToken)
            => await _inner.FlushAsync(cancellationToken).ConfigureAwait(false);
        public override async Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
            => await _inner.WriteAsync(buffer, offset, count, cancellationToken).ConfigureAwait(false);
        public override async ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
            => await _inner.WriteAsync(buffer, cancellationToken).ConfigureAwait(false);
    }
}

public sealed class AzureBlobObjectSink : IBlobObjectSink
{
    private readonly BlobServiceClient _svc;

    public AzureBlobObjectSink(AzureBlobOptions opt)
    {
        _svc = new BlobServiceClient(opt.BlobServiceUri, new DefaultAzureCredential());
    }

    public async Task UploadAsync(
        string container,
        string blobName,
        Stream content,
        IDictionary<string, string>? metadata,
        CancellationToken ct)
    {
        var containerClient = _svc.GetBlobContainerClient(container);
        await containerClient.CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);

        var blob = containerClient.GetBlobClient(blobName);
        await blob.DeleteIfExistsAsync(cancellationToken: ct).ConfigureAwait(false);

        var opts = new BlobUploadOptions
        {
            Metadata = metadata is null ? null : new Dictionary<string, string>(metadata)
        };

        await blob.UploadAsync(content, opts, ct).ConfigureAwait(false);
    }
}

public sealed class BlobCheckpointStore : ICheckpointStore
{
    private readonly BlobContainerClient _container;
    private static readonly JsonSerializerOptions JsonOpt = new(JsonSerializerDefaults.Web);

    public BlobCheckpointStore(BlobCheckpointStoreOptions opt)
    {
        var svc = new BlobServiceClient(opt.BlobServiceUri, new DefaultAzureCredential());
        _container = svc.GetBlobContainerClient(opt.Container);
    }

    public async Task<CopyCheckpoint?> TryLoadAsync(string checkpointName, CancellationToken ct)
    {
        await _container.CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);
        var blob = _container.GetBlobClient(checkpointName);

        try
        {
            var dl = await blob.DownloadStreamingAsync(cancellationToken: ct).ConfigureAwait(false);
            using var reader = new StreamReader(dl.Value.Content, Encoding.UTF8);
            var json = await reader.ReadToEndAsync().ConfigureAwait(false);
            return JsonSerializer.Deserialize<CopyCheckpoint>(json, JsonOpt);
        }
        catch (Azure.RequestFailedException ex) when (ex.Status == 404)
        {
            return null;
        }
    }

    public async Task SaveAsync(string checkpointName, CopyCheckpoint checkpoint, CancellationToken ct)
    {
        await _container.CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);
        var blob = _container.GetBlobClient(checkpointName);
        var json = JsonSerializer.Serialize(checkpoint, JsonOpt);
        using var ms = new MemoryStream(Encoding.UTF8.GetBytes(json));
        await blob.UploadAsync(ms, overwrite: true, cancellationToken: ct).ConfigureAwait(false);
    }
}

public sealed class BlobAppendManifestWriter : ICopyManifestWriter
{
    private readonly BlobContainerClient _container;
    private readonly SemaphoreSlim _appendLock = new(1, 1);

    public BlobAppendManifestWriter(BlobManifestWriterOptions opt)
    {
        var svc = new BlobServiceClient(opt.BlobServiceUri, new DefaultAzureCredential());
        _container = svc.GetBlobContainerClient(opt.Container);
    }

    public async Task AppendSuccessAsync(string manifestName, EcsObjectDescriptor obj, string targetBlobName, CancellationToken ct)
    {
        await _appendLock.WaitAsync(ct).ConfigureAwait(false);
        try
        {
            await _container.CreateIfNotExistsAsync(cancellationToken: ct).ConfigureAwait(false);
            var append = _container.GetAppendBlobClient(manifestName);
            if (!await append.ExistsAsync(ct).ConfigureAwait(false))
                await append.CreateAsync(cancellationToken: ct).ConfigureAwait(false);

            var line = $"{DateTimeOffset.UtcNow:O}	{obj.Bucket}	{obj.Key}	{obj.LastModifiedUtc:O}	{obj.Size}	{targetBlobName}\n";
            using var ms = new MemoryStream(Encoding.UTF8.GetBytes(line));
            await append.AppendBlockAsync(ms, cancellationToken: ct).ConfigureAwait(false);
        }
        finally
        {
            _appendLock.Release();
        }
    }
}

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
    private long _copiedCount;
    private long _failedCount;
    private long _processedCount;
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

    public async Task<ParallelCopyResult> RunAsync(CopyRequest req, ParallelCopyOptions opt, CancellationToken ct)
    {
        _lastProgressLogUtc = DateTimeOffset.MinValue;
        Interlocked.Exchange(ref _progressCounter, 0);
        Interlocked.Exchange(ref _totalBytes, 0);
        Interlocked.Exchange(ref _copiedCount, 0);
        Interlocked.Exchange(ref _failedCount, 0);
        Interlocked.Exchange(ref _processedCount, 0);
        _globalSw = Stopwatch.StartNew();

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

        var resumeFromKey = cp?.LastProcessedKey;
        var effectiveAfter = cp is null ? req.ModifiedAfterUtc : null;

        _log.LogInformation(
            "Parallel copy start. Bucket={Bucket}, Prefix={Prefix}, TargetContainer={Container}, DOP={Dop}, Capacity={Cap}, ResumeFromKey={ResumeFromKey}, InitialModifiedAfter={After}, ProgressLogInterval={ProgressLogInterval}",
            req.SourceBucket, req.SourcePrefix, req.TargetContainer, opt.MaxDegreeOfParallelism, opt.BoundedCapacity,
            resumeFromKey, effectiveAfter, opt.ProgressLogInterval);

        var channel = Channel.CreateBounded<SequencedObject>(new BoundedChannelOptions(opt.BoundedCapacity)
        {
            FullMode = BoundedChannelFullMode.Wait,
            SingleWriter = true,
            SingleReader = false
        });

        using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
        var token = linkedCts.Token;

        var committer = new OrderedCheckpointCommitter(
            req.CheckpointName,
            cp,
            _checkpointStore,
            req,
            _log,
            opt.CheckpointEveryCommits,
            opt.CheckpointMinInterval);

        var producer = Task.Run(async () =>
        {
            try
            {
                long seq = 0;

                await foreach (var obj in _source.ListAsync(
                    req.SourceBucket,
                    req.SourcePrefix,
                    effectiveAfter,
                    resumeFromKey,
                    token).ConfigureAwait(false))
                {
                    token.ThrowIfCancellationRequested();
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

                            await _sink.UploadAsync(req.TargetContainer, blobName, stream, metadata, token).ConfigureAwait(false);
                            await _manifest.AppendSuccessAsync(req.ManifestName, obj, blobName, token).ConfigureAwait(false);

                            var currentIndex = Interlocked.Increment(ref _progressCounter);
                            Interlocked.Add(ref _totalBytes, obj.Size);
                            Interlocked.Increment(ref _copiedCount);
                            Interlocked.Increment(ref _processedCount);

                            MaybeLogProgress(opt.ProgressLogInterval, currentIndex, obj, blobName, i);
                            await committer.MarkDoneAsync(item.Seq, obj, token).ConfigureAwait(false);
                        }
                        catch (Exception ex)
                        {
                            Interlocked.Increment(ref _failedCount);
                            Interlocked.Increment(ref _processedCount);

                            _log.LogError(ex, "Copy failed for {Key} (seq={Seq}). Cancelling run.", obj.Key, item.Seq);
                            linkedCts.Cancel();
                            throw;
                        }
                    }
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                }
            }, token))
            .ToArray();

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
            try { await committer.FlushAsync(CancellationToken.None).ConfigureAwait(false); } catch { }
            throw;
        }

        _globalSw.Stop();

        var finalWm = committer.CurrentWatermark;

        var copied = (int)Interlocked.Read(ref _copiedCount);
        var failed = (int)Interlocked.Read(ref _failedCount);
        var processed = (int)Interlocked.Read(ref _processedCount);

        _log.LogInformation(
            "Parallel copy finished. Copied={Copied}, Failed={Failed}, Processed={Processed}, Watermark=({WmLm},{WmKey})",
            copied, failed, processed, finalWm.LastModifiedUtc, finalWm.Key);

        return new ParallelCopyResult(copied, failed, processed, finalWm.Key, finalWm.LastModifiedUtc);
    }

    private void MaybeLogProgress(
        TimeSpan progressLogInterval,
        long currentIndex,
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

        if (!shouldLog) return;

        var elapsed = _globalSw.Elapsed.TotalSeconds;
        var objectsPerSecond = currentIndex / Math.Max(1d, elapsed);
        var mbPerSecond = (Interlocked.Read(ref _totalBytes) / 1024d / 1024d) / Math.Max(1d, elapsed);

        var copied = Interlocked.Read(ref _copiedCount);
        var failed = Interlocked.Read(ref _failedCount);
        var processed = Interlocked.Read(ref _processedCount);

        _log.LogInformation(
            "[Progress] Index={Index}, Copied={Copied}, Failed={Failed}, Processed={Processed}, ObjectsPerSecond={ObjectsPerSecond:F2}, MBPerSecond={MBPerSecond:F2}, CurrentKey={Key}, TargetBlob={Blob}, Worker={Worker}",
            currentIndex, copied, failed, processed, objectsPerSecond, mbPerSecond, obj.Key, blobName, workerId);
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
                _done[seq] = obj;

                while (_done.TryGetValue(_nextToCommit, out var nextObj))
                {
                    _done.Remove(_nextToCommit);
                    _nextToCommit++;
                    CurrentWatermark = (nextObj.LastModifiedUtc, nextObj.Key);
                    _commitCountSinceSave++;

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
            => new(
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
            _log.LogInformation(
                "Checkpoint saved. Watermark=({WmLm},{WmKey}) UpdatedUtc={Updated}",
                cp.LastProcessedLastModifiedUtc, cp.LastProcessedKey, cp.UpdatedUtc);
        }
    }
}

public static class Program
{
    public static async Task Main(string[] args)
    {
        var ecsOptions = new EcsS3Options
        {
            ServiceUrl = new Uri("https://ecs.example.com"),
            AccessKeyId = "ECS_ACCESS_KEY",
            SecretAccessKey = "ECS_SECRET_KEY",
            ForcePathStyle = true,
            Region = "us-east-1"
        };

        var azureBlobServiceUri = new Uri("https://youraccount.blob.core.windows.net/");

        using var loggerFactory = LoggerFactory.Create(builder =>
        {
            builder.AddSimpleConsole(o =>
            {
                o.SingleLine = true;
                o.TimestampFormat = "yyyy-MM-dd HH:mm:ss ";
            });
            builder.SetMinimumLevel(LogLevel.Information);
        });

        var source = new EcsS3ObjectSource(ecsOptions);
        var sink = new AzureBlobObjectSink(new AzureBlobOptions
        {
            BlobServiceUri = azureBlobServiceUri
        });
        var checkpointStore = new BlobCheckpointStore(new BlobCheckpointStoreOptions
        {
            BlobServiceUri = azureBlobServiceUri,
            Container = "ecs-copy-state"
        });
        var manifestWriter = new BlobAppendManifestWriter(new BlobManifestWriterOptions
        {
            BlobServiceUri = azureBlobServiceUri,
            Container = "ecs-copy-manifest"
        });

        var runner = new ParallelCopyRunner(
            source,
            sink,
            checkpointStore,
            manifestWriter,
            loggerFactory.CreateLogger<ParallelCopyRunner>());

        var request = new CopyRequest
        {
            SourceBucket = "my-bucket",
            SourcePrefix = "data/",
            TargetContainer = "backup",
            TargetPrefix = "ecs/",
            ModifiedAfterUtc = new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero),
            CheckpointName = "myjob.json",
            ManifestName = "myjob-success.log"
        };

        var options = new ParallelCopyOptions
        {
            MaxDegreeOfParallelism = 16,
            BoundedCapacity = 512,
            CheckpointEveryCommits = 25,
            CheckpointMinInterval = TimeSpan.FromSeconds(5),
            ReplayWindow = TimeSpan.Zero,
            ProgressLogInterval = TimeSpan.FromMinutes(10)
        };

        var result = await runner.RunAsync(request, options, CancellationToken.None);

        Console.WriteLine();
        Console.WriteLine("===== FINAL RESULT =====");
        Console.WriteLine($"CopiedCount              : {result.CopiedCount}");
        Console.WriteLine($"FailedCount              : {result.FailedCount}");
        Console.WriteLine($"TotalProcessed           : {result.TotalProcessed}");
        Console.WriteLine($"WatermarkKey             : {result.WatermarkKey}");
        Console.WriteLine($"WatermarkLastModifiedUtc : {result.WatermarkLastModifiedUtc:O}");
    }
}
