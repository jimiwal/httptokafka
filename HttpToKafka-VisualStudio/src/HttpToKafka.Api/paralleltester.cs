// Program.cs
// .NET 6+
// NuGet: AWSSDK.S3
//
// Program:
// 1) finds 100,000 smallest objects in the given ECS bucket
// 2) downloads them to memory with 10 parallel downloads
// 3) measures:
//    - average objects/sec
//    - average MB/sec
//    - min total download time per minute
//    - max total download time per minute
// 4) saves the final report to output.txt
//
// Fill in the constants below.

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

internal static class Program
{
    // ===== CONFIG =====
    private const string ECS_SERVICE_URL = "https://ecs.example.com";
    private const string ECS_ACCESS_KEY = "your-access-key";
    private const string ECS_SECRET_KEY = "your-secret-key";
    private const string ECS_REGION = "us-east-1";
    private const bool ECS_FORCE_PATH_STYLE = true;

    private const string BUCKET_NAME = "your-bucket-name";

    private const int TARGET_COUNT = 100_000;
    private const int MAX_PARALLEL_DOWNLOADS = 10;

    private const string OUTPUT_FILE = "output.txt";
    // ==================

    private sealed record ObjRef(string Key, long Size, DateTime LastModifiedUtc);

    private sealed record DownloadItemResult(
        string Key,
        long Size,
        double ElapsedSeconds,
        int MinuteIndex
    );

    private sealed record DownloadStats(
        int TotalCount,
        long TotalBytes,
        double TotalSeconds,
        double AvgObjectsPerSecond,
        double AvgMiBPerSecond,
        double? MinMinuteTotalSeconds,
        double? MaxMinuteTotalSeconds,
        IReadOnlyDictionary<int, double> MinuteTotalsSeconds
    );

    private static async Task Main()
    {
        using var s3 = CreateEcsClient();

        Console.WriteLine($"ECS URL                : {ECS_SERVICE_URL}");
        Console.WriteLine($"Bucket                 : {BUCKET_NAME}");
        Console.WriteLine($"Selected smallest      : {TARGET_COUNT:N0}");
        Console.WriteLine($"Parallel downloads     : {MAX_PARALLEL_DOWNLOADS}");
        Console.WriteLine();

        Console.WriteLine("Listing objects and selecting the 100,000 smallest...");
        var smallest = await FindSmallestAsync(s3, BUCKET_NAME, TARGET_COUNT, CancellationToken.None);
        Console.WriteLine($"Selected {smallest.Count:N0} objects.\n");

        if (smallest.Count == 0)
        {
            const string msg = "No objects found.\n";
            Console.WriteLine(msg);
            await File.WriteAllTextAsync(OUTPUT_FILE, msg);
            return;
        }

        Console.WriteLine("Downloading selected objects with parallelism = 10...");
        var stats = await DownloadAndMeasureParallelAsync(
            s3,
            BUCKET_NAME,
            smallest,
            MAX_PARALLEL_DOWNLOADS,
            CancellationToken.None);

        var report = BuildReport(stats, smallest);

        Console.WriteLine();
        Console.WriteLine(report);

        await File.WriteAllTextAsync(OUTPUT_FILE, report);
        Console.WriteLine($"\nSaved stats to: {Path.GetFullPath(OUTPUT_FILE)}");
    }

    private static AmazonS3Client CreateEcsClient()
    {
        var creds = new BasicAWSCredentials(ECS_ACCESS_KEY, ECS_SECRET_KEY);
        var cfg = new AmazonS3Config
        {
            ServiceURL = ECS_SERVICE_URL,
            ForcePathStyle = ECS_FORCE_PATH_STYLE,
            AuthenticationRegion = ECS_REGION,
            SignatureVersion = "4"
        };

        return new AmazonS3Client(creds, cfg);
    }

    private static async Task<List<ObjRef>> FindSmallestAsync(
        IAmazonS3 s3,
        string bucket,
        int k,
        CancellationToken ct)
    {
        // Keep only k smallest objects in memory.
        // .NET PriorityQueue is a min-heap, so using priority = -Size gives us
        // effective max-heap behavior by size.
        var pq = new PriorityQueue<ObjRef, long>();

        string? continuation = null;
        long listed = 0;

        do
        {
            var req = new ListObjectsV2Request
            {
                BucketName = bucket,
                ContinuationToken = continuation
            };

            var resp = await s3.ListObjectsV2Async(req, ct).ConfigureAwait(false);

            foreach (var o in resp.S3Objects)
            {
                listed++;

                var obj = new ObjRef(
                    Key: o.Key,
                    Size: o.Size,
                    LastModifiedUtc: o.LastModified.ToUniversalTime());

                if (pq.Count < k)
                {
                    pq.Enqueue(obj, -obj.Size);
                }
                else
                {
                    var currentLargestAmongSmallest = pq.Peek();
                    if (obj.Size < currentLargestAmongSmallest.Size)
                    {
                        pq.Dequeue();
                        pq.Enqueue(obj, -obj.Size);
                    }
                }

                if (listed % 200_000 == 0)
                {
                    Console.WriteLine($"  listed: {listed:N0} ... kept: {pq.Count:N0}");
                }
            }

            continuation = resp.IsTruncated ? resp.NextContinuationToken : null;
        }
        while (continuation is not null);

        Console.WriteLine($"Listing done. Total listed: {listed:N0}");

        var list = new List<ObjRef>(pq.Count);
        while (pq.Count > 0)
            list.Add(pq.Dequeue());

        list.Sort((a, b) =>
        {
            var sizeCmp = a.Size.CompareTo(b.Size);
            return sizeCmp != 0 ? sizeCmp : string.CompareOrdinal(a.Key, b.Key);
        });

        return list;
    }

    private static async Task<DownloadStats> DownloadAndMeasureParallelAsync(
        IAmazonS3 s3,
        string bucket,
        List<ObjRef> objects,
        int maxParallelDownloads,
        CancellationToken ct)
    {
        var overall = Stopwatch.StartNew();
        var benchmarkStartUtc = DateTimeOffset.UtcNow;

        long totalBytes = 0;
        int completedCount = 0;

        var minuteTotals = new ConcurrentDictionary<int, double>();
        var throttler = new SemaphoreSlim(maxParallelDownloads);
        var tasks = new List<Task>(objects.Count);

        foreach (var obj in objects)
        {
            await throttler.WaitAsync(ct).ConfigureAwait(false);

            tasks.Add(Task.Run(async () =>
            {
                try
                {
                    ct.ThrowIfCancellationRequested();

                    var sw = Stopwatch.StartNew();

                    using var resp = await s3.GetObjectAsync(new GetObjectRequest
                    {
                        BucketName = bucket,
                        Key = obj.Key
                    }, ct).ConfigureAwait(false);

                    await using (resp.ResponseStream.ConfigureAwait(false))
                    {
                        using var ms = obj.Size <= int.MaxValue
                            ? new MemoryStream((int)obj.Size)
                            : new MemoryStream();

                        await resp.ResponseStream.CopyToAsync(ms, 81920, ct).ConfigureAwait(false);
                        _ = ms.Length;
                    }

                    sw.Stop();

                    Interlocked.Add(ref totalBytes, obj.Size);
                    var done = Interlocked.Increment(ref completedCount);

                    var minuteIndex = (int)Math.Floor((DateTimeOffset.UtcNow - benchmarkStartUtc).TotalMinutes);
                    minuteTotals.AddOrUpdate(minuteIndex, sw.Elapsed.TotalSeconds, (_, old) => old + sw.Elapsed.TotalSeconds);

                    if (done % 1000 == 0)
                    {
                        var elapsed = overall.Elapsed.TotalSeconds;
                        var ops = done / Math.Max(0.000001, elapsed);
                        var mibPerSec = (totalBytes / 1024.0 / 1024.0) / Math.Max(0.000001, elapsed);

                        Console.WriteLine(
                            $"  downloaded: {done:N0}/{objects.Count:N0} | avg objs/sec: {ops:F2} | avg MiB/sec: {mibPerSec:F2}");
                    }
                }
                finally
                {
                    throttler.Release();
                }
            }, ct));
        }

        await Task.WhenAll(tasks).ConfigureAwait(false);

        overall.Stop();

        var totalSeconds = overall.Elapsed.TotalSeconds;
        var avgObjectsPerSecond = objects.Count / Math.Max(0.000001, totalSeconds);
        var avgMiBPerSecond = (totalBytes / 1024.0 / 1024.0) / Math.Max(0.000001, totalSeconds);

        double? minMinute = null;
        double? maxMinute = null;

        foreach (var kvp in minuteTotals)
        {
            minMinute = minMinute is null ? kvp.Value : Math.Min(minMinute.Value, kvp.Value);
            maxMinute = maxMinute is null ? kvp.Value : Math.Max(maxMinute.Value, kvp.Value);
        }

        return new DownloadStats(
            TotalCount: objects.Count,
            TotalBytes: totalBytes,
            TotalSeconds: totalSeconds,
            AvgObjectsPerSecond: avgObjectsPerSecond,
            AvgMiBPerSecond: avgMiBPerSecond,
            MinMinuteTotalSeconds: minuteTotals.Count > 0 ? minMinute : null,
            MaxMinuteTotalSeconds: minuteTotals.Count > 0 ? maxMinute : null,
            MinuteTotalsSeconds: new SortedDictionary<int, double>(minuteTotals)
        );
    }

    private static string BuildReport(DownloadStats stats, List<ObjRef> selected)
    {
        long totalSelectedBytes = 0;
        foreach (var o in selected)
            totalSelectedBytes += o.Size;

        var sb = new System.Text.StringBuilder();

        sb.AppendLine("===== ECS Small-Object Parallel Download Benchmark =====");
        sb.AppendLine($"UTC Timestamp       : {DateTimeOffset.UtcNow:O}");
        sb.AppendLine($"ECS URL             : {ECS_SERVICE_URL}");
        sb.AppendLine($"Bucket              : {BUCKET_NAME}");
        sb.AppendLine($"Selected objects    : {selected.Count:N0} smallest");
        sb.AppendLine($"Parallel downloads  : {MAX_PARALLEL_DOWNLOADS}");
        sb.AppendLine($"Selected bytes      : {totalSelectedBytes.ToString(CultureInfo.InvariantCulture)} ({(totalSelectedBytes / 1024.0 / 1024.0):F2} MiB)");
        sb.AppendLine();

        sb.AppendLine("===== RESULTS =====");
        sb.AppendLine($"Downloaded objects  : {stats.TotalCount:N0}");
        sb.AppendLine($"Downloaded bytes    : {stats.TotalBytes.ToString(CultureInfo.InvariantCulture)} ({(stats.TotalBytes / 1024.0 / 1024.0):F2} MiB)");
        sb.AppendLine($"Total time          : {stats.TotalSeconds:F2} s");
        sb.AppendLine($"Avg objects/sec     : {stats.AvgObjectsPerSecond:F2}");
        sb.AppendLine($"Avg MiB/sec         : {stats.AvgMiBPerSecond:F2}");

        if (stats.MinMinuteTotalSeconds is not null && stats.MaxMinuteTotalSeconds is not null)
        {
            sb.AppendLine($"Min download time per minute (sum of download durations in minute bucket): {stats.MinMinuteTotalSeconds.Value:F2} s");
            sb.AppendLine($"Max download time per minute (sum of download durations in minute bucket): {stats.MaxMinuteTotalSeconds.Value:F2} s");
        }
        else
        {
            sb.AppendLine("Per-minute stats    : not available.");
        }

        sb.AppendLine();
        sb.AppendLine("===== Per-minute buckets =====");
        sb.AppendLine("MinuteIndex\tTotalDownloadSeconds");
        foreach (var kvp in stats.MinuteTotalsSeconds)
        {
            sb.AppendLine($"{kvp.Key}\t\t{kvp.Value:F2}");
        }

        return sb.ToString();
    }
}
