// Program.cs
// .NET 6+
// NuGet: AWSSDK.S3
//
// WYPEŁNIJ TE STAŁE: ECS URL, AccessKey, SecretKey, Bucket.
// Program:
// 1) znajduje 100 największych obiektów w bucket
// 2) pobiera je 1 po 1 do pamięci (MemoryStream)
// 3) liczy: avg obiekty/sek, min i max "czas pobierania na minutę" (suma czasów downloadów w danej minucie)
// 4) zapisuje statystyki do output.txt (lokalnie)

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

internal static class Program
{
    // ====== KONFIGURACJA (WPROWADŹ SWOJE DANE) ======
    private const string ECS_SERVICE_URL = "https://ecs.example.com";
    private const string ECS_ACCESS_KEY = "your-access-key";
    private const string ECS_SECRET_KEY = "your-secret-key";
    private const string ECS_REGION = "us-east-1";
    private const bool ECS_FORCE_PATH_STYLE = true;

    private const string BUCKET_NAME = "your-bucket-name";

    // ile największych obiektów wybieramy
    private const int TARGET_COUNT = 100;

    // plik wynikowy
    private const string OUTPUT_FILE = "output.txt";
    // ===============================================

    private sealed record ObjRef(string Key, long Size, DateTime LastModifiedUtc);

    private static async Task Main()
    {
        using var s3 = CreateEcsClient();

        Console.WriteLine($"ECS URL: {ECS_SERVICE_URL}");
        Console.WriteLine($"Bucket : {BUCKET_NAME}");
        Console.WriteLine($"Select : {TARGET_COUNT:N0} largest");
        Console.WriteLine();

        // 1) Find largest
        Console.WriteLine("Listing objects and selecting largest...");
        var largest = await FindLargestAsync(s3, BUCKET_NAME, TARGET_COUNT, CancellationToken.None);
        Console.WriteLine($"Selected {largest.Count:N0} objects.\n");

        if (largest.Count == 0)
        {
            var msg = "No objects found.\n";
            Console.WriteLine(msg);
            await File.WriteAllTextAsync(OUTPUT_FILE, msg);
            return;
        }

        // 2) Download + measure
        Console.WriteLine("Downloading selected objects sequentially...");
        var stats = await DownloadAndMeasureAsync(s3, BUCKET_NAME, largest, CancellationToken.None);

        // 3) Render + save output
        var report = BuildReport(stats, largest);
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

    private static async Task<List<ObjRef>> FindLargestAsync(
        IAmazonS3 s3,
        string bucket,
        int k,
        CancellationToken ct)
    {
        // Trzymamy k największych.
        // PriorityQueue jest min-heap, więc priorytet = Size.
        // Na górze mamy najmniejszy z aktualnie trzymanych największych.
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
                    LastModifiedUtc: o.LastModified.ToUniversalTime()
                );

                if (pq.Count < k)
                {
                    pq.Enqueue(obj, obj.Size);
                }
                else
                {
                    var currentSmallestAmongLargest = pq.Peek();
                    if (obj.Size > currentSmallestAmongLargest.Size)
                    {
                        pq.Dequeue();
                        pq.Enqueue(obj, obj.Size);
                    }
                }

                if (listed % 200_000 == 0)
                    Console.WriteLine($"  listed: {listed:N0} ... kept: {pq.Count:N0}");
            }

            continuation = resp.IsTruncated ? resp.NextContinuationToken : null;
        }
        while (continuation is not null);

        Console.WriteLine($"Listing done. Total listed: {listed:N0}");

        var list = new List<ObjRef>(pq.Count);
        while (pq.Count > 0) list.Add(pq.Dequeue());

        // sort descending by size (largest first)
        list.Sort((a, b) =>
        {
            var c = b.Size.CompareTo(a.Size);
            return c != 0 ? c : string.CompareOrdinal(a.Key, b.Key);
        });

        return list;
    }

    private sealed record DownloadStats(
        int TotalCount,
        double TotalSeconds,
        double AvgObjectsPerSecond,
        double? MinMinuteTotalSeconds,
        double? MaxMinuteTotalSeconds,
        IReadOnlyDictionary<int, double> MinuteTotalsSeconds
    );

    private static async Task<DownloadStats> DownloadAndMeasureAsync(
        IAmazonS3 s3,
        string bucket,
        List<ObjRef> objs,
        CancellationToken ct)
    {
        var overall = Stopwatch.StartNew();
        var startUtc = DateTimeOffset.UtcNow;

        // minuteIndex -> sum download durations within that minute (seconds)
        var minuteTotals = new ConcurrentDictionary<int, double>();

        for (int i = 0; i < objs.Count; i++)
        {
            ct.ThrowIfCancellationRequested();

            var obj = objs[i];
            var sw = Stopwatch.StartNew();

            using var resp = await s3.GetObjectAsync(new GetObjectRequest
            {
                BucketName = bucket,
                Key = obj.Key
            }, ct).ConfigureAwait(false);

            await using (resp.ResponseStream.ConfigureAwait(false))
            {
                // download into memory only
                using var ms = obj.Size <= int.MaxValue ? new MemoryStream((int)obj.Size) : new MemoryStream();
                await resp.ResponseStream.CopyToAsync(ms, 81920, ct).ConfigureAwait(false);
                _ = ms.Length;
            }

            sw.Stop();

            var nowUtc = DateTimeOffset.UtcNow;
            var minuteIndex = (int)Math.Floor((nowUtc - startUtc).TotalMinutes);

            minuteTotals.AddOrUpdate(minuteIndex, sw.Elapsed.TotalSeconds, (_, old) => old + sw.Elapsed.TotalSeconds);

            Console.WriteLine(
                $"  downloaded: {i + 1:N0}/{objs.Count:N0} | key: {obj.Key} | size: {obj.Size:N0} bytes | elapsed: {sw.Elapsed.TotalSeconds:F2} s");
        }

        overall.Stop();

        var totalSeconds = overall.Elapsed.TotalSeconds;
        var avgOps = objs.Count / Math.Max(0.000001, totalSeconds);

        double? minMinute = null;
        double? maxMinute = null;

        foreach (var kvp in minuteTotals)
        {
            var sec = kvp.Value;
            minMinute = minMinute is null ? sec : Math.Min(minMinute.Value, sec);
            maxMinute = maxMinute is null ? sec : Math.Max(maxMinute.Value, sec);
        }

        return new DownloadStats(
            TotalCount: objs.Count,
            TotalSeconds: totalSeconds,
            AvgObjectsPerSecond: avgOps,
            MinMinuteTotalSeconds: minuteTotals.Count >= 1 ? minMinute : null,
            MaxMinuteTotalSeconds: minuteTotals.Count >= 1 ? maxMinute : null,
            MinuteTotalsSeconds: new SortedDictionary<int, double>(minuteTotals)
        );
    }

    private static string BuildReport(DownloadStats stats, List<ObjRef> selected)
    {
        long totalSelectedBytes = 0;
        foreach (var o in selected) totalSelectedBytes += o.Size;

        var sb = new System.Text.StringBuilder();

        sb.AppendLine("===== ECS Large-Object Download Benchmark =====");
        sb.AppendLine($"UTC Timestamp : {DateTimeOffset.UtcNow:O}");
        sb.AppendLine($"ECS URL       : {ECS_SERVICE_URL}");
        sb.AppendLine($"Bucket        : {BUCKET_NAME}");
        sb.AppendLine($"Selected      : {selected.Count:N0} largest objects");
        sb.AppendLine($"Selected bytes: {totalSelectedBytes.ToString(CultureInfo.InvariantCulture)} ({(totalSelectedBytes / 1024.0 / 1024.0 / 1024.0):F2} GiB)");
        sb.AppendLine();

        sb.AppendLine("===== RESULTS =====");
        sb.AppendLine($"Downloaded objects : {stats.TotalCount:N0}");
        sb.AppendLine($"Total time         : {stats.TotalSeconds:F2} s");
        sb.AppendLine($"Avg objects/sec    : {stats.AvgObjectsPerSecond:F4}");

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
        sb.AppendLine("===== Per-minute buckets (sum of download durations) =====");
        sb.AppendLine("MinuteIndex\tTotalDownloadSeconds");
        foreach (var kvp in stats.MinuteTotalsSeconds)
            sb.AppendLine($"{kvp.Key}\t\t{kvp.Value:F2}");

        sb.AppendLine();
        sb.AppendLine("===== Selected objects =====");
        sb.AppendLine("Rank\tSizeBytes\tLastModifiedUtc\tKey");
        for (int i = 0; i < selected.Count; i++)
        {
            var o = selected[i];
            sb.AppendLine($"{i + 1}\t{o.Size}\t{o.LastModifiedUtc:O}\t{o.Key}");
        }

        return sb.ToString();
    }
}
