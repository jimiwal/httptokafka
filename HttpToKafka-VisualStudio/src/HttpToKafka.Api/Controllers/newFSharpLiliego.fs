open System
open System.Globalization
open System.Threading
open Microsoft.Extensions.Logging
open EcsToAzureBlobCopier.SingleFile

module Program =

    let private parseModifiedAfter (value: string) : Nullable<DateTimeOffset> =
        if String.IsNullOrWhiteSpace(value) then
            Nullable()
        else
            let dt =
                DateTime.ParseExact(
                    value,
                    "yyyy-MM-dd",
                    CultureInfo.InvariantCulture,
                    DateTimeStyles.AssumeUniversal)

            Nullable(DateTimeOffset(dt, TimeSpan.Zero))

    [<EntryPoint>]
    let main _argv =
        // ECS connection settings - this should be read from configuration.
        let ecsServiceUrl = "https://ecs.example.com"
        let ecsAccessKey = "ECS_ACCESS_KEY"
        let ecsSecretKey = "ECS_SECRET_KEY"
        let ecsRegion = "us-east-1"
        let ecsForcePathStyle = true

        // Azure Blob connection settings - this should be read from configuration.
        let azureBlobServiceUri = "https://youraccount.blob.core.windows.net/"

        // Shared Azure container names - this should be read from configuration.
        let checkpointContainer = "ecs-copy-state"
        let manifestContainer = "ecs-copy-manifest"

        // Migration request settings - this should be read from configuration.
        let sourceBucket = "my-bucket"
        let sourcePrefix = "data/"
        let targetContainer = "backup"
        let targetPrefix = "ecs/"
        let checkpointName = "myjob.json"
        let manifestName = "myjob-success.log"

        // ModifiedAfterUtc may be empty => null, or for example "2026-01-01".
        // this should be read from configuration.
        let modifiedAfterRaw = ""

        // Parallel copy options - this should be read from configuration.
        let maxDegreeOfParallelism = 16
        let boundedCapacity = 512
        let checkpointEveryCommits = 25
        let checkpointMinIntervalSeconds = 5
        let replayWindowSeconds = 0
        let progressLogIntervalMinutes = 10

        // Retry and blacklist settings - this should be read from configuration.
        let maxReadRetries = 3
        let retryBaseDelaySeconds = 2
        let blacklistBlobName = "BlackList.json"

        use loggerFactory =
            LoggerFactory.Create(fun builder ->
                builder
                    .AddSimpleConsole(fun o ->
                        o.SingleLine <- true
                        o.TimestampFormat <- "yyyy-MM-dd HH:mm:ss ")
                    .SetMinimumLevel(LogLevel.Information)
                |> ignore)

        let ecsOptions =
            EcsS3Options(
                ServiceUrl = Uri(ecsServiceUrl),
                AccessKeyId = ecsAccessKey,
                SecretAccessKey = ecsSecretKey,
                ForcePathStyle = ecsForcePathStyle,
                Region = ecsRegion
            )

        let source = EcsS3ObjectSource(ecsOptions)

        let sink =
            AzureBlobObjectSink(
                AzureBlobOptions(
                    BlobServiceUri = Uri(azureBlobServiceUri)
                ))

        let checkpointStore =
            BlobCheckpointStore(
                BlobCheckpointStoreOptions(
                    BlobServiceUri = Uri(azureBlobServiceUri),
                    Container = checkpointContainer
                ))

        let manifestWriter =
            BlobAppendManifestWriter(
                BlobManifestWriterOptions(
                    BlobServiceUri = Uri(azureBlobServiceUri),
                    Container = manifestContainer
                ))

        let blacklistStore =
            BlobJsonBlacklistStore(
                BlobCheckpointStoreOptions(
                    BlobServiceUri = Uri(azureBlobServiceUri),
                    Container = checkpointContainer
                ),
                blacklistBlobName
            )

        let runner =
            ParallelCopyRunner(
                source,
                sink,
                checkpointStore,
                manifestWriter,
                blacklistStore,
                loggerFactory.CreateLogger<ParallelCopyRunner>()
            )

        let request =
            CopyRequest(
                SourceBucket = sourceBucket,
                SourcePrefix = sourcePrefix,
                TargetContainer = targetContainer,
                TargetPrefix = targetPrefix,
                ModifiedAfterUtc = parseModifiedAfter modifiedAfterRaw,
                CheckpointName = checkpointName,
                ManifestName = manifestName
            )

        let options =
            ParallelCopyOptions(
                MaxDegreeOfParallelism = maxDegreeOfParallelism,
                BoundedCapacity = boundedCapacity,
                CheckpointEveryCommits = checkpointEveryCommits,
                CheckpointMinInterval = TimeSpan.FromSeconds(float checkpointMinIntervalSeconds),
                ReplayWindow = TimeSpan.FromSeconds(float replayWindowSeconds),
                ProgressLogInterval = TimeSpan.FromMinutes(float progressLogIntervalMinutes),
                MaxReadRetries = maxReadRetries,
                RetryBaseDelay = TimeSpan.FromSeconds(float retryBaseDelaySeconds),
                BlacklistBlobName = blacklistBlobName
            )

        let result =
            runner.RunAsync(request, options, CancellationToken.None)
                  .GetAwaiter()
                  .GetResult()

        printfn ""
        printfn "===== FINAL RESULT ====="
        printfn "CopiedCount              : %d" result.CopiedCount
        printfn "FailedCount              : %d" result.FailedCount
        printfn "TotalProcessed           : %d" result.TotalProcessed
        printfn "WatermarkKey             : %s" (if isNull result.WatermarkKey then "<null>" else result.WatermarkKey)
        printfn "WatermarkLastModifiedUtc : %O" result.WatermarkLastModifiedUtc

        0
