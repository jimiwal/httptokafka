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

        // Migration request settings - this should be read from configuration.
        let sourceBucket = "my-bucket"
        let sourcePrefix = "data/"                  // this should be read from configuration.
        let targetContainer = "backup"             // this should be read from configuration.
        let targetPrefix = "ecs/"                  // this should be read from configuration.
        let checkpointName = "myjob.json"          // this should be read from configuration.
        let manifestName = "myjob-success.log"     // this should be read from configuration.

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

        let source = new EcsS3ObjectSource(ecsOptions)

        let sink =
            new AzureBlobObjectSink(
                AzureBlobOptions(
                    BlobServiceUri = Uri(azureBlobServiceUri)
                ))

        let checkpointStore =
            new BlobCheckpointStore(
                BlobCheckpointStoreOptions(
                    BlobServiceUri = Uri(azureBlobServiceUri),
                    Container = "ecs-copy-state"   // this should be read from configuration.
                ))

        let manifestWriter =
            new BlobAppendManifestWriter(
                BlobManifestWriterOptions(
                    BlobServiceUri = Uri(azureBlobServiceUri),
                    Container = "ecs-copy-manifest" // this should be read from configuration.
                ))

        let runner =
            new ParallelCopyRunner(
                source,
                sink,
                checkpointStore,
                manifestWriter,
                loggerFactory.CreateLogger<ParallelCopyRunner>())

        let modifiedAfter = parseModifiedAfter modifiedAfterRaw

        let request =
            CopyRequest(
                SourceBucket = sourceBucket,
                SourcePrefix = sourcePrefix,
                TargetContainer = targetContainer,
                TargetPrefix = targetPrefix,
                ModifiedAfterUtc = modifiedAfter,
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
                ProgressLogInterval = TimeSpan.FromMinutes(float progressLogIntervalMinutes)
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
