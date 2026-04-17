using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;

public static class EcsVersionCleanup
{
    public static IAmazonS3 CreateS3Client()
    {
        var serviceUrl = "https://ecs.example.com";
        var accessKey = "ECS_ACCESS_KEY";
        var secretKey = "ECS_SECRET_KEY";
        var region = "us-east-1";

        var credentials = new BasicAWSCredentials(accessKey, secretKey);

        var config = new AmazonS3Config
        {
            ServiceURL = serviceUrl,
            ForcePathStyle = true,
            AuthenticationRegion = region,
            SignatureVersion = "4"
        };

        return new AmazonS3Client(credentials, config);
    }

    public static async Task DeleteAllObjectVersionsAsync(
        IAmazonS3 s3,
        string bucket,
        CancellationToken ct)
    {
        string keyMarker = null;
        string versionIdMarker = null;

        do
        {
            var listRequest = new ListVersionsRequest
            {
                BucketName = bucket,
                KeyMarker = keyMarker,
                VersionIdMarker = versionIdMarker
            };

            var response = await s3.ListVersionsAsync(listRequest, ct);

            var toDelete = new List<KeyVersion>();

            foreach (var version in response.Versions)
            {
                toDelete.Add(new KeyVersion
                {
                    Key = version.Key,
                    VersionId = version.VersionId
                });
            }

            foreach (var batch in Batch(toDelete, 1000))
            {
                var deleteRequest = new DeleteObjectsRequest
                {
                    BucketName = bucket,
                    Objects = batch.ToList()
                };

                var deleteResponse = await s3.DeleteObjectsAsync(deleteRequest, ct);
                Console.WriteLine($"Deleted {deleteResponse.DeletedObjects.Count} object versions");
            }

            keyMarker = response.IsTruncated == true ? response.NextKeyMarker : null;
            versionIdMarker = response.IsTruncated == true ? response.NextVersionIdMarker : null;

        } while (responseHasMore(keyMarker, versionIdMarker));
    }

    private static bool responseHasMore(string keyMarker, string versionIdMarker)
    {
        return !string.IsNullOrWhiteSpace(keyMarker) || !string.IsNullOrWhiteSpace(versionIdMarker);
    }

    private static IEnumerable<List<T>> Batch<T>(IEnumerable<T> source, int size)
    {
        var batch = new List<T>(size);

        foreach (var item in source)
        {
            batch.Add(item);

            if (batch.Count == size)
            {
                yield return batch;
                batch = new List<T>(size);
            }
        }

        if (batch.Count > 0)
            yield return batch;
    }

    public static async Task Main()
    {
        var bucketName = "my-bucket";

        using var s3 = CreateS3Client();

        await DeleteAllObjectVersionsAsync(s3, bucketName, CancellationToken.None);

        Console.WriteLine("Finished deleting all object versions.");
    }
}
