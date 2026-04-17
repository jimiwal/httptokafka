using System;
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

    public static async Task DeleteAllObjectVersionsOneByOneAsync(
        IAmazonS3 s3,
        string bucket,
        CancellationToken ct)
    {
        string keyMarker = null;
        string versionIdMarker = null;
        long deletedCount = 0;

        do
        {
            var listRequest = new ListVersionsRequest
            {
                BucketName = bucket,
                KeyMarker = keyMarker,
                VersionIdMarker = versionIdMarker
            };

            var response = await s3.ListVersionsAsync(listRequest, ct).ConfigureAwait(false);

            foreach (var version in response.Versions)
            {
                ct.ThrowIfCancellationRequested();

                var deleteRequest = new DeleteObjectRequest
                {
                    BucketName = bucket,
                    Key = version.Key,
                    VersionId = version.VersionId
                };

                await s3.DeleteObjectAsync(deleteRequest, ct).ConfigureAwait(false);

                deletedCount++;
                Console.WriteLine(
                    $"Deleted version #{deletedCount}: Key='{version.Key}', VersionId='{version.VersionId}'");
            }

            keyMarker = response.IsTruncated == true ? response.NextKeyMarker : null;
            versionIdMarker = response.IsTruncated == true ? response.NextVersionIdMarker : null;

        } while (!string.IsNullOrWhiteSpace(keyMarker) || !string.IsNullOrWhiteSpace(versionIdMarker));

        Console.WriteLine($"Finished. Total deleted versions: {deletedCount}");
    }

    public static async Task Main()
    {
        var bucketName = "my-bucket";

        using var s3 = CreateS3Client();

        await DeleteAllObjectVersionsOneByOneAsync(
            s3,
            bucketName,
            CancellationToken.None);

        Console.WriteLine("Done.");
    }
}
