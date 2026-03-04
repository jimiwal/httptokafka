using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.Runtime;
using System.Threading;
using Newtonsoft.Json;

namespace EcsObjectCounterWithCheckpoint
{
    public class Program
    {
        static async Task Main(string[] args)
        {
            // Ustawienia ECS (S3-compatible)
            var ecsOptions = new EcsS3Options
            {
                ServiceUrl = new Uri("https://ecs.example.com"), // Twój ECS URL
                AccessKeyId = "your-access-key-id",              // Twój Access Key
                SecretAccessKey = "your-secret-access-key",      // Twój Secret Key
                ForcePathStyle = true                            // ECS wymaga ForcePathStyle
            };

            // Nazwa bucketu
            string bucketName = "your-bucket-name";

            // Tworzymy klienta ECS
            var ecsClient = new AmazonS3Client(new BasicAWSCredentials(ecsOptions.AccessKeyId, ecsOptions.SecretAccessKey), new AmazonS3Config
            {
                ServiceURL = ecsOptions.ServiceUrl.ToString(),
                ForcePathStyle = ecsOptions.ForcePathStyle
            });

            // Sprawdzenie czy istnieje zapisany checkpoint, aby wznowić
            var checkpoint = LoadCheckpoint("checkpoint.json");

            // Zliczanie obiektów i sumowanie rozmiaru, wznowienie od checkpointu
            await CountObjectsAndSizeAsync(ecsClient, bucketName, checkpoint);
        }

        // Metoda do zliczania obiektów i sumowania ich rozmiaru
        public static async Task CountObjectsAndSizeAsync(IAmazonS3 s3Client, string bucketName, Checkpoint checkpoint)
        {
            int objectCount = checkpoint?.ObjectCount ?? 0;
            long totalSize = checkpoint?.TotalSize ?? 0;
            string continuationToken = null;
            DateTime lastCheckpointTime = DateTime.UtcNow;

            try
            {
                do
                {
                    var listObjectsRequest = new ListObjectsV2Request
                    {
                        BucketName = bucketName,
                        ContinuationToken = continuationToken
                    };

                    var listObjectsResponse = await s3Client.ListObjectsV2Async(listObjectsRequest);

                    foreach (var obj in listObjectsResponse.S3Objects)
                    {
                        if (checkpoint != null && DateTime.Compare(obj.LastModified, checkpoint.LastProcessedTime) < 0)
                            continue; // Skip objects already processed based on checkpoint

                        objectCount++;
                        totalSize += obj.Size;

                        // Zapisywanie checkpoint co godzinę
                        if (DateTime.UtcNow - lastCheckpointTime >= TimeSpan.FromHours(1))
                        {
                            checkpoint.LastProcessedKey = obj.Key;
                            checkpoint.LastProcessedTime = obj.LastModified;
                            checkpoint.ObjectCount = objectCount;
                            checkpoint.TotalSize = totalSize;
                            SaveCheckpoint(checkpoint, "checkpoint.json");
                            lastCheckpointTime = DateTime.UtcNow;
                            Console.WriteLine($"Checkpoint zapisany: {checkpoint.LastProcessedKey}, {checkpoint.LastProcessedTime}, {checkpoint.ObjectCount} obiektów, {checkpoint.TotalSize / 1024 / 1024} MB");
                        }
                    }

                    continuationToken = listObjectsResponse.IsTruncated ? listObjectsResponse.NextContinuationToken : null;

                } while (continuationToken != null);

                // Po zakończeniu procesu logujemy wynik
                Console.WriteLine($"Liczba obiektów w bucketcie '{bucketName}': {objectCount}");
                Console.WriteLine($"Łączny rozmiar obiektów: {totalSize / 1024 / 1024} MB");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Wystąpił błąd: {ex.Message}");
            }
        }

        // Metoda do zapisania checkpointu do pliku JSON
        public static void SaveCheckpoint(Checkpoint checkpoint, string filePath)
        {
            var json = JsonConvert.SerializeObject(checkpoint, Formatting.Indented);
            File.WriteAllText(filePath, json);
        }

        // Metoda do załadowania checkpointu z pliku JSON
        public static Checkpoint LoadCheckpoint(string filePath)
        {
            if (File.Exists(filePath))
            {
                var json = File.ReadAllText(filePath);
                return JsonConvert.DeserializeObject<Checkpoint>(json);
            }
            return new Checkpoint(); // Jeśli brak checkpointu, zaczynamy od początku
        }
    }

    // Klasa do przechowywania checkpointu
    public class Checkpoint
    {
        public string LastProcessedKey { get; set; }
        public DateTime LastProcessedTime { get; set; } = DateTime.MinValue;
        public int ObjectCount { get; set; } = 0;  // Liczba obiektów przetworzonych
        public long TotalSize { get; set; } = 0;  // Łączny rozmiar przetworzonych obiektów
    }

    // Opcje ECS (S3 compatible)
    public class EcsS3Options
    {
        public Uri ServiceUrl { get; set; }          // np. https://ecs.example.com
        public string AccessKeyId { get; set; }      // Twój Access Key
        public string SecretAccessKey { get; set; }  // Twój Secret Key
        public bool ForcePathStyle { get; set; }     // ECS wymaga ForcePathStyle
    }
}