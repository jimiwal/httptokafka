using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Amazon;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.ECS;
using Amazon.ECS.Model;

namespace AwsConnectivityVerifier
{
    /// <summary>
    /// Verifies AWS connectivity through proxy using ECS and S3 tests.
    /// Returns detailed JSON status with object names and sizes.
    /// </summary>
    public class AwsConnectivityThroughProxyVerifier
    {
        public class ProxyConfig
        {
            public string Host { get; set; }
            public int Port { get; set; }
            public string Username { get; set; }
            public string Password { get; set; }

            public bool Enabled =>
                !string.IsNullOrWhiteSpace(Host) && Port > 0;
        }

        public class VerifierConfig
        {
            public RegionEndpoint Region { get; set; }
            public AWSCredentials Credentials { get; set; }
            public ProxyConfig Proxy { get; set; }
            public string S3BucketName { get; set; }
            public string ECSClusterHint { get; set; }
        }

        public class ObjectInfo
        {
            public string Name { get; set; }
            public long SizeBytes { get; set; }
        }

        public class TestResult
        {
            public string Name { get; set; }
            public bool Ok { get; set; }
            public long TookMillis { get; set; }
            public object Details { get; set; }
        }

        public class ErrorInfo
        {
            public string Test { get; set; }
            public string Message { get; set; }
            public string ExceptionType { get; set; }
            public string ExceptionMessage { get; set; }
        }

        public class FinalResult
        {
            public bool Ok { get; set; }
            public string Region { get; set; }
            public object Proxy { get; set; }

            public TestResult ECSConnectivity { get; set; }
            public TestResult S3List { get; set; }
            public TestResult S3Get { get; set; }

            public List<ObjectInfo> Objects { get; set; } = new();
            public List<ErrorInfo> Errors { get; set; } = new();

            public long TotalMillis { get; set; }
        }

        private readonly VerifierConfig _config;

        public AwsConnectivityThroughProxyVerifier(VerifierConfig config)
        {
            _config = config ?? throw new ArgumentNullException(nameof(config));
        }

        /// <summary>
        /// Runs all connectivity tests and returns JSON string result.
        /// </summary>
        public async Task<string> RunAsync()
        {
            var swTotal = Stopwatch.StartNew();

            var result = new FinalResult
            {
                Region = _config.Region.SystemName,
                Proxy = new
                {
                    enabled = _config.Proxy?.Enabled ?? false,
                    host = _config.Proxy?.Host,
                    port = _config.Proxy?.Port,
                    auth = !string.IsNullOrWhiteSpace(_config.Proxy?.Username)
                }
            };

            try
            {
                var ecsClient = CreateECSClient();
                var s3Client = CreateS3Client();

                result.ECSConnectivity = await TestECSConnectivity(ecsClient, result.Errors);
                result.S3List = await TestS3List(s3Client, result.Objects, result.Errors);
                result.S3Get = await TestS3Get(s3Client, result.Objects, result.Errors);

                result.Ok = result.Errors.Count == 0;
            }
            catch (Exception ex)
            {
                result.Errors.Add(CreateError("FATAL", "Unhandled exception", ex));
                result.Ok = false;
            }

            swTotal.Stop();
            result.TotalMillis = swTotal.ElapsedMilliseconds;

            return JsonSerializer.Serialize(result, new JsonSerializerOptions
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            });
        }

        /// <summary>
        /// Test ECS connectivity by listing clusters and optionally describing one.
        /// </summary>
        private async Task<TestResult> TestECSConnectivity(
            IAmazonECS ecs,
            List<ErrorInfo> errors)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                var listResponse = await ecs.ListClustersAsync(new ListClustersRequest
                {
                    MaxResults = 10
                });

                object describeResult = null;

                if (!string.IsNullOrWhiteSpace(_config.ECSClusterHint))
                {
                    var match = listResponse.ClusterArns
                        .FirstOrDefault(x => x.Contains(_config.ECSClusterHint));

                    if (match != null)
                    {
                        var describe = await ecs.DescribeClustersAsync(
                            new DescribeClustersRequest
                            {
                                Clusters = new List<string> { match }
                            });

                        describeResult = new
                        {
                            described = describe.Clusters.Count,
                            failures = describe.Failures.Count
                        };
                    }
                }

                sw.Stop();

                return new TestResult
                {
                    Name = "ECS_CONNECTIVITY",
                    Ok = true,
                    TookMillis = sw.ElapsedMilliseconds,
                    Details = new
                    {
                        clustersReturned = listResponse.ClusterArns.Count,
                        sample = listResponse.ClusterArns.Take(3),
                        describe = describeResult
                    }
                };
            }
            catch (Exception ex)
            {
                errors.Add(CreateError("ECS_CONNECTIVITY", "ECS connectivity failed", ex));

                sw.Stop();

                return new TestResult
                {
                    Name = "ECS_CONNECTIVITY",
                    Ok = false,
                    TookMillis = sw.ElapsedMilliseconds
                };
            }
        }

        /// <summary>
        /// Lists first 10 objects from bucket root.
        /// </summary>
        private async Task<TestResult> TestS3List(
            IAmazonS3 s3,
            List<ObjectInfo> objects,
            List<ErrorInfo> errors)
        {
            var sw = Stopwatch.StartNew();

            try
            {
                var response = await s3.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = _config.S3BucketName,
                    MaxKeys = 10
                });

                foreach (var obj in response.S3Objects)
                {
                    objects.Add(new ObjectInfo
                    {
                        Name = obj.Key,
                        SizeBytes = obj.Size
                    });
                }

                sw.Stop();

                return new TestResult
                {
                    Name = "S3_LIST_ROOT",
                    Ok = true,
                    TookMillis = sw.ElapsedMilliseconds,
                    Details = new { count = objects.Count }
                };
            }
            catch (Exception ex)
            {
                errors.Add(CreateError("S3_LIST_ROOT", "Failed to list objects", ex));

                sw.Stop();

                return new TestResult
                {
                    Name = "S3_LIST_ROOT",
                    Ok = false,
                    TookMillis = sw.ElapsedMilliseconds
                };
            }
        }

        /// <summary>
        /// Downloads one object to verify full connectivity.
        /// </summary>
        private async Task<TestResult> TestS3Get(
            IAmazonS3 s3,
            List<ObjectInfo> objects,
            List<ErrorInfo> errors)
        {
            var sw = Stopwatch.StartNew();

            if (objects.Count == 0)
            {
                errors.Add(new ErrorInfo
                {
                    Test = "S3_GET",
                    Message = "No objects available to download"
                });

                return new TestResult
                {
                    Name = "S3_GET",
                    Ok = false
                };
            }

            var key = objects[0].Name;

            try
            {
                using var response = await s3.GetObjectAsync(_config.S3BucketName, key);

                using var stream = response.ResponseStream;

                byte[] buffer = new byte[8192];
                long totalRead = 0;
                int read;

                while ((read = await stream.ReadAsync(buffer, 0, buffer.Length)) > 0)
                {
                    totalRead += read;

                    if (totalRead > 2 * 1024 * 1024)
                        break;
                }

                sw.Stop();

                return new TestResult
                {
                    Name = "S3_GET",
                    Ok = true,
                    TookMillis = sw.ElapsedMilliseconds,
                    Details = new
                    {
                        key,
                        bytesRead = totalRead
                    }
                };
            }
            catch (Exception ex)
            {
                errors.Add(CreateError("S3_GET", "Failed to download object", ex));

                sw.Stop();

                return new TestResult
                {
                    Name = "S3_GET",
                    Ok = false,
                    TookMillis = sw.ElapsedMilliseconds
                };
            }
        }

        private IAmazonS3 CreateS3Client()
        {
            return new AmazonS3Client(
                _config.Credentials,
                CreateClientConfig<AmazonS3Config>());
        }

        private IAmazonECS CreateECSClient()
        {
            return new AmazonECSClient(
                _config.Credentials,
                CreateClientConfig<AmazonECSConfig>());
        }

        private T CreateClientConfig<T>() where T : ClientConfig, new()
        {
            var config = new T
            {
                RegionEndpoint = _config.Region,
                Timeout = TimeSpan.FromSeconds(60)
            };

            if (_config.Proxy?.Enabled == true)
            {
                config.ProxyHost = _config.Proxy.Host;
                config.ProxyPort = _config.Proxy.Port;

                if (!string.IsNullOrWhiteSpace(_config.Proxy.Username))
                {
                    config.ProxyCredentials = new NetworkCredential(
                        _config.Proxy.Username,
                        _config.Proxy.Password);
                }
            }

            return config;
        }

        private ErrorInfo CreateError(string test, string message, Exception ex)
        {
            return new ErrorInfo
            {
                Test = test,
                Message = message,
                ExceptionType = ex?.GetType().FullName,
                ExceptionMessage = ex?.Message
            };
        }
    }


    class Test
    {
        void Start(){
                var verifier = new AwsConnectivityThroughProxyVerifier(
                new AwsConnectivityThroughProxyVerifier.VerifierConfig
                {
                    Region = RegionEndpoint.EUCentral1,
                    Credentials = new EnvironmentVariablesAWSCredentials(),
                    S3BucketName = "your-bucket",
                    ECSClusterHint = "your-cluster",

                    Proxy = new AwsConnectivityThroughProxyVerifier.ProxyConfig
                    {
                        Host = "proxy.company.local",
                        Port = 3128,
                        Username = "user",
                        Password = "pass"
                    }
                });

            string json = await verifier.RunAsync();

            Console.WriteLine(json);
        }
    }
}