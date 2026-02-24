using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net;
using System.Text.Json;
using System.Text.Json.Serialization;
using System.Threading;
using System.Threading.Tasks;
using Amazon.Runtime;
using Amazon.S3;
using Amazon.S3.Model;
using Amazon.ECS;
using Amazon.ECS.Model;

namespace AwsConnectivityVerifier
{
    /// <summary>
    /// Verifies connectivity through an HTTP proxy to:
    /// - ECS via an explicit ECS endpoint URL (ServiceURL)
    /// - S3 (or S3-compatible) via an explicit S3 endpoint URL (ServiceURL)
    ///
    /// Tests:
    /// 1) ECS connectivity test (custom): ListClusters + optional DescribeClusters by hint
    /// 2) S3 user test: list 10 objects from bucket root
    /// 3) S3 user test: download one of those objects (first one) and read up to N bytes
    ///
    /// If all operations succeed, returns a detailed JSON status including object names and sizes.
    /// </summary>
    public sealed class AwsConnectivityThroughProxyVerifier
    {
        // -------------------- Configuration models --------------------

        public sealed class ProxyConfig
        {
            public string Host { get; init; }
            public int Port { get; init; }
            public string Username { get; init; }
            public string Password { get; init; }

            public bool Enabled => !string.IsNullOrWhiteSpace(Host) && Port > 0;
            public bool HasAuth => !string.IsNullOrWhiteSpace(Username);
        }

        public sealed class VerifierConfig
        {
            /// <summary>
            /// Explicit ECS endpoint URL (required), e.g. https://ecs.mycompany.internal
            /// </summary>
            public string EcsEndpointUrl { get; init; }

            /// <summary>
            /// Explicit S3 endpoint URL (required), e.g. https://s3.mycompany.internal
            /// </summary>
            public string S3EndpointUrl { get; init; }

            /// <summary>
            /// AWS credentials (Access Key ID + Secret Access Key).
            /// </summary>
            public AWSCredentials Credentials { get; init; }

            /// <summary>
            /// Optional proxy settings.
            /// </summary>
            public ProxyConfig Proxy { get; init; }

            /// <summary>
            /// S3 bucket name for list/get tests.
            /// </summary>
            public string S3BucketName { get; init; }

            /// <summary>
            /// Optional ECS cluster hint: if provided, we attempt DescribeClusters on a cluster whose ARN contains this string.
            /// </summary>
            public string EcsClusterHint { get; init; }

            /// <summary>
            /// Per-call timeout budget for AWS SDK calls.
            /// </summary>
            public TimeSpan PerCallTimeout { get; init; } = TimeSpan.FromSeconds(60);

            /// <summary>
            /// Max bytes to read during S3 GET test (enough to validate real download through proxy).
            /// </summary>
            public long S3GetMaxBytesToRead { get; init; } = 2L * 1024 * 1024; // 2 MiB

            /// <summary>
            /// If true, forces path-style addressing for S3, which is commonly required for S3-compatible endpoints.
            /// Example: https://endpoint/bucket/key instead of https://bucket.endpoint/key
            /// </summary>
            public bool ForceS3PathStyle { get; init; } = true;
        }

        // -------------------- Output models --------------------

        public sealed class ObjectInfo
        {
            public string Name { get; init; }
            public long SizeBytes { get; init; }
        }

        public sealed class ErrorInfo
        {
            public string Test { get; init; }
            public string Message { get; init; }
            public string ExceptionType { get; init; }
            public string ExceptionMessage { get; init; }
        }

        public sealed class TestResult
        {
            public string Name { get; init; }
            public bool Ok { get; init; }
            public long TookMillis { get; init; }
            public object Details { get; init; }
        }

        public sealed class FinalResult
        {
            public bool Ok { get; set; }

            public object Proxy { get; init; }
            public string EcsEndpointUrl { get; init; }
            public string S3EndpointUrl { get; init; }
            public string S3BucketName { get; init; }

            public TestResult EcsConnectivity { get; set; }
            public TestResult S3List10Root { get; set; }
            public TestResult S3GetOne { get; set; }

            public List<ObjectInfo> Objects { get; init; } = new();
            public List<ErrorInfo> Errors { get; init; } = new();

            public long TotalMillis { get; set; }
        }

        // -------------------- Fields --------------------

        private readonly VerifierConfig _cfg;

        public AwsConnectivityThroughProxyVerifier(VerifierConfig config)
        {
            _cfg = config ?? throw new ArgumentNullException(nameof(config));

            if (string.IsNullOrWhiteSpace(_cfg.EcsEndpointUrl))
                throw new ArgumentException("EcsEndpointUrl is required.", nameof(config));

            if (string.IsNullOrWhiteSpace(_cfg.S3EndpointUrl))
                throw new ArgumentException("S3EndpointUrl is required.", nameof(config));

            if (_cfg.Credentials == null)
                throw new ArgumentException("Credentials are required.", nameof(config));

            if (string.IsNullOrWhiteSpace(_cfg.S3BucketName))
                throw new ArgumentException("S3BucketName is required.", nameof(config));
        }

        // -------------------- Public API --------------------

        /// <summary>
        /// Runs all tests and returns the detailed JSON result.
        /// </summary>
        public async Task<string> RunAsync(CancellationToken ct = default)
        {
            var swTotal = Stopwatch.StartNew();

            var result = new FinalResult
            {
                EcsEndpointUrl = _cfg.EcsEndpointUrl,
                S3EndpointUrl = _cfg.S3EndpointUrl,
                S3BucketName = _cfg.S3BucketName,
                Proxy = new
                {
                    enabled = _cfg.Proxy?.Enabled ?? false,
                    host = _cfg.Proxy?.Host,
                    port = _cfg.Proxy?.Port,
                    auth = _cfg.Proxy?.HasAuth ?? false
                }
            };

            try
            {
                using var ecs = CreateEcsClient();
                using var s3 = CreateS3Client();

                result.EcsConnectivity = await TestEcsConnectivityAsync(ecs, result.Errors, ct);
                result.S3List10Root = await TestS3List10RootAsync(s3, result.Objects, result.Errors, ct);
                result.S3GetOne = await TestS3GetOneAsync(s3, result.Objects, result.Errors, ct);

                result.Ok = result.Errors.Count == 0;
            }
            catch (Exception ex)
            {
                result.Errors.Add(CreateError("FATAL", "Unhandled exception", ex));
                result.Ok = false;
            }
            finally
            {
                swTotal.Stop();
                result.TotalMillis = swTotal.ElapsedMilliseconds;
            }

            return JsonSerializer.Serialize(result, new JsonSerializerOptions
            {
                WriteIndented = true,
                DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull
            });
        }

        // -------------------- Tests --------------------

        /// <summary>
        /// Custom ECS connectivity test through proxy using explicit ServiceURL:
        /// - ListClusters (max 10)
        /// - Optionally DescribeClusters if cluster hint matches an ARN from the first page
        /// </summary>
        private async Task<TestResult> TestEcsConnectivityAsync(IAmazonECS ecs, List<ErrorInfo> errors, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            const string testName = "ECS_CONNECTIVITY";

            try
            {
                using var callCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                callCts.CancelAfter(_cfg.PerCallTimeout);

                var list = await ecs.ListClustersAsync(new ListClustersRequest { MaxResults = 10 }, callCts.Token);

                object describe = null;

                if (!string.IsNullOrWhiteSpace(_cfg.EcsClusterHint))
                {
                    var matchArn = (list.ClusterArns ?? new List<string>())
                        .FirstOrDefault(a => !string.IsNullOrWhiteSpace(a) &&
                                             a.IndexOf(_cfg.EcsClusterHint, StringComparison.OrdinalIgnoreCase) >= 0);

                    if (!string.IsNullOrWhiteSpace(matchArn))
                    {
                        using var callCts2 = CancellationTokenSource.CreateLinkedTokenSource(ct);
                        callCts2.CancelAfter(_cfg.PerCallTimeout);

                        var d = await ecs.DescribeClustersAsync(
                            new DescribeClustersRequest { Clusters = new List<string> { matchArn } },
                            callCts2.Token);

                        describe = new
                        {
                            requestedArn = matchArn,
                            describedCount = d.Clusters?.Count ?? 0,
                            failuresCount = d.Failures?.Count ?? 0
                        };
                    }
                    else
                    {
                        describe = new { skipped = "cluster hint not found in first page" };
                    }
                }
                else
                {
                    describe = new { skipped = "no cluster hint provided" };
                }

                sw.Stop();

                return new TestResult
                {
                    Name = testName,
                    Ok = true,
                    TookMillis = sw.ElapsedMilliseconds,
                    Details = new
                    {
                        clustersReturned = list.ClusterArns?.Count ?? 0,
                        sampleClusterArns = (list.ClusterArns ?? new List<string>()).Take(3).ToArray(),
                        describe
                    }
                };
            }
            catch (Exception ex)
            {
                errors.Add(CreateError(testName, "ECS connectivity failed", ex));

                sw.Stop();
                return new TestResult
                {
                    Name = testName,
                    Ok = false,
                    TookMillis = sw.ElapsedMilliseconds
                };
            }
        }

        /// <summary>
        /// User test: list first 10 objects from bucket root and collect their names and sizes.
        /// </summary>
        private async Task<TestResult> TestS3List10RootAsync(IAmazonS3 s3, List<ObjectInfo> objects, List<ErrorInfo> errors, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            const string testName = "S3_LIST_10_FROM_ROOT";

            try
            {
                using var callCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                callCts.CancelAfter(_cfg.PerCallTimeout);

                var resp = await s3.ListObjectsV2Async(new ListObjectsV2Request
                {
                    BucketName = _cfg.S3BucketName,
                    Prefix = "",
                    MaxKeys = 10
                }, callCts.Token);

                objects.Clear();

                foreach (var o in resp.S3Objects ?? new List<S3Object>())
                {
                    objects.Add(new ObjectInfo
                    {
                        Name = o.Key,
                        SizeBytes = o.Size
                    });
                }

                sw.Stop();

                return new TestResult
                {
                    Name = testName,
                    Ok = true,
                    TookMillis = sw.ElapsedMilliseconds,
                    Details = new
                    {
                        listedCount = objects.Count,
                        isTruncated = resp.IsTruncated
                    }
                };
            }
            catch (Exception ex)
            {
                errors.Add(CreateError(testName, "S3 list failed", ex));

                sw.Stop();
                return new TestResult
                {
                    Name = testName,
                    Ok = false,
                    TookMillis = sw.ElapsedMilliseconds
                };
            }
        }

        /// <summary>
        /// User test: download one of the listed objects (first one) to verify GET works through proxy.
        /// Reads up to S3GetMaxBytesToRead bytes to avoid large downloads.
        /// Fails if listing is empty (per requirement: must download one of them).
        /// </summary>
        private async Task<TestResult> TestS3GetOneAsync(IAmazonS3 s3, List<ObjectInfo> objects, List<ErrorInfo> errors, CancellationToken ct)
        {
            var sw = Stopwatch.StartNew();
            const string testName = "S3_GET_ONE_OBJECT";

            if (objects == null || objects.Count == 0)
            {
                errors.Add(new ErrorInfo
                {
                    Test = testName,
                    Message = "No objects returned from list(10) – cannot download any object."
                });

                return new TestResult
                {
                    Name = testName,
                    Ok = false,
                    TookMillis = 0,
                    Details = new { reason = "empty listing" }
                };
            }

            var key = objects[0].Name;

            try
            {
                using var callCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
                callCts.CancelAfter(_cfg.PerCallTimeout);

                using var resp = await s3.GetObjectAsync(new GetObjectRequest
                {
                    BucketName = _cfg.S3BucketName,
                    Key = key
                }, callCts.Token);

                long bytesRead = await DrainUpToAsync(resp.ResponseStream, _cfg.S3GetMaxBytesToRead, callCts.Token);

                sw.Stop();

                return new TestResult
                {
                    Name = testName,
                    Ok = true,
                    TookMillis = sw.ElapsedMilliseconds,
                    Details = new
                    {
                        downloadedKey = key,
                        bytesRead,
                        maxBytesToRead = _cfg.S3GetMaxBytesToRead
                    }
                };
            }
            catch (Exception ex)
            {
                errors.Add(CreateError(testName, "S3 get failed", ex));

                sw.Stop();
                return new TestResult
                {
                    Name = testName,
                    Ok = false,
                    TookMillis = sw.ElapsedMilliseconds,
                    Details = new { downloadedKey = key }
                };
            }
        }

        // -------------------- Client creation --------------------

        private IAmazonECS CreateEcsClient()
        {
            // No regions are set here; we use an explicit endpoint URL.
            // This assumes your endpoint accepts standard AWS ECS API calls at the given URL.
            var config = new AmazonECSConfig
            {
                ServiceURL = _cfg.EcsEndpointUrl,
                Timeout = _cfg.PerCallTimeout
            };

            ApplyProxy(config);
            return new AmazonECSClient(_cfg.Credentials, config);
        }

        private IAmazonS3 CreateS3Client()
        {
            // No regions are set here; we use an explicit endpoint URL.
            // ForcePathStyle is commonly required for S3-compatible endpoints.
            var config = new AmazonS3Config
            {
                ServiceURL = _cfg.S3EndpointUrl,
                ForcePathStyle = _cfg.ForceS3PathStyle,
                Timeout = _cfg.PerCallTimeout,
                UseHttp = _cfg.S3EndpointUrl.StartsWith("http://", StringComparison.OrdinalIgnoreCase)
            };

            ApplyProxy(config);
            return new AmazonS3Client(_cfg.Credentials, config);
        }

        private void ApplyProxy(ClientConfig cfg)
        {
            if (_cfg.Proxy?.Enabled != true)
                return;

            cfg.ProxyHost = _cfg.Proxy.Host;
            cfg.ProxyPort = _cfg.Proxy.Port;

            if (_cfg.Proxy.HasAuth)
            {
                cfg.ProxyCredentials = new NetworkCredential(
                    _cfg.Proxy.Username,
                    _cfg.Proxy.Password ?? string.Empty);
            }
        }

        // -------------------- Helpers --------------------

        private static async Task<long> DrainUpToAsync(Stream stream, long maxBytes, CancellationToken ct)
        {
            var buffer = new byte[8192];
            long total = 0;

            while (total < maxBytes)
            {
                int toRead = (int)Math.Min(buffer.Length, maxBytes - total);
                int read = await stream.ReadAsync(buffer.AsMemory(0, toRead), ct);
                if (read <= 0)
                    break;

                total += read;
            }

            return total;
        }

        private static ErrorInfo CreateError(string test, string message, Exception ex)
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
}