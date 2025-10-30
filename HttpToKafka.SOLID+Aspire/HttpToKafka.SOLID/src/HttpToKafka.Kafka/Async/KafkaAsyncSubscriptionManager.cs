using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using HttpToKafka.Kafka.Consumers;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using HttpToKafka.Kafka.SchemaRegistry;
using HttpToKafka.Kafka.Serialization;

namespace HttpToKafka.Kafka.Async;

internal class SubscriptionState
{
    public required string Id { get; init; }
    public required SubscribeAsyncRequest Request { get; init; }
    public DateTimeOffset StartedAt { get; init; } = DateTimeOffset.UtcNow;
    public DateTimeOffset? ExpiresAt { get; set; }
    public int DeliveredCount { get; set; }
    public int FailedCount { get; set; }
    public DateTimeOffset? LastMessageAt { get; set; }
    public string Status { get; set; } = "running";
}

public class HttpCallbackSender : ICallbackSender
{
    private readonly IHttpClientFactory _httpClientFactory;
    private readonly ICallbackSigner _signer;
    public HttpCallbackSender(IHttpClientFactory httpClientFactory, ICallbackSigner signer)
    {
        _httpClientFactory = httpClientFactory;
        _signer = signer;
    }

    public async Task<bool> SendAsync(string url, string body, IDictionary<string, string> headers, CancellationToken ct)
    {
        var http = _httpClientFactory.CreateClient("callbacks");
        var req = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        foreach (var kv in headers) req.Headers.TryAddWithoutValidation(kv.Key, kv.Value);
        var resp = await http.SendAsync(req, ct);
        return resp.IsSuccessStatusCode;
    }

    public async Task<bool> SendDeadLetterAsync(string url, string body, IDictionary<string, string>? headers, CancellationToken ct)
    {
        var http = _httpClientFactory.CreateClient("callbacks");
        var req = new HttpRequestMessage(HttpMethod.Post, url)
        {
            Content = new StringContent(body, Encoding.UTF8, "application/json")
        };
        if (headers != null) foreach (var kv in headers) req.Headers.TryAddWithoutValidation(kv.Key, kv.Value);
        var resp = await http.SendAsync(req, ct);
        return resp.IsSuccessStatusCode;
    }
}

public class KafkaAsyncSubscriptionManager : IAsyncSubscriptionManager
{
    private readonly KafkaConsumerFactory _factory;
    private readonly SchemaRegistryClientFactory _sr;
    private readonly ICallbackSender _sender;
    private readonly ICallbackSigner _signer;
    private readonly IBackoffStrategy _backoff;
    private readonly IClock _clock;
    private readonly ConcurrentDictionary<string, CancellationTokenSource> _cts = new();
    private readonly ConcurrentDictionary<string, SubscriptionState> _states = new();

    public KafkaAsyncSubscriptionManager(KafkaConsumerFactory factory, SchemaRegistryClientFactory sr, ICallbackSender sender, ICallbackSigner signer, IBackoffStrategy backoff, IClock clock)
    {
        _factory = factory;
        _sr = sr;
        _sender = sender;
        _signer = signer;
        _backoff = backoff;
        _clock = clock;
    }

    public Task<SubscribeAsyncResponse> StartAsync(SubscribeAsyncRequest request, CancellationToken ct)
    {
        var id = Guid.NewGuid().ToString("N");
        var cts = new CancellationTokenSource();
        if (request.MaxSeconds is null && request.MaxMessages is null) cts.CancelAfter(TimeSpan.FromHours(1));
        else if (request.MaxSeconds is not null) cts.CancelAfter(TimeSpan.FromSeconds(request.MaxSeconds.Value));

        _cts[id] = cts;
        _states[id] = new SubscriptionState { Id = id, Request = request, ExpiresAt = _clock.UtcNow().AddSeconds(request.MaxSeconds ?? 3600) };

        _ = Task.Run(() => RunAsync(id, request, cts.Token));

        return Task.FromResult(new SubscribeAsyncResponse
        {
            SubscriptionId = id,
            Status = "running",
            Topic = request.Topic,
            StartFromNow = request.StartFromNow,
            CallbackUrl = request.CallbackUrl,
            ExpiresAt = _clock.UtcNow().AddSeconds(request.MaxSeconds ?? 3600)
        });
    }

    public bool Cancel(string subscriptionId)
    {
        if (_cts.TryRemove(subscriptionId, out var c))
        {
            c.Cancel(); c.Dispose();
            if (_states.TryGetValue(subscriptionId, out var st)) st.Status = "cancelled";
            return true;
        }
        return false;
    }

    public SubscriptionInfo? Get(string id) => _states.TryGetValue(id, out var st) ? ToInfo(st) : null;
    public IEnumerable<SubscriptionInfo> List() => _states.Values.Select(ToInfo);

    private SubscriptionInfo ToInfo(SubscriptionState st) => new SubscriptionInfo
    {
        SubscriptionId = st.Id,
        Topic = st.Request.Topic,
        GroupId = st.Request.GroupId,
        Format = st.Request.Format.ToString(),
        AutoCommit = st.Request.AutoCommit,
        StartFromNow = st.Request.StartFromNow,
        CallbackUrl = st.Request.CallbackUrl,
        StartedAt = st.StartedAt,
        ExpiresAt = st.ExpiresAt,
        LastMessageAt = st.LastMessageAt,
        DeliveredCount = st.DeliveredCount,
        FailedCount = st.FailedCount,
        Status = st.Status
    };

    private async Task RunAsync(string id, SubscribeAsyncRequest req, CancellationToken ct)
    {
        try
        {
            if (req.Format == MessageFormat.Bytes)
            {
                using var consumer = _factory.CreateBytesConsumer(req.GroupId, req.AutoCommit);
                consumer.Subscribe(req.Topic);
                if (req.StartFromNow) ConsumerStartPositionHelper.AssignToEnd(consumer, req.Topic);
                await ConsumeLoopBytes(id, consumer, req, ct);
            }
            else
            {
                var sr = _sr.Get();
                var valueDeserializer = new AvroDeserializer<Avro.Generic.GenericRecord>(sr);
                var cfg = _factory.BuildConsumerConfig(req.GroupId, req.AutoCommit);
                using var consumer = new ConsumerBuilder<string?, Avro.Generic.GenericRecord>(cfg)
                    .SetValueDeserializer(valueDeserializer.AsSyncOverAsync())
                    .Build();
                consumer.Subscribe(req.Topic);
                if (req.StartFromNow) ConsumerStartPositionHelper.AssignToEnd(consumer, req.Topic);
                await ConsumeLoopAvro(id, consumer, req, ct);
            }
        }
        catch
        {
            if (_states.TryGetValue(id, out var st)) st.Status = "error";
        }
        finally
        {
            Cancel(id);
            if (_states.TryGetValue(id, out var st) && st.Status == "running") st.Status = "completed";
        }
    }

    private async Task ConsumeLoopBytes(string id, IConsumer<byte[], byte[]> consumer, SubscribeAsyncRequest req, CancellationToken ct)
    {
        var delivered = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var cr = consumer.Consume(ct);
                var payloadObj = new
                {
                    topic = cr.Topic,
                    partition = cr.Partition.Value,
                    offset = cr.Offset.Value,
                    timestamp = cr.Message.Timestamp.UtcDateTime,
                    headers = cr.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes() ?? Array.Empty<byte>())),
                    valueBase64 = Convert.ToBase64String(cr.Message.Value ?? Array.Empty<byte>())
                };
                var json = JsonSerializer.Serialize(payloadObj);
                var headers = new Dictionary<string,string>(req.CallbackHeaders ?? new Dictionary<string,string>());
                foreach (var kv in _signer.BuildHeaders(json, req)) headers[kv.Key] = kv.Value;

                var ok = await SendWithRetry(json, headers, req, ct);
                if (ok)
                {
                    consumer.Commit(cr);
                    delivered++;
                    if (_states.TryGetValue(id, out var st)) { st.DeliveredCount++; st.LastMessageAt = DateTimeOffset.UtcNow; }
                    if (req.MaxMessages.HasValue && delivered >= req.MaxMessages.Value) break;
                }
                else
                {
                    if (_states.TryGetValue(id, out var st)) st.FailedCount++;
                }
            }
            catch (OperationCanceledException) { break; }
            catch { await Task.Delay(500, ct); }
        }
    }

    private async Task ConsumeLoopAvro(string id, IConsumer<string?, Avro.Generic.GenericRecord> consumer, SubscribeAsyncRequest req, CancellationToken ct)
    {
        var delivered = 0;
        while (!ct.IsCancellationRequested)
        {
            try
            {
                var cr = consumer.Consume(ct);
                var dict = AvroUtils.GenericRecordToDictionary(cr.Message.Value);
                var payloadObj = new
                {
                    topic = cr.Topic,
                    partition = cr.Partition.Value,
                    offset = cr.Offset.Value,
                    timestamp = cr.Message.Timestamp.UtcDateTime,
                    headers = cr.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes() ?? Array.Empty<byte>())),
                    avro = dict
                };
                var json = JsonSerializer.Serialize(payloadObj);
                var headers = new Dictionary<string,string>(req.CallbackHeaders ?? new Dictionary<string,string>());
                foreach (var kv in _signer.BuildHeaders(json, req)) headers[kv.Key] = kv.Value;

                var ok = await SendWithRetry(json, headers, req, ct);
                if (ok)
                {
                    consumer.Commit(cr);
                    delivered++;
                    if (_states.TryGetValue(id, out var st)) { st.DeliveredCount++; st.LastMessageAt = DateTimeOffset.UtcNow; }
                    if (req.MaxMessages.HasValue && delivered >= req.MaxMessages.Value) break;
                }
                else
                {
                    if (_states.TryGetValue(id, out var st)) st.FailedCount++;
                }
            }
            catch (OperationCanceledException) { break; }
            catch { await Task.Delay(500, ct); }
        }
    }

    private async Task<bool> SendWithRetry(string json, IDictionary<string,string> headers, SubscribeAsyncRequest req, CancellationToken ct)
    {
        var attempt = 0;
        while (!ct.IsCancellationRequested)
        {
            attempt++;
            if (await _sender.SendAsync(req.CallbackUrl, json, headers, ct)) return true;
            if (attempt > req.MaxCallbackRetries) break;
            var delay = _backoff.NextDelayMs(attempt);
            await Task.Delay(delay, ct);
        }
        if (!string.IsNullOrEmpty(req.DeadLetterUrl))
        {
            try { await _sender.SendDeadLetterAsync(req.DeadLetterUrl, json, req.DeadLetterHeaders, ct); } catch { }
        }
        return false;
    }
}
