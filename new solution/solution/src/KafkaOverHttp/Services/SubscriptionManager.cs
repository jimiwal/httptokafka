using Confluent.Kafka;
using KafkaOverHttp.Models;
using Microsoft.Extensions.Options;
using Polly;
using Polly.Retry;
using System.Collections.Concurrent;

namespace KafkaOverHttp.Services;

public sealed class SubscriptionManager : BackgroundService
{
    private sealed record Subscription(
        string Id,
        string Topic,
        string Target,
        SerializationKind Kind,
        DateTime ExpireUtc,
        int PollIntervalMs);

    private readonly ConsumerConfig _baseConsumerCfg;
    private readonly IHttpClientFactory _httpFactory;
    private readonly ILogger<SubscriptionManager> _logger;
    private readonly ConcurrentDictionary<string, Subscription> _subs = new();
    private readonly int _httpTimeoutSec;
    private readonly int _maxParallel;
    private readonly AsyncRetryPolicy _retryPolicy;

    public SubscriptionManager(
        IOptions<ConsumerConfig> consumerCfg,
        IHttpClientFactory httpFactory,
        IOptionsMonitor<PushOptions> pushOptions,
        ILogger<SubscriptionManager> logger)
    {
        _baseConsumerCfg = consumerCfg.Value;
        _httpFactory = httpFactory;
        _logger = logger;

        var opts = pushOptions.CurrentValue;
        _httpTimeoutSec = opts.HttpTimeoutSeconds;
        _maxParallel = opts.MaxParallelDeliveries;
        _retryPolicy = Policy
            .Handle<Exception>()
            .WaitAndRetryAsync(pushOptions.CurrentValue.RetryPolicy.RetryCount,
                attempt => TimeSpan.FromMilliseconds(pushOptions.CurrentValue.RetryPolicy.BaseDelayMs * Math.Pow(2, attempt - 1)));
    }

    public bool Register(SubscribePushRequest req)
    {
        var sub = new Subscription(
            req.SubscriptionId,
            req.Topic,
            req.TargetEndpoint,
            req.Serialization,
            DateTime.UtcNow.AddMilliseconds(req.DurationMs),
            req.PollIntervalMs);

        return _subs.TryAdd(req.SubscriptionId, sub);
    }

    public bool Cancel(string id) => _subs.TryRemove(id, out _);

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var cfg = new ConsumerConfig(_baseConsumerCfg)
        {
            GroupId = $"{_baseConsumerCfg.GroupId}-push-{Guid.NewGuid():N}",
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Earliest
        };

        using var consumer = new ConsumerBuilder<byte[], byte[]>(cfg).Build();

        var http = _httpFactory.CreateClient(nameof(SubscriptionManager));
        http.Timeout = TimeSpan.FromSeconds(_httpTimeoutSec);

        string[]? currentlySubscribed = null;

        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                // Zbierz aktualne topiki z aktywnych subskrypcji
                var topics = _subs.Values.Select(s => s.Topic).Distinct().OrderBy(t => t).ToArray();

                // Jeœli lista tematów siê zmieni³a – prze³¹cz subskrypcjê
                bool changed = currentlySubscribed is null
                               || topics.Length != currentlySubscribed.Length
                               || !topics.SequenceEqual(currentlySubscribed);

                if (topics.Length == 0)
                {
                    if (currentlySubscribed is not null && currentlySubscribed.Length > 0)
                    {
                        consumer.Unsubscribe();
                        currentlySubscribed = Array.Empty<string>();
                    }

                    // Brak subów – krótki odpoczynek i lecimy dalej
                    CleanupExpired();
                    await Task.Delay(200, stoppingToken);
                    continue;
                }
                else if (changed)
                {
                    consumer.Subscribe(topics);
                    currentlySubscribed = topics;
                }

                var cr = consumer.Consume(TimeSpan.FromMilliseconds(200));
                if (cr is null)
                {
                    CleanupExpired();
                    await Task.Delay(50, stoppingToken);
                    continue;
                }

                var matching = _subs.Values.Where(s => s.Topic == cr.Topic).ToArray();
                if (matching.Length == 0) continue;

                var payload = new MessageEnvelope
                {
                    Topic = cr.Topic,
                    KeyBase64 = cr.Message.Key is null ? null : Convert.ToBase64String(cr.Message.Key),
                    ValueBase64 = cr.Message.Value is null ? null : Convert.ToBase64String(cr.Message.Value),
                    AvroValue = null,
                    Partition = cr.Partition.Value,
                    Offset = cr.Offset.Value,
                    TimestampUtc = cr.Message.Timestamp.UtcDateTime,
                    Headers = cr.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()))
                };

                // Wyœlij do wszystkich subskrybentów tego topicu (z retry)
                var tasks = matching.Select(sub =>
                    _retryPolicy.ExecuteAsync(ct => PostToTargetAsync(http, sub.Target, payload, ct), stoppingToken));
                await Task.WhenAll(tasks);
            }
            catch (OperationCanceledException) { /* stop */ }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error in push loop");
                await Task.Delay(500, stoppingToken);
            }

            CleanupExpired();
        }
    }


    private static MessageEnvelope BuildEnvelope(ConsumeResult<byte[], byte[]> cr)
    {
        return new MessageEnvelope
        {
            Topic = cr.Topic,
            KeyBase64 = cr.Message.Key is null ? null : Convert.ToBase64String(cr.Message.Key),
            ValueBase64 = cr.Message.Value is null ? null : Convert.ToBase64String(cr.Message.Value),
            AvroValue = null,
            Partition = cr.Partition.Value,
            Offset = cr.Offset.Value,
            TimestampUtc = cr.Message.Timestamp.UtcDateTime,
            Headers = cr.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()))
        };
    }

    private async Task PostToTargetAsync(HttpClient http, string url, MessageEnvelope payload, CancellationToken ct)
    {
        using var content = new StringContent(System.Text.Json.JsonSerializer.Serialize(payload), System.Text.Encoding.UTF8, "application/json");
        using var resp = await http.PostAsync(url, content, ct);
        resp.EnsureSuccessStatusCode();
    }

    private void CleanupExpired()
    {
        var now = DateTime.UtcNow;
        foreach (var kv in _subs.ToArray())
        {
            if (kv.Value.ExpireUtc < now)
                _subs.TryRemove(kv.Key, out _);
        }
    }
}

public sealed class PushOptions
{
    public int HttpTimeoutSeconds { get; set; } = 15;
    public int MaxParallelDeliveries { get; set; } = 8;
    public RetryOpts RetryPolicy { get; set; } = new();
    public sealed class RetryOpts
    {
        public int RetryCount { get; set; } = 3;
        public int BaseDelayMs { get; set; } = 500;
    }
}
