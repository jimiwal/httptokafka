using Confluent.Kafka;
using HttpToKafka.Api.Services.Kafka;
using HttpToKafka.Api.Services.Webhooks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace HttpToKafka.Api.Services.Subscriptions;

public class SubscriptionWorker : BackgroundService
{
    private readonly ISubscriptionManager _subs;
    private readonly IKafkaConsumerFactory _factory;
    private readonly IWebhookClient _webhook;
    private readonly ILogger<SubscriptionWorker> _logger;

    public SubscriptionWorker(ISubscriptionManager subs, IKafkaConsumerFactory factory, IWebhookClient webhook,
        ILogger<SubscriptionWorker> logger)
    {
        _subs = subs; _factory = factory; _webhook = webhook; _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = new Dictionary<string, IConsumer<byte[], byte[]>>();

        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.UtcNow;
            foreach (var s in _subs.GetAll())
            {
                if (s.Status != HttpToKafka.Api.Models.SubscriptionStatus.Active) continue;

                if (s.ExpiresUtc <= now)
                {
                    _subs.Expire(s.Id);
                    if (consumers.Remove(s.Id, out var old)) old.Close();
                    continue;
                }

                if (!consumers.ContainsKey(s.Id))
                {
                    var consumer = _factory.Create(s.GroupId, s.FromNow);
                    consumer.Subscribe(s.Topic);
                    consumers[s.Id] = consumer;
                    _ = Task.Run(() => ConsumeLoop(s.Id, consumer, stoppingToken));
                }
            }
            await Task.Delay(1000, stoppingToken);
        }

        foreach (var c in consumers.Values) c.Close();
    }

    private async Task ConsumeLoop(string sid, IConsumer<byte[], byte[]> consumer, CancellationToken ct)
    {
        try
        {
            while (!ct.IsCancellationRequested && _subs.TryGet(sid, out var dto) && dto.Status == HttpToKafka.Api.Models.SubscriptionStatus.Active)
            {
                var cr = consumer.Consume(TimeSpan.FromSeconds(1));
                if (cr == null) continue;

                var payload = new
                {
                    subscriptionId = sid,
                    topic = cr.Topic,
                    partition = cr.Partition.Value,
                    offset = cr.Offset.Value,
                    timestamp = cr.Message.Timestamp.UnixTimestampMs,
                    headers = cr.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()!)),
                    keyBase64 = cr.Message.Key != null ? Convert.ToBase64String(cr.Message.Key) : null,
                    valueBase64 = cr.Message.Value != null ? Convert.ToBase64String(cr.Message.Value) : null
                };

                var started = DateTime.UtcNow;
                var ok = await _webhook.PostAsync(dto.CallbackUrl, payload, sign: true, subscriptionId: sid, deadLetterUrl: dto.DeadLetterUrl, ct);
                var latency = DateTime.UtcNow - started;

                if (ok)
                {
                    consumer.Commit(cr);
                    _subs.IncrementDelivered(sid, latency);
                }
                else
                {
                    _subs.IncrementFailed(sid);
                    await Task.Delay(1000, ct);
                }
            }
        }
        catch (OperationCanceledException) { }
        catch (Exception ex)
        {
            _logger.LogError(ex, "Subscription {sid} failed.", sid);
        }
        finally
        {
            try { consumer.Close(); } catch { }
        }
    }
}
