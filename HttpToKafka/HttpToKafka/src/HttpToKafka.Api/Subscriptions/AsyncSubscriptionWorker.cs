using Confluent.Kafka;
using HttpToKafka.Api.Kafka;
using HttpToKafka.Api.Subscriptions.Models;
using HttpToKafka.Api.Webhooks;

namespace HttpToKafka.Api.Subscriptions;
public sealed class AsyncSubscriptionWorker : BackgroundService, IAsyncSubscriptionWorker
{
    private readonly ISubscriptionManager _manager;
    private readonly IKafkaConsumerFactory _factory;
    private readonly IWebhookSender _sender;
    public AsyncSubscriptionWorker(ISubscriptionManager manager, IKafkaConsumerFactory factory, IWebhookSender sender)
    { _manager = manager; _factory = factory; _sender = sender; }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumers = new Dictionary<string, IConsumer<byte[], byte[]>>();
        while (!stoppingToken.IsCancellationRequested)
        {
            var now = DateTime.UtcNow;
            foreach (var sub in _manager.All())
            {
                if (sub.ExpiresUtc < now || !sub.Active) { consumers.Remove(sub.Id, out _); continue; }
                if (!consumers.ContainsKey(sub.Id))
                {
                    var c = _factory.Create(sub.GroupId);
                    c.Subscribe(sub.Topic);
                    if (sub.StartFromEnd)
                    {
                        var md = c.GetMetadata(sub.Topic, TimeSpan.FromSeconds(5));
                        foreach (var p in md.Topics.First().Partitions)
                        {
                            var tpp = new TopicPartition(sub.Topic, new Partition(p.PartitionId));
                            var end = c.QueryWatermarkOffsets(tpp, TimeSpan.FromSeconds(5)).High;
                            c.Assign(new TopicPartitionOffset(tpp, end));
                        }
                    }
                    consumers[sub.Id] = c;
                }
                var consumer = consumers[sub.Id];
                var cr = consumer.Consume(TimeSpan.FromMilliseconds(50));
                if (cr is not null && cr.Message is not null)
                {
                    var payload = new
                    {
                        SubscriptionId = sub.Id,
                        Topic = cr.Topic,
                        Partition = cr.Partition.Value,
                        Offset = cr.Offset.Value,
                        KeyBase64 = cr.Message.Key is null ? null : Convert.ToBase64String(cr.Message.Key!),
                        ValueBase64 = Convert.ToBase64String(cr.Message.Value)
                    };
                    var result = await _sender.SendAsync(sub, payload, CancellationToken.None);
                    if (result.Success) sub.Metrics.Delivered++; else sub.Metrics.Failed++;
                }
            }
            await Task.Delay(25, stoppingToken);
        }
        foreach (var c in consumers.Values) c.Close();
    }
}
