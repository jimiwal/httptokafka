using Confluent.Kafka;
using KafkaOverHttp.Models;
using Microsoft.Extensions.Options;

namespace KafkaOverHttp.Services;

public sealed class KafkaConsumerService
{
    private readonly ConsumerConfig _cfg;

    public KafkaConsumerService(IOptions<ConsumerConfig> cfg)
    {
        _cfg = cfg.Value;
    }

    public async Task<MessageEnvelope?> ConsumeOneAsync(ConsumeOneRequest req, CancellationToken ct)
    {
        var config = new ConsumerConfig(_cfg)
        {
            EnableAutoCommit = false,
            GroupId = $"{_cfg.GroupId}-pull-{Guid.NewGuid():N}"
        };

        using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
        consumer.Subscribe(req.Topic);

        try
        {
            if (req.Partition.HasValue)
            {
                consumer.Assign(new TopicPartitionOffset(
                    req.Topic,
                    new Partition(req.Partition.Value),
                    req.Offset.HasValue ? new Offset(req.Offset.Value) : Offset.Beginning));
            }

            var deadline = DateTime.UtcNow.AddMilliseconds(req.MaxWaitMs);
            while (DateTime.UtcNow < deadline)
            {
                ct.ThrowIfCancellationRequested();
                var remaining = deadline - DateTime.UtcNow;
                var cr = consumer.Consume(TimeSpan.FromMilliseconds(Math.Max(50, remaining.TotalMilliseconds)));
                if (cr == null) continue;

                var env = new MessageEnvelope
                {
                    Topic = cr.Topic,
                    KeyBase64 = cr.Message.Key is null ? null : Convert.ToBase64String(cr.Message.Key),
                    ValueBase64 = req.Serialization == SerializationKind.ByteArray && cr.Message.Value != null
                        ? Convert.ToBase64String(cr.Message.Value)
                        : null,
                    AvroValue = req.Serialization == SerializationKind.Avro ? null : null,
                    Partition = cr.Partition.Value,
                    Offset = cr.Offset.Value,
                    TimestampUtc = cr.Message.Timestamp.UtcDateTime,
                    Headers = cr.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()))
                };

                return env;
            }
            return null;
        }
        finally
        {
            try { consumer.Close(); } catch { }
        }
    }
}
