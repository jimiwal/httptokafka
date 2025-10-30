using System.Text;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using HttpToKafka.Kafka.Consumers;
using HttpToKafka.Kafka.SchemaRegistry;
using HttpToKafka.Kafka.Serialization;

namespace HttpToKafka.Kafka.Services;

public class KafkaConsumerService : IConsumerService
{
    private readonly KafkaConsumerFactory _factory;
    private readonly SchemaRegistryClientFactory _sr;

    public KafkaConsumerService(KafkaConsumerFactory factory, SchemaRegistryClientFactory sr)
    {
        _factory = factory;
        _sr = sr;
    }

    public Task<SubscribeSyncResponse> SubscribeOnceAsync(SubscribeSyncRequest request, CancellationToken ct)
    {
        return request.Format == MessageFormat.Bytes
            ? ReceiveBytesAsync(request, ct)
            : ReceiveAvroAsync(request, ct);
    }

    private Task<SubscribeSyncResponse> ReceiveBytesAsync(SubscribeSyncRequest request, CancellationToken ct)
    {
        using var consumer = _factory.CreateBytesConsumer(request.GroupId, request.AutoCommit);
        consumer.Subscribe(request.Topic);
        if (request.StartFromNow) ConsumerStartPositionHelper.AssignToEnd(consumer, request.Topic);

        var cr = consumer.Consume(TimeSpan.FromMilliseconds(request.TimeoutMs));
        if (cr is null) return Task.FromResult(new SubscribeSyncResponse { Outcome = "timeout", Topic = request.Topic });
        if (!request.AutoCommit) consumer.Commit(cr);

        var resp = new SubscribeSyncResponse
        {
            Outcome = "message",
            Topic = cr.Topic,
            Partition = cr.Partition.Value,
            Offset = cr.Offset.Value,
            Timestamp = cr.Message.Timestamp.UtcDateTime,
            Headers = cr.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes() ?? Array.Empty<byte>())),
            Value = Convert.ToBase64String(cr.Message.Value ?? Array.Empty<byte>())
        };
        return Task.FromResult(resp);
    }

    private Task<SubscribeSyncResponse> ReceiveAvroAsync(SubscribeSyncRequest request, CancellationToken ct)
    {
        var sr = _sr.Get();
        var valueDeserializer = new AvroDeserializer<Avro.Generic.GenericRecord>(sr);
        var cfg = _factory.BuildConsumerConfig(request.GroupId, request.AutoCommit);
        using var consumer = new ConsumerBuilder<string?, Avro.Generic.GenericRecord>(cfg)
            .SetValueDeserializer(valueDeserializer.AsSyncOverAsync())
            .Build();

        consumer.Subscribe(request.Topic);
        if (request.StartFromNow) ConsumerStartPositionHelper.AssignToEnd(consumer, request.Topic);

        var cr = consumer.Consume(TimeSpan.FromMilliseconds(request.TimeoutMs));
        if (cr is null) return Task.FromResult(new SubscribeSyncResponse { Outcome = "timeout", Topic = request.Topic });
        if (!request.AutoCommit) consumer.Commit(cr);

        var dict = AvroUtils.GenericRecordToDictionary(cr.Message.Value);
        var resp = new SubscribeSyncResponse
        {
            Outcome = "message",
            Topic = cr.Topic,
            Partition = cr.Partition.Value,
            Offset = cr.Offset.Value,
            Timestamp = cr.Message.Timestamp.UtcDateTime,
            Headers = cr.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes() ?? Array.Empty<byte>())),
            Value = dict
        };
        return Task.FromResult(resp);
    }
}
