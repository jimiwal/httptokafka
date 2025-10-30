using System.Text;
using Confluent.Kafka;
using Confluent.SchemaRegistry.Serdes;
using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using HttpToKafka.Core.Options;
using HttpToKafka.Kafka.Producer;
using HttpToKafka.Kafka.SchemaRegistry;
using HttpToKafka.Kafka.Serialization;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Kafka.Services;

public class KafkaProducerService : IProducerService, IDisposable
{
    private readonly ProducerPool _pool;
    private readonly SchemaRegistryClientFactory _srFactory;
    private readonly IOptions<KafkaSettings> _settings;
    private Confluent.Kafka.IProducer<string?, Avro.Generic.GenericRecord>? _avroProducer;

    public KafkaProducerService(ProducerPool pool, SchemaRegistryClientFactory srFactory, IOptions<KafkaSettings> settings)
    {
        _pool = pool;
        _srFactory = srFactory;
        _settings = settings;
    }

    public async Task<ProduceResponse> ProduceAsync(ProduceRequest req, CancellationToken ct)
    {
        if (req.Format == MessageFormat.Bytes) return await ProduceBytesAsync(req, ct);
        return await ProduceAvroAsync(req, ct);
    }

    private async Task<ProduceResponse> ProduceBytesAsync(ProduceRequest req, CancellationToken ct)
    {
        if (string.IsNullOrEmpty(req.Value)) throw new ArgumentException("Value is required for bytes format");
        var producer = _pool.GetBytesProducer();
        var headers = new Headers();
        if (req.Headers != null) foreach (var kv in req.Headers) headers.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));
        byte[]? key = null;
        if (!string.IsNullOrEmpty(req.Key))
        {
            key = req.KeyEncoding.ToLowerInvariant() == "base64" ? Convert.FromBase64String(req.Key) : Encoding.UTF8.GetBytes(req.Key);
        }
        var val = req.ValueEncoding.ToLowerInvariant() == "base64" ? Convert.FromBase64String(req.Value) : Encoding.UTF8.GetBytes(req.Value);

        var msg = new Message<byte[], byte[]> { Key = key, Value = val, Headers = headers };
        if (req.Mode == ProduceMode.Sync)
        {
            var dr = req.Partition.HasValue
                ? await producer.ProduceAsync(new TopicPartition(req.Topic, req.Partition.Value), msg, ct)
                : await producer.ProduceAsync(req.Topic, msg, ct);
            return new ProduceResponse { Topic = dr.Topic, Partition = dr.Partition, Offset = dr.Offset, Timestamp = dr.Timestamp.UtcDateTime, Mode = "sync", Status = "delivered" };
        }
        else
        {
            var corr = Guid.NewGuid().ToString("N");
            if (req.Partition.HasValue) producer.Produce(new TopicPartition(req.Topic, req.Partition.Value), msg);
            else producer.Produce(req.Topic, msg);
            return new ProduceResponse { Topic = req.Topic, Mode = "async", Status = "queued", CorrelationId = corr };
        }
    }

    private Confluent.Kafka.IProducer<string?, Avro.Generic.GenericRecord> EnsureAvroProducer()
    {
        if (_avroProducer != null) return _avroProducer;
        var cfg = _pool.BuildProducerConfig();
        var sr = _srFactory.Get();
        var serializer = new AvroSerializer<Avro.Generic.GenericRecord>(sr);
        _avroProducer = new ProducerBuilder<string?, Avro.Generic.GenericRecord>(cfg).SetValueSerializer(serializer).Build();
        return _avroProducer;
    }

    private async Task<ProduceResponse> ProduceAvroAsync(ProduceRequest req, CancellationToken ct)
    {
        if (string.IsNullOrWhiteSpace(req.AvroSchema) || req.AvroPayload is null)
            throw new ArgumentException("AvroSchema and AvroPayload are required for Avro format");
        var producer = EnsureAvroProducer();
        var record = AvroUtils.BuildGenericRecord(req.AvroSchema!, req.AvroPayload!);
        var headers = new Headers();
        if (req.Headers != null) foreach (var kv in req.Headers) headers.Add(kv.Key, Encoding.UTF8.GetBytes(kv.Value));

        var msg = new Message<string?, Avro.Generic.GenericRecord> { Key = req.Key, Value = record, Headers = headers };
        if (req.Mode == ProduceMode.Sync)
        {
            var dr = req.Partition.HasValue
                ? await producer.ProduceAsync(new TopicPartition(req.Topic, req.Partition.Value), msg, ct)
                : await producer.ProduceAsync(req.Topic, msg, ct);
            return new ProduceResponse { Topic = dr.Topic, Partition = dr.Partition, Offset = dr.Offset, Timestamp = dr.Timestamp.UtcDateTime, Mode = "sync", Status = "delivered" };
        }
        else
        {
            var corr = Guid.NewGuid().ToString("N");
            if (req.Partition.HasValue) producer.Produce(new TopicPartition(req.Topic, req.Partition.Value), msg);
            else producer.Produce(req.Topic, msg);
            return new ProduceResponse { Topic = req.Topic, Mode = "async", Status = "queued", CorrelationId = corr };
        }
    }

    public void Dispose()
    {
        try { _avroProducer?.Flush(TimeSpan.FromSeconds(3)); } catch { }
        _avroProducer?.Dispose();
    }
}
