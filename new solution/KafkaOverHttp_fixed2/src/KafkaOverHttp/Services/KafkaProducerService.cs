using Confluent.Kafka;
using KafkaOverHttp.Models;
using Microsoft.Extensions.Options;

namespace KafkaOverHttp.Services;

public sealed class KafkaProducerService
{
    private readonly ProducerConfig _cfg;
    private readonly AvroSerializerFactory _avroFactory;

    public KafkaProducerService(IOptions<ProducerConfig> cfg, AvroSerializerFactory avroFactory)
    {
        _cfg = cfg.Value;
        _avroFactory = avroFactory;
    }

    public async Task<DeliveryResult<byte[], byte[]>> ProduceByteArrayAsync(ProduceRequest req, bool awaitDelivery, CancellationToken ct)
    {
        var producer = new ProducerBuilder<byte[], byte[]>(_cfg).Build();
        try
        {
            byte[]? key = req.Key != null ? TryParseBase64(req.Key) : null;
            var val = TryParseBase64(req.Value);

            var msg = new Message<byte[], byte[]>
            {
                Key = key,
                Value = val,
                Headers = BuildHeaders(req.Headers)
            };

            if (req.Partition.HasValue)
            {
                var topicPartition = new TopicPartition(req.Topic, new Partition(req.Partition.Value));
                var dr = await producer.ProduceAsync(topicPartition, msg, ct);
                if (awaitDelivery) producer.Flush(TimeSpan.FromSeconds(10));
                return dr;
            }
            else
            {
                var tcs = new TaskCompletionSource<DeliveryResult<byte[], byte[]>>();
                producer.Produce(req.Topic, msg, report => tcs.TrySetResult(report));
                var dr = await tcs.Task.WaitAsync(ct);
                if (awaitDelivery) producer.Flush(TimeSpan.FromSeconds(10));
                return dr;
            }
        }
        finally
        {
            producer.Dispose();
        }
    }

    public async Task<DeliveryResult<byte[], object>> ProduceAvroAsync(ProduceRequest req, bool awaitDelivery, CancellationToken ct)
    {
        var serializer = await _avroFactory.CreateValueSerializerAsync();
        var producer = new ProducerBuilder<byte[], object>(_cfg).SetValueSerializer(serializer).Build();

        try
        {
            byte[]? key = req.Key != null ? Convert.FromBase64String(req.Key) : null;
            object avroObj = System.Text.Json.JsonSerializer.Deserialize<object>(req.Value)!;

            var msg = new Message<byte[], object>
            {
                Key = key,
                Value = avroObj,
                Headers = BuildHeaders(req.Headers)
            };

            if (req.Partition.HasValue)
            {
                var topicPartition = new TopicPartition(req.Topic, new Partition(req.Partition.Value));
                var dr = await producer.ProduceAsync(topicPartition, msg, ct);
                if (awaitDelivery) producer.Flush(TimeSpan.FromSeconds(10));
                return dr;
            }
            else
            {
                var tcs = new TaskCompletionSource<DeliveryResult<byte[], object>>();
                producer.Produce(req.Topic, msg, report => tcs.TrySetResult(report));
                var dr = await tcs.Task.WaitAsync(ct);
                if (awaitDelivery) producer.Flush(TimeSpan.FromSeconds(10));
                return dr;
            }
        }
        finally
        {
            producer.Dispose();
        }
    }

    private static Headers? BuildHeaders(Dictionary<string,string>? dict)
    {
        if (dict is null || dict.Count == 0) return null;
        var headers = new Headers();
        foreach (var kv in dict)
            headers.Add(kv.Key, System.Text.Encoding.UTF8.GetBytes(kv.Value));
        return headers;
    }

    private static byte[] TryParseBase64(string input)
    {
        try
        {
            // jeœli to poprawny Base64 – dekoduj
            return Convert.FromBase64String(input);
        }
        catch (FormatException)
        {
            // jeœli nie – zakoduj jako UTF8
            return System.Text.Encoding.UTF8.GetBytes(input);
        }
    }
}
