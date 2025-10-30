using Confluent.Kafka;
using HttpToKafka.Api.Models;
using HttpToKafka.Api.Services.Serialization;
using Microsoft.Extensions.Configuration;

namespace HttpToKafka.Api.Services.Kafka;

public interface IKafkaProducer
{
    Task<List<ProduceResult>> ProduceAsync(ProduceRequest req, CancellationToken ct);
}

public class KafkaProducerService : IKafkaProducer
{
    private readonly IConfiguration _cfg;
    private readonly SerializerFactory _serializerFactory;
    private readonly OAuthBearerTokenProvider _oauth;

    public KafkaProducerService(IConfiguration cfg, SerializerFactory serializerFactory, OAuthBearerTokenProvider oauth)
    {
        _cfg = cfg; _serializerFactory = serializerFactory; _oauth = oauth;
    }

    private IProducer<byte[], byte[]> BuildProducer()
    {
        var p = new ProducerConfig
        {
            BootstrapServers = _cfg["Kafka:BootstrapServers"],
            Acks = Enum.TryParse<Acks>(_cfg["Kafka:Producer:Acks"], true, out var acks) ? acks : Acks.All,
            EnableIdempotence = bool.TryParse(_cfg["Kafka:Producer:EnableIdempotence"], out var idemp) && idemp,
            MessageTimeoutMs = int.TryParse(_cfg["Kafka:Producer:MessageTimeoutMs"], out var mt) ? mt : 30000
        };

        var sec = _cfg["Kafka:SecurityProtocol"];
        if (!string.IsNullOrEmpty(sec) && sec != "PLAINTEXT")
        {
            p.SecurityProtocol = Enum.Parse<SecurityProtocol>(sec, true);
            p.SaslMechanism = Enum.Parse<SaslMechanism>(_cfg["Kafka:Sasl:Mechanism"] ?? "OAUTHBEARER", true);
            return new ProducerBuilder<byte[], byte[]>(p)
                .SetOAuthBearerTokenRefreshHandler(async (client, cfg) =>
                {
                    try
                    {
                        var tok = await _oauth.GetTokenAsync();
                        client.OAuthBearerSetToken(tok.AccessToken, tok.Expiration, tok.Principal, tok.Extensions);
                    }
                    catch (Exception ex)
                    {
                        client.OAuthBearerSetTokenFailure(ex.Message);
                    }
                })
                .Build();
        }
        return new ProducerBuilder<byte[], byte[]>(p).Build();
    }

    public async Task<List<ProduceResult>> ProduceAsync(ProduceRequest req, CancellationToken ct)
    {
        using var producer = BuildProducer();
        var serializer = _serializerFactory.Create(req.Format, req.Avro, req.JsonSchema, req.Protobuf);

        var results = new List<ProduceResult>(req.Messages.Count);

        foreach (var m in req.Messages)
        {
            ct.ThrowIfCancellationRequested();

            var valBytes = await serializer.SerializeAsync(req.Topic, m.ValueJson, m.ValueBase64, ct);
            byte[]? keyBytes = m.KeyBase64 != null ? Convert.FromBase64String(m.KeyBase64)
                : (m.Key != null ? System.Text.Encoding.UTF8.GetBytes(m.Key) : null);

            var message = new Message<byte[], byte[]>
            {
                Key = keyBytes,
                Value = valBytes,
                Headers = new Headers()
            };
            if (m.Headers != null)
                foreach (var kv in m.Headers)
                    message.Headers.Add(kv.Key, System.Text.Encoding.UTF8.GetBytes(kv.Value));

            var tp = m.Partition.HasValue
                ? new TopicPartition(req.Topic, new Partition(m.Partition.Value))
                : new TopicPartition(req.Topic, Partition.Any);

            if (req.Mode == ProduceMode.Sync)
            {
                var dr = await producer.ProduceAsync(tp, message, ct);
                results.Add(new ProduceResult
                {
                    Topic = dr.Topic, Partition = dr.Partition.Value, Offset = dr.Offset.Value,
                    Key = m.Key ?? m.KeyBase64
                });
            }
            else
            {
                producer.Produce(tp, message);
            }
        }

        if (req.Mode == ProduceMode.Async)
            producer.Flush(TimeSpan.FromSeconds(1));

        return results;
    }
}
