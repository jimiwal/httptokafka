using Confluent.Kafka;
using HttpToKafka.Api.Configuration;
using HttpToKafka.Api.Contracts;
using HttpToKafka.Api.Kafka.Serialization;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Api.Kafka;
public sealed class KafkaProducerService : IKafkaProducerService
{
    private readonly KafkaOptions _options;
    public KafkaProducerService(IOptions<KafkaOptions> options) => _options = options.Value;

    private ProducerConfig BuildProducerConfig()
    {
        var cfg = new ProducerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            Acks = Acks.All,
            EnableIdempotence = true
        };
        if (_options.UseOAuthBearer)
        {
            cfg.SecurityProtocol = Enum.Parse<SecurityProtocol>(_options.SecurityProtocol!);
            cfg.SaslMechanism = Enum.Parse<SaslMechanism>(_options.SaslMechanism!);
            cfg.SaslOauthbearerTokenEndpointUrl = _options.SaslOauthbearerTokenEndpointUrl;
            cfg.SaslOauthbearerClientId = _options.SaslOauthbearerClientId;
            cfg.SaslOauthbearerClientSecret = _options.SaslOauthbearerClientSecret;
        }
        return cfg;
    }

    public async Task<ProduceResponse> ProduceAsync(ProduceRequest request, CancellationToken ct)
    {
        var cfg = BuildProducerConfig();
        using var producer = new ProducerBuilder<byte[], byte[]>(cfg).Build();
        var offsets = new List<string>();

        foreach (var item in request.Messages)
        {
            var (key, value) = await SerializerFactory.SerializeAsync(request.Serializer, item, _options, ct);
            if (request.Mode == ProduceMode.Sync)
            {
                var dr = await producer.ProduceAsync(request.Topic, new Message<byte[], byte[]>{ Key = key, Value = value }, ct);
                offsets.Add($"{dr.TopicPartitionOffset}");
            }
            else
            {
                producer.Produce(request.Topic, new Message<byte[], byte[]>{ Key = key, Value = value });
            }
        }
        if (request.Mode == ProduceMode.Async) producer.Flush(TimeSpan.FromSeconds(2));
        return new ProduceResponse { Topic = request.Topic, Count = request.Messages.Count, Offsets = offsets };
    }
}
