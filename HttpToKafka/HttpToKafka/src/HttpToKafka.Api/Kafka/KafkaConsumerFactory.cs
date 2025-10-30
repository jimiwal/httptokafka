using Confluent.Kafka;
using HttpToKafka.Api.Configuration;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Api.Kafka;
public sealed class KafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly KafkaOptions _options;
    public KafkaConsumerFactory(IOptions<KafkaOptions> options) => _options = options.Value;

    public IConsumer<byte[], byte[]> Create(string groupId)
    {
        var cfg = new ConsumerConfig
        {
            BootstrapServers = _options.BootstrapServers,
            GroupId = groupId,
            EnableAutoCommit = true,
            AutoOffsetReset = AutoOffsetReset.Latest
        };
        if (_options.UseOAuthBearer)
        {
            cfg.SecurityProtocol = Enum.Parse<SecurityProtocol>(_options.SecurityProtocol!);
            cfg.SaslMechanism = Enum.Parse<SaslMechanism>(_options.SaslMechanism!);
            cfg.SaslOauthbearerTokenEndpointUrl = _options.SaslOauthbearerTokenEndpointUrl;
            cfg.SaslOauthbearerClientId = _options.SaslOauthbearerClientId;
            cfg.SaslOauthbearerClientSecret = _options.SaslOauthbearerClientSecret;
        }
        return new ConsumerBuilder<byte[], byte[]>(cfg).Build();
    }
}
