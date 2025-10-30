using Confluent.Kafka;
using Microsoft.Extensions.Configuration;

namespace HttpToKafka.Api.Services.Kafka;

public interface IKafkaConsumerFactory
{
    IConsumer<byte[], byte[]> Create(string? groupIdOverride = null, bool fromNow = false);
}

public class KafkaConsumerFactory : IKafkaConsumerFactory
{
    private readonly IConfiguration _cfg;
    private readonly OAuthBearerTokenProvider _oauth;

    public KafkaConsumerFactory(IConfiguration cfg, OAuthBearerTokenProvider oauth)
    { _cfg = cfg; _oauth = oauth; }

    public IConsumer<byte[], byte[]> Create(string? groupIdOverride = null, bool fromNow = false)
    {
        var c = new ConsumerConfig
        {
            BootstrapServers = _cfg["Kafka:BootstrapServers"],
            GroupId = groupIdOverride ?? $"http-to-kafka-{Guid.NewGuid():N}",
            AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(_cfg["Kafka:Consumer:AutoOffsetReset"] ?? "Earliest", true, out var ar) ? ar : AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        };

        var sec = _cfg["Kafka:SecurityProtocol"];
        var builder = new ConsumerBuilder<byte[], byte[]>(c);
        if (!string.IsNullOrEmpty(sec) && sec != "PLAINTEXT")
        {
            c.SecurityProtocol = Enum.Parse<SecurityProtocol>(sec, true);
            c.SaslMechanism = Enum.Parse<SaslMechanism>(_cfg["Kafka:Sasl:Mechanism"] ?? "OAUTHBEARER", true);

            builder.SetOAuthBearerTokenRefreshHandler(async (client, cfg) =>
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
            });
        }

        var consumer = builder.Build();

        if (fromNow)
        {
            consumer.OnPartitionsAssigned += (_, partitions) =>
            {
                var endOffsets = partitions.Select(p => new TopicPartitionOffset(p, Offset.End));
                consumer.Assign(endOffsets);
            };
        }

        return consumer;
    }
}
