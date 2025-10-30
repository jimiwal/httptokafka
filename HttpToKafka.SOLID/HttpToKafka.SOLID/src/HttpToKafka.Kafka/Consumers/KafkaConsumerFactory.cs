using Confluent.Kafka;
using HttpToKafka.Core.Options;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Kafka.Consumers;

internal class KafkaConsumerFactory
{
    private readonly IOptions<KafkaSettings> _settings;
    public KafkaConsumerFactory(IOptions<KafkaSettings> settings) => _settings = settings;

    public ConsumerConfig BuildConsumerConfig(string? groupId=null, bool? autoCommit=null)
    {
        var s = _settings.Value;
        var cfg = new ConsumerConfig
        {
            BootstrapServers = s.BootstrapServers,
            GroupId = string.IsNullOrWhiteSpace(groupId) ? s.Consumer.GroupId : groupId,
            AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(s.Consumer.AutoOffsetReset, true, out var aor) ? aor : AutoOffsetReset.Earliest,
            EnableAutoCommit = autoCommit ?? s.Consumer.EnableAutoCommit,
            SessionTimeoutMs = s.Consumer.SessionTimeoutMs,
            MaxPollIntervalMs = s.Consumer.MaxPollIntervalMs
        };
        ApplySecurity(cfg, s);
        return cfg;
    }

    public IConsumer<byte[], byte[]> CreateBytesConsumer(string? groupId=null, bool? autoCommit=null)
    {
        var cfg = BuildConsumerConfig(groupId, autoCommit);
        return new ConsumerBuilder<byte[], byte[]>(cfg).Build();
    }

    private static void ApplySecurity(ClientConfig cfg, KafkaSettings s)
    {
        if (!string.IsNullOrWhiteSpace(s.SecurityProtocol) && Enum.TryParse<SecurityProtocol>(s.SecurityProtocol, true, out var sp)) cfg.SecurityProtocol = sp;
        if (!string.IsNullOrWhiteSpace(s.SaslMechanism) && Enum.TryParse<SaslMechanism>(s.SaslMechanism, true, out var mech)) cfg.SaslMechanism = mech;
        if (!string.IsNullOrWhiteSpace(s.SaslUsername)) cfg.SaslUsername = s.SaslUsername;
        if (!string.IsNullOrWhiteSpace(s.SaslPassword)) cfg.SaslPassword = s.SaslPassword;
    }
}
