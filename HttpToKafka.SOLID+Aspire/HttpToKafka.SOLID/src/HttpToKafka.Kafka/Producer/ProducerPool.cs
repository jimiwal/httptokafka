using Confluent.Kafka;
using HttpToKafka.Core.Options;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Kafka.Producer;

internal class ProducerPool : IDisposable
{
    private readonly IOptions<KafkaSettings> _settings;
    private IProducer<byte[], byte[]>? _bytesProducer;
    private readonly object _lock = new();

    public ProducerPool(IOptions<KafkaSettings> settings) => _settings = settings;

    public IProducer<byte[], byte[]> GetBytesProducer()
    {
        if (_bytesProducer != null) return _bytesProducer;
        lock (_lock)
        {
            if (_bytesProducer != null) return _bytesProducer;
            _bytesProducer = new ProducerBuilder<byte[], byte[]>(BuildProducerConfig()).Build();
        }
        return _bytesProducer!;
    }

    internal ProducerConfig BuildProducerConfig()
    {
        var s = _settings.Value;
        var cfg = new ProducerConfig
        {
            BootstrapServers = s.BootstrapServers,
            EnableIdempotence = s.Producer.EnableIdempotence,
            Acks = s.Producer.Acks?.ToLowerInvariant() switch
            {
                "none" => Acks.None,
                "leader" => Acks.Leader,
                _ => Acks.All
            }
        };
        if (s.Producer.MessageTimeoutMs.HasValue) cfg.MessageTimeoutMs = s.Producer.MessageTimeoutMs;
        if (!string.IsNullOrWhiteSpace(s.Producer.CompressionType) &&
            Enum.TryParse<CompressionType>(s.Producer.CompressionType, true, out var ct))
        {
            cfg.CompressionType = ct;
        }
        ApplySecurity(cfg, s);
        return cfg;
    }

    private static void ApplySecurity(ClientConfig cfg, KafkaSettings s)
    {
        if (!string.IsNullOrWhiteSpace(s.SecurityProtocol) &&
            Enum.TryParse<SecurityProtocol>(s.SecurityProtocol, true, out var sp))
            cfg.SecurityProtocol = sp;
        if (!string.IsNullOrWhiteSpace(s.SaslMechanism) &&
            Enum.TryParse<SaslMechanism>(s.SaslMechanism, true, out var mech))
            cfg.SaslMechanism = mech;
        if (!string.IsNullOrWhiteSpace(s.SaslUsername)) cfg.SaslUsername = s.SaslUsername;
        if (!string.IsNullOrWhiteSpace(s.SaslPassword)) cfg.SaslPassword = s.SaslPassword;
    }

    public void Dispose()
    {
        try { _bytesProducer?.Flush(TimeSpan.FromSeconds(3)); } catch { }
        _bytesProducer?.Dispose();
    }
}
