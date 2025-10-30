using Confluent.SchemaRegistry;
using HttpToKafka.Core.Options;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Kafka.SchemaRegistry;

public class SchemaRegistryClientFactory
{
    private readonly IOptions<SchemaRegistrySettings> _settings;
    private ISchemaRegistryClient? _client;
    private readonly object _lock = new();

    public SchemaRegistryClientFactory(IOptions<SchemaRegistrySettings> settings) => _settings = settings;

    public ISchemaRegistryClient Get()
    {
        if (_client != null) return _client;
        lock (_lock)
        {
            if (_client != null) return _client;
            var s = _settings.Value;
            if (string.IsNullOrWhiteSpace(s.Url))
                throw new InvalidOperationException("SchemaRegistry.Url is not configured");
            var cfg = new SchemaRegistryConfig
            {
                Url = s.Url,
                BasicAuthUserInfo = s.BasicAuthUserInfo,
                BasicAuthCredentialsSource = s.BasicAuthCredentialsSource
            };
            _client = new CachedSchemaRegistryClient(cfg);
        }
        return _client!;
    }
}
