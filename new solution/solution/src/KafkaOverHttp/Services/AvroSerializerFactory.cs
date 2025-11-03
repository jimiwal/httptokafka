using Confluent.Kafka;
using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;

namespace KafkaOverHttp.Services;

public sealed class AvroSerializerFactory
{
    private readonly SchemaRegistryConfig _cfg;
    private readonly CachedSchemaRegistryClient _client;

    public AvroSerializerFactory(Microsoft.Extensions.Options.IOptions<SchemaRegistryConfig> cfg)
    {
        _cfg = cfg.Value;
        _client = new CachedSchemaRegistryClient(_cfg);
    }

    public ISchemaRegistryClient Client => _client;

    // Producer side: provide async serializer (IAsyncSerializer<object>).
    public async Task<IAsyncSerializer<object>> CreateValueSerializerAsync()
        => await Task.FromResult(new AvroSerializer<object>(_client));
}
