using Confluent.SchemaRegistry;
using System.Text.Json;

namespace HttpToKafka.Api.Services.Serialization;

public sealed class JsonSchemaSerializerWrapper : IValueSerializer
{
    private readonly ISchemaRegistryClient _sr;
    private readonly HttpToKafka.Api.Models.JsonSchemaSettings? _settings;

    public JsonSchemaSerializerWrapper(ISchemaRegistryClient sr, HttpToKafka.Api.Models.JsonSchemaSettings? settings)
    { _sr = sr; _settings = settings; }

    public async Task<byte[]> SerializeAsync(string topic, JsonElement? jsonValue, string? base64Value, CancellationToken ct)
    {
        if (jsonValue is null) throw new ArgumentException("ValueJson is required for JSON Schema.");

        var serializer = new Confluent.SchemaRegistry.Serdes.JsonSerializer<JsonElement>(_sr, new Confluent.SchemaRegistry.Serdes.JsonSerializerConfig
        {
            AutoRegisterSchemas = _settings?.AutoRegisterSchemas ?? true,
            SubjectNameStrategy = Confluent.SchemaRegistry.SubjectNameStrategy.TopicName
        });

        return await serializer.SerializeAsync(jsonValue.Value,
            new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic), ct);
    }
}
