using Confluent.SchemaRegistry;
using Confluent.SchemaRegistry.Serdes;
using Avro;
using Avro.Generic;
using System.Text.Json;

namespace HttpToKafka.Api.Services.Serialization;

public sealed class AvroSerializerWrapper : IValueSerializer
{
    private readonly ISchemaRegistryClient _sr;
    private readonly HttpToKafka.Api.Models.AvroSettings? _settings;

    public AvroSerializerWrapper(ISchemaRegistryClient sr, HttpToKafka.Api.Models.AvroSettings? settings)
    { _sr = sr; _settings = settings; }

    public async Task<byte[]> SerializeAsync(string topic, JsonElement? jsonValue, string? base64Value, CancellationToken ct)
    {
        if (jsonValue is null) throw new ArgumentException("ValueJson is required for Avro.");
        var schemaStr = _settings?.Schema ?? throw new ArgumentException("Avro schema is required.");
        var schema = Avro.Schema.Parse(schemaStr);
        var record = new GenericRecord((RecordSchema)schema);

        foreach (var f in ((RecordSchema)schema).Fields)
        {
            if (jsonValue.Value.TryGetProperty(f.Name, out var val))
                record.Add(f.Name, Extract(val));
            else
                record.Add(f.Name, null);
        }

        var serializer = new AvroSerializer<GenericRecord>(_sr, new AvroSerializerConfig
        {
            AutoRegisterSchemas = _settings?.AutoRegisterSchemas ?? true,
            SubjectNameStrategy = SubjectNameStrategy.TopicRecord
        });

        return await serializer.SerializeAsync(record,
            new Confluent.Kafka.SerializationContext(Confluent.Kafka.MessageComponentType.Value, topic), ct);
    }

    private static object? Extract(JsonElement el) =>
        el.ValueKind switch
        {
            JsonValueKind.String => el.GetString(),
            JsonValueKind.Number => el.TryGetInt64(out var l) ? l :
                                    el.TryGetDouble(out var d) ? d : (object?)el.GetDecimal(),
            JsonValueKind.True => true,
            JsonValueKind.False => false,
            JsonValueKind.Null => null,
            _ => el.ToString()
        };
}
