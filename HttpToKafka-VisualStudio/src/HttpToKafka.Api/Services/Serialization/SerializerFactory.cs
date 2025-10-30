using Confluent.SchemaRegistry;
using HttpToKafka.Api.Models;

namespace HttpToKafka.Api.Services.Serialization;

public class SerializerFactory
{
    private readonly ISchemaRegistryClient _sr;
    public SerializerFactory(ISchemaRegistryClient sr) => _sr = sr;

    public IValueSerializer Create(PayloadFormat fmt, AvroSettings? avro, JsonSchemaSettings? json, ProtobufSettings? proto)
        => fmt switch
        {
            PayloadFormat.Bytes      => new ByteArraySerializerWrapper(),
            PayloadFormat.Avro       => new AvroSerializerWrapper(_sr, avro),
            PayloadFormat.JsonSchema => new JsonSchemaSerializerWrapper(_sr, json),
            PayloadFormat.Protobuf   => new ProtobufSerializerWrapper(_sr, proto),
            _ => throw new NotSupportedException(fmt.ToString())
        };
}
