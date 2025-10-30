using HttpToKafka.Api.Configuration;
using HttpToKafka.Api.Contracts;

namespace HttpToKafka.Api.Kafka.Serialization;
public interface ISerializerStrategy
{
    Task<(byte[]? key, byte[] value)> SerializeAsync(MessageItem item, KafkaOptions options, CancellationToken ct);
}
public static class SerializerFactory
{
    public static async Task<(byte[]? key, byte[] value)> SerializeAsync(Contracts.SerializerType type, MessageItem item, KafkaOptions options, CancellationToken ct)
    {
        ISerializerStrategy strategy = type switch
        {
            Contracts.SerializerType.Avro => new AvroSerializerStrategy(),
            Contracts.SerializerType.Protobuf => new ProtobufSerializerStrategy(),
            Contracts.SerializerType.JsonSchema => new JsonSchemaSerializerStrategy(),
            _ => new RawBytesSerializer()
        };
        return await strategy.SerializeAsync(item, options, ct);
    }
}
