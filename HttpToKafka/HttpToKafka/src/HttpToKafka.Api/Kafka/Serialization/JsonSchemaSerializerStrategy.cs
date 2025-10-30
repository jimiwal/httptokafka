using HttpToKafka.Api.Configuration;
using HttpToKafka.Api.Contracts;

namespace HttpToKafka.Api.Kafka.Serialization;
public sealed class JsonSchemaSerializerStrategy : ISerializerStrategy
{
    public Task<(byte[]? key, byte[] value)> SerializeAsync(MessageItem item, KafkaOptions options, CancellationToken ct)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(item.Value);
        var bytes = System.Text.Encoding.UTF8.GetBytes(json);
        return Task.FromResult<(byte[]?, byte[])>((null, bytes));
    }
}
