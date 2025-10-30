using HttpToKafka.Api.Configuration;
using HttpToKafka.Api.Contracts;

namespace HttpToKafka.Api.Kafka.Serialization;
public sealed class RawBytesSerializer : ISerializerStrategy
{
    public Task<(byte[]? key, byte[] value)> SerializeAsync(MessageItem item, KafkaOptions options, CancellationToken ct)
    {
        var key = string.IsNullOrEmpty(item.KeyBase64) ? null : Convert.FromBase64String(item.KeyBase64);
        var value = Convert.FromBase64String(item.ValueBase64);
        return Task.FromResult<(byte[]?, byte[])>((key, value));
    }
}
