using System.Text;
using System.Text.Json;

namespace HttpToKafka.Api.Services.Serialization;

public sealed class ByteArraySerializerWrapper : IValueSerializer
{
    public Task<byte[]> SerializeAsync(string topic, JsonElement? jsonValue, string? base64Value, CancellationToken ct)
    {
        if (!string.IsNullOrEmpty(base64Value))
            return Task.FromResult(Convert.FromBase64String(base64Value));

        if (jsonValue is { } je)
        {
            var s = je.ToString();
            return Task.FromResult(Encoding.UTF8.GetBytes(s));
        }
        throw new ArgumentException("ValueBase64 or ValueJson is required for Bytes format.");
    }
}
