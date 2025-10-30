using System.Text.Json;

namespace HttpToKafka.Api.Services.Serialization;

public interface IValueSerializer
{
    Task<byte[]> SerializeAsync(string topic, JsonElement? jsonValue, string? base64Value, CancellationToken ct);
}
