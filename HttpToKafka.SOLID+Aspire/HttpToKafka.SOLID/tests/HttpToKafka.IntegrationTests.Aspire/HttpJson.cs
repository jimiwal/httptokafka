using System.Text;
using System.Text.Json;

namespace HttpToKafka.IntegrationTests.Aspire;

internal static class HttpJson
{
    public static StringContent Json(object obj) =>
        new StringContent(JsonSerializer.Serialize(obj), Encoding.UTF8, "application/json");
}
