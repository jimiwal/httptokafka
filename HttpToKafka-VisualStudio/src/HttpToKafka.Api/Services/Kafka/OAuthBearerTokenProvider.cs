using System.Net.Http.Headers;
using System.Text;
using System.Text.Json;

namespace HttpToKafka.Api.Services.Kafka;

public sealed class OAuthBearerTokenProvider
{
    private readonly IConfiguration _cfg;
    private readonly HttpClient _http = new();

    public OAuthBearerTokenProvider(IConfiguration cfg) => _cfg = cfg;

    public async Task<(string AccessToken, DateTimeOffset Expiration, string Principal, IEnumerable<KeyValuePair<string,string>>? Extensions)> GetTokenAsync()
    {
        var tokenEndpoint = _cfg["Kafka:Sasl:TokenEndpoint"] ?? throw new InvalidOperationException("Kafka:Sasl:TokenEndpoint not set.");
        var clientId = _cfg["Kafka:Sasl:ClientId"] ?? throw new InvalidOperationException("Kafka:Sasl:ClientId not set.");
        var clientSecret = _cfg["Kafka:Sasl:ClientSecret"] ?? throw new InvalidOperationException("Kafka:Sasl:ClientSecret not set.");
        var scope = _cfg["Kafka:Sasl:Scope"];
        var body = new List<KeyValuePair<string, string>> { new("grant_type","client_credentials") };
        if (!string.IsNullOrWhiteSpace(scope)) body.Add(new("scope", scope));

        var req = new HttpRequestMessage(HttpMethod.Post, tokenEndpoint) { Content = new FormUrlEncodedContent(body) };
        var basic = Convert.ToBase64String(Encoding.UTF8.GetBytes($"{clientId}:{clientSecret}"));
        req.Headers.Authorization = new AuthenticationHeaderValue("Basic", basic);

        var resp = await _http.SendAsync(req);
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(json);
        var tok = doc.RootElement.GetProperty("access_token").GetString()!;
        var expSec = doc.RootElement.TryGetProperty("expires_in", out var exp) ? exp.GetInt32() : 3600;

        return (tok, DateTimeOffset.UtcNow.AddSeconds(expSec - 30), clientId, null);
    }
}
