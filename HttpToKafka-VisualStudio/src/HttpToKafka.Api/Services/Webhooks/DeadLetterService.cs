using System.Text;
using System.Text.Json;

namespace HttpToKafka.Api.Services.Webhooks;

public class DeadLetterService
{
    private readonly HttpClient _http;
    private readonly HmacSigner _signer;
    private readonly IConfiguration _cfg;
    public DeadLetterService(HttpClient http, HmacSigner signer, IConfiguration cfg) { _http = http; _signer = signer; _cfg = cfg; }

    public async Task SendAsync(Uri deadUrl, string subscriptionId, string originalJson, int statusCode, string responseBody, CancellationToken ct)
    {
        var payload = new { subscriptionId, error = new { statusCode, responseBody }, originalPayload = originalJson };
        var json = JsonSerializer.Serialize(payload);
        var content = new StringContent(json, Encoding.UTF8, "application/json");

        var (sig, alg, ts) = _signer.Sign(Encoding.UTF8.GetBytes(json));
        var req = new HttpRequestMessage(HttpMethod.Post, deadUrl) { Content = content };
        req.Headers.Add(_cfg["Subscriptions:SignatureHeader"]!, sig);
        req.Headers.Add(_cfg["Subscriptions:SignatureAlgHeader"]!, alg);
        req.Headers.Add(_cfg["Subscriptions:TimestampHeader"]!, ts);
        req.Headers.Add("X-Subscription-Id", subscriptionId);

        await _http.SendAsync(req, ct);
    }
}
