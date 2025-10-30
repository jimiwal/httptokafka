using System.Text;
using System.Text.Json;
using Polly;

namespace HttpToKafka.Api.Services.Webhooks;

public interface IWebhookClient
{
    Task<bool> PostAsync(Uri url, object payload, bool sign, string subscriptionId, Uri? deadLetterUrl, CancellationToken ct);
}

public class WebhookClient : IWebhookClient
{
    private readonly HttpClient _http;
    private readonly HmacSigner _signer;
    private readonly IConfiguration _cfg;
    private readonly DeadLetterService _dead;

    private readonly IAsyncPolicy<HttpResponseMessage> _retry;

    public WebhookClient(HttpClient http, HmacSigner signer, IConfiguration cfg, DeadLetterService dead)
    {
        _http = http; _signer = signer; _cfg = cfg; _dead = dead;

        var attempts = Math.Max(1, int.TryParse(cfg["Subscriptions:MaxWebhookAttempts"], out var a) ? a : 5);
        _retry = Policy<HttpResponseMessage>
            .Handle<HttpRequestException>()
            .OrResult(r => !r.IsSuccessStatusCode)
            .WaitAndRetryAsync(attempts, i => TimeSpan.FromSeconds(Math.Min(30, Math.Pow(2, i))));
    }

    public async Task<bool> PostAsync(Uri url, object payload, bool sign, string subscriptionId, Uri? deadLetterUrl, CancellationToken ct)
    {
        var json = JsonSerializer.Serialize(payload);
        var content = new StringContent(json, Encoding.UTF8, "application/json");
        var req = new HttpRequestMessage(HttpMethod.Post, url) { Content = content };

        if (sign)
        {
            var (sig, alg, ts) = _signer.Sign(Encoding.UTF8.GetBytes(json));
            req.Headers.Add(_cfg["Subscriptions:SignatureHeader"]!, sig);
            req.Headers.Add(_cfg["Subscriptions:SignatureAlgHeader"]!, alg);
            req.Headers.Add(_cfg["Subscriptions:TimestampHeader"]!, ts);
            req.Headers.Add("X-Subscription-Id", subscriptionId);
        }

        var response = await _retry.ExecuteAsync(ct => _http.SendAsync(req, ct), ct);

        if (!response.IsSuccessStatusCode && deadLetterUrl != null)
        {
            await _dead.SendAsync(deadLetterUrl, subscriptionId, json, (int)response.StatusCode, await response.Content.ReadAsStringAsync(ct), ct);
        }

        return response.IsSuccessStatusCode;
    }
}
