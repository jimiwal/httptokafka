using HttpToKafka.Api.Security;
using HttpToKafka.Api.Subscriptions.Models;
using Microsoft.Extensions.Options;
using HttpToKafka.Api.Configuration;
using System.Net.Http.Json;

namespace HttpToKafka.Api.Webhooks;
public sealed class WebhookSender : IWebhookSender
{
    private readonly HttpClient _http;
    private readonly HmacSigner _signer;
    private readonly WebhookOptions _opts;
    private readonly DeadLetterSender _dead;
    public WebhookSender(HttpClient http, HmacSigner signer, IOptions<WebhookOptions> opts, DeadLetterSender dead)
    { _http = http; _signer = signer; _opts = opts.Value; _dead = dead; }

    public async Task<(bool Success, int Status, string? Error)> SendAsync(Subscription sub, object payload, CancellationToken ct)
    {
        var json = System.Text.Json.JsonSerializer.Serialize(payload);
        var req = new HttpRequestMessage(HttpMethod.Post, sub.CallbackUrl)
        {
            Content = new StringContent(json, System.Text.Encoding.UTF8, "application/json")
        };
        if (!string.IsNullOrEmpty(sub.HmacSecret))
        {
            var sig = _signer.Sign(sub.HmacSecret!, json);
            req.Headers.Add("X-Signature", sig);
        }
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            cts.CancelAfter(TimeSpan.FromSeconds(_opts.DeliveryTimeoutSeconds));
            var res = await _http.SendAsync(req, cts.Token);
            if (!res.IsSuccessStatusCode)
            {
                await _dead.SendAsync(sub, json, (int)res.StatusCode, $"Non-2xx: {res.StatusCode}");
                return (false, (int)res.StatusCode, res.ReasonPhrase);
            }
            return (true, (int)res.StatusCode, null);
        }
        catch (Exception ex)
        {
            await _dead.SendAsync(sub, json, 0, ex.Message);
            return (false, 0, ex.Message);
        }
    }
}
