using HttpToKafka.Api.Configuration;
using HttpToKafka.Api.Subscriptions.Models;
using Microsoft.Extensions.Options;
using System.Net.Http.Json;

namespace HttpToKafka.Api.Webhooks;
public sealed class DeadLetterSender
{
    private readonly HttpClient _http;
    private readonly WebhookOptions _opts;
    public DeadLetterSender(HttpClient http, IOptions<WebhookOptions> opts) { _http = http; _opts = opts.Value; }
    public async Task SendAsync(Subscription sub, string json, int status, string error)
    {
        var url = sub.DeadLetterUrl ?? _opts.DefaultDeadLetterUrl;
        if (string.IsNullOrWhiteSpace(url)) return;
        var payload = new { sub.Id, sub.Topic, Status = status, Error = error, Body = json };
        await _http.PostAsJsonAsync(url, payload);
    }
}
