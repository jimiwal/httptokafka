using HttpToKafka.Api.Subscriptions.Models;
namespace HttpToKafka.Api.Webhooks;
public interface IWebhookSender
{
    Task<(bool Success, int Status, string? Error)> SendAsync(Subscription sub, object payload, CancellationToken ct);
}
