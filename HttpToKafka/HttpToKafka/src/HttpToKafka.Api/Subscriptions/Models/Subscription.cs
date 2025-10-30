namespace HttpToKafka.Api.Subscriptions.Models;
public sealed class Subscription
{
    public string Id { get; init; } = Guid.NewGuid().ToString("N");
    public string Topic { get; init; } = string.Empty;
    public string CallbackUrl { get; init; } = string.Empty;
    public string? DeadLetterUrl { get; init; }
    public string? HmacSecret { get; init; }
    public string GroupId { get; init; } = string.Empty;
    public DateTime ExpiresUtc { get; set; }
    public bool StartFromEnd { get; init; }
    public SubscriptionMetrics Metrics { get; } = new();
    public bool Active { get; set; } = true;
}
