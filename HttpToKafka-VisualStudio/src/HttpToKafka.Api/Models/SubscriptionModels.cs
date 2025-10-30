namespace HttpToKafka.Api.Models;

public enum SubscriptionStatus { Active, Expired, Stopped }

public class SubscriptionDto
{
    public string Id { get; set; } = default!;
    public string Topic { get; set; } = default!;
    public string GroupId { get; set; } = default!;
    public SubscriptionStatus Status { get; set; }
    public DateTime CreatedUtc { get; set; }
    public DateTime ExpiresUtc { get; set; }
    public bool FromNow { get; set; }
    public Uri CallbackUrl { get; set; } = default!;
    public Uri? DeadLetterUrl { get; set; }
    public long DeliveredCount { get; set; }
    public long FailedCount { get; set; }
    public double AvgDeliveryLatencyMs { get; set; }
}
