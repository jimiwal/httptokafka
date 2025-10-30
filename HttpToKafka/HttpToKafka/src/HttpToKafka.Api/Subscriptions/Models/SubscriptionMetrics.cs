namespace HttpToKafka.Api.Subscriptions.Models;
public sealed class SubscriptionMetrics
{
    public long Delivered { get; set; }
    public long Failed { get; set; }
}
