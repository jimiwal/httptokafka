namespace HttpToKafka.Api.Contracts;
public sealed class SubscriptionStatusDto
{
    public string Id { get; set; } = string.Empty;
    public string Topic { get; set; } = string.Empty;
    public DateTime ExpiresUtc { get; set; }
    public long Delivered { get; set; }
    public long Failed { get; set; }
    public bool Active { get; set; }
}
public sealed class RenewSubscriptionRequest
{
    public int ExtendSeconds { get; set; } = 3600;
}
