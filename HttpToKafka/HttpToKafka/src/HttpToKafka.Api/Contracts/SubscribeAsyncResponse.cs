namespace HttpToKafka.Api.Contracts;
public sealed class SubscribeAsyncResponse
{
    public string SubscriptionId { get; set; } = string.Empty;
    public string Status { get; set; } = string.Empty;
    public DateTime ExpiresUtc { get; set; }
}
