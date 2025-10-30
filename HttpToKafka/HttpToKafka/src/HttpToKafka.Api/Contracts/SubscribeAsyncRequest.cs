namespace HttpToKafka.Api.Contracts;
public sealed class SubscribeAsyncRequest
{
    public string Topic { get; set; } = string.Empty;
    public string CallbackUrl { get; set; } = string.Empty;
    public string? DeadLetterUrl { get; set; }
    public string? HmacSecret { get; set; }
    public int TtlSeconds { get; set; } = 3600;
    public bool StartFromEnd { get; set; } = false;
    public string? GroupId { get; set; }
}
