namespace HttpToKafka.Api.Contracts;
public sealed class SubscribeSyncRequest
{
    public string Topic { get; set; } = string.Empty;
    public int WaitSeconds { get; set; } = 15;
    public bool StartFromEnd { get; set; } = false;
    public string? GroupId { get; set; }
}
