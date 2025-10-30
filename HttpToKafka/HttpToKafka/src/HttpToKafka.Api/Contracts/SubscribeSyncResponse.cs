namespace HttpToKafka.Api.Contracts;
public sealed class SubscribeSyncResponse
{
    public bool HasMessage { get; set; }
    public string? KeyBase64 { get; set; }
    public string? ValueBase64 { get; set; }
    public string? Partition { get; set; }
    public string? Offset { get; set; }
    public string? Reason { get; set; }
}
