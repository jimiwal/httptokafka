namespace HttpToKafka.Api.Contracts;
public sealed class MessageItem
{
    public string KeyBase64 { get; set; } = string.Empty;
    public string ValueBase64 { get; set; } = string.Empty;
    public object? Value { get; set; }
    public string? Schema { get; set; }
    public string? SchemaSubject { get; set; }
}
