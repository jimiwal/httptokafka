namespace HttpToKafka.Api.Contracts;
public sealed class WebhookDeliveryResult
{
    public bool Success { get; set; }
    public int HttpStatus { get; set; }
    public string? Error { get; set; }
}
