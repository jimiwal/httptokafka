namespace HttpToKafka.Api.Configuration;
public sealed class WebhookOptions
{
    public string? DefaultDeadLetterUrl { get; set; }
    public int DeliveryTimeoutSeconds { get; set; } = 15;
}
