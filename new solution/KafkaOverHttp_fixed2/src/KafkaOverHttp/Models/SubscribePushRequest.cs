using System.ComponentModel.DataAnnotations;

namespace KafkaOverHttp.Models;

public sealed class SubscribePushRequest
{
    [Required] public string Topic { get; set; } = default!;
    [Required, Url] public string TargetEndpoint { get; set; } = default!;
    [Range(1000, int.MaxValue)]
    public int DurationMs { get; set; } = 600000;
    [Required] public SerializationKind Serialization { get; set; }
    public string? AvroSchema { get; set; }
    [Range(50, int.MaxValue)]
    public int PollIntervalMs { get; set; } = 200;
    [Required] public string SubscriptionId { get; set; } = default!;
}
