using System.ComponentModel.DataAnnotations;

namespace KafkaOverHttp.Models;

public sealed class CancelSubscriptionRequest
{
    [Required] public string SubscriptionId { get; set; } = default!;
}
