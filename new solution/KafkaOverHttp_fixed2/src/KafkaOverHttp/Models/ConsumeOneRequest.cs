using System.ComponentModel.DataAnnotations;

namespace KafkaOverHttp.Models;

public sealed class ConsumeOneRequest
{
    [Required] public string Topic { get; set; } = default!;
    [Range(1, int.MaxValue)]
    public int MaxWaitMs { get; set; } = 5000;
    [Required] public SerializationKind Serialization { get; set; }
    public string? AvroSchema { get; set; }
    public int? Partition { get; set; }
    public long? Offset { get; set; }
}
