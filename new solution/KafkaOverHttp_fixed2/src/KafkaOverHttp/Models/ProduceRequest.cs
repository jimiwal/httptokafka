using System.ComponentModel.DataAnnotations;

namespace KafkaOverHttp.Models;

public sealed class ProduceRequest
{
    [Required] public string Topic { get; set; } = default!;
    public string? Key { get; set; }
    [Required] public string Value { get; set; } = default!;
    [Required] public SerializationKind Serialization { get; set; }
    public string? AvroSchema { get; set; }
    public Dictionary<string,string>? Headers { get; set; }
    public int? Partition { get; set; }
}
