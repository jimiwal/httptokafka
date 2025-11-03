namespace KafkaOverHttp.Models;

public sealed class MessageEnvelope
{
    public string Topic { get; set; } = default!;
    public string? KeyBase64 { get; set; }
    public string? ValueBase64 { get; set; }
    public object? AvroValue { get; set; }
    public int Partition { get; set; }
    public long Offset { get; set; }
    public DateTime TimestampUtc { get; set; }
    public Dictionary<string,string>? Headers { get; set; }
}
