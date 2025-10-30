using System.ComponentModel.DataAnnotations;
using System.Text.Json.Nodes;

namespace HttpToKafka.Core.Models;

public record ProduceRequest
{
    [Required] public string Topic { get; init; } = default!;
    public ProduceMode Mode { get; init; } = ProduceMode.Sync;
    public MessageFormat Format { get; init; } = MessageFormat.Bytes;

    public string? Key { get; init; }
    public string KeyEncoding { get; init; } = "utf8"; // utf8 | base64

    // Bytes
    public string? Value { get; init; }
    public string ValueEncoding { get; init; } = "utf8"; // utf8 | base64

    // Avro
    public string? AvroSchema { get; init; }
    public JsonObject? AvroPayload { get; init; }

    // Kafka opts
    public int? Partition { get; init; }
    public Dictionary<string,string>? Headers { get; init; }
}

public record ProduceResponse
{
    public string Topic { get; init; } = "";
    public int? Partition { get; init; }
    public long? Offset { get; init; }
    public DateTimeOffset? Timestamp { get; init; }
    public string Mode { get; init; } = "";
    public string Status { get; init; } = "ok";
    public string? Error { get; init; }
    public string? CorrelationId { get; init; }
}
