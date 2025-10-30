using System.ComponentModel.DataAnnotations;
using System.Text.Json;
using HttpToKafka.Api.Services.Serialization;

namespace HttpToKafka.Api.Models;

public enum ProduceMode { Sync, Async }

public class ProduceMessage
{
    public string? Key { get; set; }
    public string? KeyBase64 { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
    public string? ValueBase64 { get; set; }
    public JsonElement? ValueJson { get; set; }
    public int? Partition { get; set; }
}

public class AvroSettings
{
    public string? Schema { get; set; }
    public string? Subject { get; set; }
    public bool AutoRegisterSchemas { get; set; } = true;
}

public class JsonSchemaSettings
{
    public string? Schema { get; set; }
    public string? Subject { get; set; }
    public bool AutoRegisterSchemas { get; set; } = true;
}

public class ProtobufSettings
{
    [Required] public string TypeName { get; set; } = default!;
    public string? Subject { get; set; }
    public bool AutoRegisterSchemas { get; set; } = true;
}

public class ProduceRequest
{
    [Required] public string Topic { get; set; } = default!;
    public ProduceMode Mode { get; set; } = ProduceMode.Sync;
    public PayloadFormat Format { get; set; } = PayloadFormat.Bytes;
    public AvroSettings? Avro { get; set; }
    public JsonSchemaSettings? JsonSchema { get; set; }
    public ProtobufSettings? Protobuf { get; set; }
    [Required, MinLength(1)] public List<ProduceMessage> Messages { get; set; } = new();
}

public class ProduceResult
{
    public string Topic { get; set; } = default!;
    public int Partition { get; set; }
    public long Offset { get; set; }
    public string? Key { get; set; }
}

public class ProduceResponse
{
    public string Mode { get; set; } = default!;
    public List<ProduceResult> Results { get; set; } = new();
}
