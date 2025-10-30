namespace HttpToKafka.Api.Contracts;
public enum ProduceMode { Sync, Async }
public enum SerializerType { Raw, Avro, Protobuf, JsonSchema }
public sealed class ProduceRequest
{
    public string Topic { get; set; } = string.Empty;
    public ProduceMode Mode { get; set; } = ProduceMode.Sync;
    public SerializerType Serializer { get; set; } = SerializerType.Raw;
    public IList<MessageItem> Messages { get; set; } = new List<MessageItem>();
}
