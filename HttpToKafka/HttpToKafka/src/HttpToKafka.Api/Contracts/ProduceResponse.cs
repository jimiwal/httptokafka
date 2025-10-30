namespace HttpToKafka.Api.Contracts;
public sealed class ProduceResponse
{
    public string Topic { get; init; } = string.Empty;
    public int Count { get; init; }
    public IList<string> Offsets { get; init; } = new List<string>();
}
