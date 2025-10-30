namespace HttpToKafka.Core.Models;

public record TopicInfo
{
    public string Name { get; init; } = "";
    public int Partitions { get; init; }
    public short ReplicationFactor { get; init; }
    public bool IsInternal { get; init; }
    public Dictionary<string,string>? Configs { get; init; }
    public List<PartitionInfo> PartitionDetails { get; init; } = new();
}

public record PartitionInfo
{
    public int Id { get; init; }
    public int Leader { get; init; }
    public int[] Replicas { get; init; } = Array.Empty<int>();
    public int[] InSyncReplicas { get; init; } = Array.Empty<int>();
}

public record KafkaConfigResponse
{
    public Dictionary<string,string?> Producer { get; init; } = new();
    public Dictionary<string,string?> Consumer { get; init; } = new();
    public Dictionary<string,string?> SchemaRegistry { get; init; } = new();
}
