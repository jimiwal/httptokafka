using Confluent.Kafka;
using Confluent.Kafka.Admin;
using HttpToKafka.Api.Configuration;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Api.Kafka;
public sealed class AdminClientService
{
    private readonly KafkaOptions _options;
    public AdminClientService(IOptions<KafkaOptions> options) => _options = options.Value;

    private IAdminClient Create() => new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _options.BootstrapServers }).Build();

    public IDictionary<string, object> GetSafeConfig() => new Dictionary<string, object>
    {
        ["BootstrapServers"] = _options.BootstrapServers,
        ["UseOAuthBearer"] = _options.UseOAuthBearer,
        ["SchemaRegistryUrl"] = _options.SchemaRegistryUrl ?? string.Empty
    };

    public async Task<IList<string>> ListTopicsAsync(CancellationToken ct)
    {
        using var admin = Create();
        var md = admin.GetMetadata(TimeSpan.FromSeconds(5));
        return md.Topics.Select(t => t.Topic).OrderBy(x => x).ToList();
    }

    public async Task<object> DescribeTopicAsync(string topic)
    {
        using var admin = Create();
        var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(5));
        var t = md.Topics.FirstOrDefault();
        if (t is null) return new { Error = "Topic not found" };
        return new
        {
            t.Topic,
            Partitions = t.Partitions.Select(p => new { p.PartitionId, p.Leader, Replicas = p.Replicas, InSyncReplicas = p.InSyncReplicas }),
            Error = t.Error.ToString()
        };
    }
}
