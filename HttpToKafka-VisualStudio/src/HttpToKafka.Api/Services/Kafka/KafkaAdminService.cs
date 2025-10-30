using Confluent.Kafka.Admin;
using HttpToKafka.Api.Security;
using Microsoft.Extensions.Configuration;
using Confluent.Kafka;

namespace HttpToKafka.Api.Services.Kafka;

public interface IKafkaAdminService
{
    object GetSafeConfig();
    Task<IEnumerable<string>> ListTopicsAsync(CancellationToken ct);
    Task<object> DescribeTopicAsync(string topic, CancellationToken ct);
}

public class KafkaAdminService : IKafkaAdminService
{
    private readonly IConfiguration _cfg;
    public KafkaAdminService(IConfiguration cfg) => _cfg = cfg;

    public object GetSafeConfig() => new
    {
        Kafka = new {
            BootstrapServers = _cfg["Kafka:BootstrapServers"],
            SecurityProtocol = _cfg["Kafka:SecurityProtocol"],
            Sasl = new {
                Mechanism = _cfg["Kafka:Sasl:Mechanism"],
                ClientId = Redaction.Mask(_cfg["Kafka:Sasl:ClientId"]),
                ClientSecret = Redaction.Mask(_cfg["Kafka:Sasl:ClientSecret"]),
                TokenEndpoint = _cfg["Kafka:Sasl:TokenEndpoint"]
            },
            Producer = new {
                Acks = _cfg["Kafka:Producer:Acks"],
                EnableIdempotence = _cfg["Kafka:Producer:EnableIdempotence"],
                MessageTimeoutMs = _cfg["Kafka:Producer:MessageTimeoutMs"]
            },
            Consumer = new {
                AutoOffsetReset = _cfg["Kafka:Consumer:AutoOffsetReset"],
                EnableAutoCommit = _cfg["Kafka:Consumer:EnableAutoCommit"],
                SessionTimeoutMs = _cfg["Kafka:Consumer:SessionTimeoutMs"]
            }
        },
        SchemaRegistry = new {
            Url = _cfg["SchemaRegistry:Url"],
            BasicAuth = new { Username = Redaction.Mask(_cfg["SchemaRegistry:BasicAuth:Username"]), Password = Redaction.Mask(_cfg["SchemaRegistry:BasicAuth:Password"]) }
        }
    };

    public async Task<IEnumerable<string>> ListTopicsAsync(CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _cfg["Kafka:BootstrapServers"] }).Build();
        var md = admin.GetMetadata(TimeSpan.FromSeconds(5));
        return md.Topics.Select(t => t.Topic);
    }

    public async Task<object> DescribeTopicAsync(string topic, CancellationToken ct)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = _cfg["Kafka:BootstrapServers"] }).Build();
        var md = admin.GetMetadata(topic, TimeSpan.FromSeconds(5)).Topics.FirstOrDefault();
        if (md == null || md.Error.IsError) throw new KeyNotFoundException($"Topic {topic} not found.");
        return new
        {
            topic = md.Topic,
            isInternal = md.IsInternal,
            partitions = md.Partitions.Select(p => new { id = p.PartitionId, leader = p.Leader, replicas = p.Replicas, isrs = p.InSyncReplicas })
        };
    }
}
