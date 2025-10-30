using Confluent.Kafka;
using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using HttpToKafka.Core.Options;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Kafka.Admin;

public class KafkaAdminService : IAdminService
{
    private readonly IOptions<KafkaSettings> _opt;
    public KafkaAdminService(IOptions<KafkaSettings> opt) => _opt = opt;

    private AdminClientConfig BuildConfig()
    {
        var s = _opt.Value;
        var cfg = new AdminClientConfig { BootstrapServers = s.BootstrapServers };
        if (!string.IsNullOrWhiteSpace(s.SecurityProtocol) && Enum.TryParse<SecurityProtocol>(s.SecurityProtocol, true, out var sp)) cfg.SecurityProtocol = sp;
        if (!string.IsNullOrWhiteSpace(s.SaslMechanism) && Enum.TryParse<SaslMechanism>(s.SaslMechanism, true, out var mech)) cfg.SaslMechanism = mech;
        if (!string.IsNullOrWhiteSpace(s.SaslUsername)) cfg.SaslUsername = s.SaslUsername;
        if (!string.IsNullOrWhiteSpace(s.SaslPassword)) cfg.SaslPassword = s.SaslPassword;
        return cfg;
    }

    public IEnumerable<TopicInfo> ListTopics(TimeSpan? timeout = null)
    {
        using var admin = new AdminClientBuilder(BuildConfig()).Build();
        var md = admin.GetMetadata(timeout ?? TimeSpan.FromSeconds(5));
        foreach (var t in md.Topics)
        {
            yield return new TopicInfo
            {
                Name = t.Topic,
                Partitions = t.Partitions.Count,
                ReplicationFactor = (short)(t.Partitions.FirstOrDefault()?.Replicas.Length ?? 0),
                IsInternal = t.IsInternal,
                PartitionDetails = t.Partitions.Select(p => new PartitionInfo
                {
                    Id = p.PartitionId,
                    Leader = p.Leader,
                    Replicas = p.Replicas.Select(r => r).ToArray(),
                    InSyncReplicas = p.InSyncReplicas.Select(r => r).ToArray()
                }).ToList()
            };
        }
    }

    public TopicInfo? GetTopicDetails(string topic, TimeSpan? timeout = null)
    {
        using var admin = new AdminClientBuilder(BuildConfig()).Build();
        var md = admin.GetMetadata(topic, timeout ?? TimeSpan.FromSeconds(5));
        var t = md.Topics.FirstOrDefault();
        if (t == null) return null;
        var info = new TopicInfo
        {
            Name = t.Topic,
            Partitions = t.Partitions.Count,
            ReplicationFactor = (short)(t.Partitions.FirstOrDefault()?.Replicas.Length ?? 0),
            IsInternal = t.IsInternal,
            PartitionDetails = t.Partitions.Select(p => new PartitionInfo
            {
                Id = p.PartitionId,
                Leader = p.Leader,
                Replicas = p.Replicas.Select(r => r).ToArray(),
                InSyncReplicas = p.InSyncReplicas.Select(r => r).ToArray()
            }).ToList()
        };
        try
        {
            var resources = new List<Confluent.Kafka.Admin.ConfigResource> { new(Confluent.Kafka.Admin.ResourceType.Topic, topic) };
            var configs = admin.DescribeConfigs(resources);
            var entries = configs.FirstOrDefault()?.Entries;
            if (entries != null) info.Configs = entries.ToDictionary(e => e.Name, e => e.Value);
        }
        catch { }
        return info;
    }
}
