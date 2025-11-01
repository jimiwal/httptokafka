using Confluent.Kafka;
using Microsoft.Extensions.Options;

namespace KafkaOverHttp.Services;

public sealed class KafkaAdminService
{
    private readonly AdminClientConfig _cfg;

    public KafkaAdminService(IOptions<AdminClientConfig> cfg)
    {
        _cfg = cfg.Value;
    }

    public IEnumerable<string> ListTopics()
    {
        using var admin = new AdminClientBuilder(_cfg).Build();
        var md = admin.GetMetadata(TimeSpan.FromSeconds(5));
        return md.Topics.Select(t => t.Topic);
    }

    public object ClusterInfo()
    {
        using var admin = new AdminClientBuilder(_cfg).Build();
        var md = admin.GetMetadata(TimeSpan.FromSeconds(5));
        return new
        {
            Brokers = md.Brokers.Select(b => new { b.BrokerId, b.Host, b.Port }),
            Topics = md.Topics.Select(t => new { t.Topic, Partitions = t.Partitions.Count })
        };
    }
}
