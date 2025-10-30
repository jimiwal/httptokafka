namespace HttpToKafka.Core.Options;

public class KafkaSettings
{
    public string BootstrapServers { get; set; } = "";
    public string? SecurityProtocol { get; set; }
    public string? SaslMechanism { get; set; }
    public string? SaslUsername { get; set; }
    public string? SaslPassword { get; set; }
    public ProducerDefaults Producer { get; set; } = new();
    public ConsumerDefaults Consumer { get; set; } = new();

    public class ProducerDefaults
    {
        public string Acks { get; set; } = "all";
        public bool EnableIdempotence { get; set; } = true;
        public string? CompressionType { get; set; }
        public int? MessageTimeoutMs { get; set; } = 120000;
    }
    public class ConsumerDefaults
    {
        public string GroupId { get; set; } = "http-to-kafka-default";
        public string AutoOffsetReset { get; set; } = "earliest";
        public bool EnableAutoCommit { get; set; } = true;
        public int SessionTimeoutMs { get; set; } = 45000;
        public int? MaxPollIntervalMs { get; set; } = 300000;
    }
}

public class SchemaRegistrySettings
{
    public string? Url { get; set; }
    public string? BasicAuthUserInfo { get; set; }
    public string? BasicAuthCredentialsSource { get; set; }
}
