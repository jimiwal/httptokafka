namespace HttpToKafka.Api.Configuration;
public sealed class KafkaOptions
{
    public string BootstrapServers { get; set; } = string.Empty;
    public bool UseOAuthBearer { get; set; } = false;
    public string? SaslOauthbearerTokenEndpointUrl { get; set; }
    public string? SaslOauthbearerClientId { get; set; }
    public string? SaslOauthbearerClientSecret { get; set; }
    public string? SecurityProtocol { get; set; } = "SASL_SSL";
    public string? SaslMechanism { get; set; } = "OAUTHBEARER";
    public string? SchemaRegistryUrl { get; set; }
    public string? SchemaRegistryApiKey { get; set; }
    public string? SchemaRegistryApiSecret { get; set; }
    public string ConsumerGroupPrefix { get; set; } = "httptokafka";
}
