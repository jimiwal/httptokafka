using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using HttpToKafka.Core.Options;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Options;

namespace HttpToKafka.Api.Controllers;

[ApiController]
[Route("kafka")]
public class ManagementController : ControllerBase
{
    private readonly IOptions<KafkaSettings> _kafka;
    private readonly IOptions<SchemaRegistrySettings> _schema;
    private readonly IAdminService _admin;

    public ManagementController(IOptions<KafkaSettings> kafka, IOptions<SchemaRegistrySettings> schema, IAdminService admin)
    {
        _kafka = kafka;
        _schema = schema;
        _admin = admin;
    }

    [HttpGet("config")]
    public ActionResult<KafkaConfigResponse> GetConfig()
    {
        var k = _kafka.Value;
        var sr = _schema.Value;
        var result = new KafkaConfigResponse
        {
            Producer = new Dictionary<string, string?>
            {
                ["Acks"] = k.Producer.Acks,
                ["EnableIdempotence"] = k.Producer.EnableIdempotence.ToString(),
                ["CompressionType"] = k.Producer.CompressionType,
                ["MessageTimeoutMs"] = k.Producer.MessageTimeoutMs?.ToString(),
                ["BootstrapServers"] = k.BootstrapServers,
                ["SecurityProtocol"] = k.SecurityProtocol,
                ["SaslMechanism"] = k.SaslMechanism,
                ["SaslUsername"] = string.IsNullOrEmpty(k.SaslUsername) ? null : "***",
                ["SaslPassword"] = string.IsNullOrEmpty(k.SaslPassword) ? null : "***"
            },
            Consumer = new Dictionary<string, string?>
            {
                ["GroupId"] = k.Consumer.GroupId,
                ["AutoOffsetReset"] = k.Consumer.AutoOffsetReset,
                ["EnableAutoCommit"] = k.Consumer.EnableAutoCommit.ToString(),
                ["SessionTimeoutMs"] = k.Consumer.SessionTimeoutMs.ToString(),
                ["MaxPollIntervalMs"] = k.Consumer.MaxPollIntervalMs?.ToString()
            },
            SchemaRegistry = new Dictionary<string, string?>
            {
                ["Url"] = sr.Url,
                ["BasicAuthUserInfo"] = string.IsNullOrEmpty(sr.BasicAuthUserInfo) ? null : "***",
                ["BasicAuthCredentialsSource"] = sr.BasicAuthCredentialsSource
            }
        };
        return Ok(result);
    }

    [HttpGet("topics")]
    public ActionResult<IEnumerable<TopicInfo>> ListTopics() => Ok(_admin.ListTopics());

    [HttpGet("topics/{name}")]
    public ActionResult<TopicInfo> TopicDetails(string name)
    {
        var t = _admin.GetTopicDetails(name);
        if (t is null) return NotFound(new { error = "topic_not_found" });
        return Ok(t);
    }
}
