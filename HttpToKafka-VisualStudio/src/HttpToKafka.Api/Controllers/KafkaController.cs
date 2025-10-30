using HttpToKafka.Api.Services.Kafka;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;

[ApiController]
[Route("kafka")]
public class KafkaController : ControllerBase
{
    private readonly IKafkaAdminService _admin;
    public KafkaController(IKafkaAdminService admin) => _admin = admin;

    [HttpGet("config")]
    [Authorize]
    public ActionResult<object> Config() => Ok(_admin.GetSafeConfig());

    [HttpGet("topics")]
    [Authorize]
    public async Task<ActionResult<IEnumerable<string>>> Topics(CancellationToken ct)
        => Ok(await _admin.ListTopicsAsync(ct));

    [HttpGet("topics/{topic}")]
    [Authorize]
    public async Task<ActionResult<object>> Describe(string topic, CancellationToken ct)
        => Ok(await _admin.DescribeTopicAsync(topic, ct));
}
