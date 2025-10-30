using HttpToKafka.Api.Contracts;
using HttpToKafka.Api.Kafka;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;
[ApiController]
[Route("api/produce")]
public sealed class ProduceController : ControllerBase
{
    private readonly IKafkaProducerService _producer;
    public ProduceController(IKafkaProducerService producer) => _producer = producer;

    [HttpPost]
    [Authorize]
    public async Task<ActionResult<ProduceResponse>> Produce([FromBody] ProduceRequest req, CancellationToken ct)
    {
        if (req.Messages == null || req.Messages.Count == 0) return BadRequest("Messages required");
        var res = await _producer.ProduceAsync(req, ct);
        return Ok(res);
    }
}
