using HttpToKafka.Api.Models;
using HttpToKafka.Api.Services.Kafka;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;

[ApiController]
[Route("produce")]
public class ProduceController : ControllerBase
{
    private readonly IKafkaProducer _producer;
    public ProduceController(IKafkaProducer producer) => _producer = producer;

    [HttpPost]
    [Authorize]
    public async Task<ActionResult<ProduceResponse>> Produce([FromBody] ProduceRequest req, CancellationToken ct)
    {
        var results = await _producer.ProduceAsync(req, ct);
        return Ok(new ProduceResponse
        {
            Mode = req.Mode.ToString(),
            Results = results
        });
    }
}
