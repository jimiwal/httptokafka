using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;

[ApiController]
[Route("produce")]
public class ProduceController : ControllerBase
{
    private readonly IProducerService _producer;
    public ProduceController(IProducerService producer) => _producer = producer;

    [HttpPost]
    public async Task<IActionResult> Produce([FromBody] ProduceRequest req, CancellationToken ct)
    {
        var res = await _producer.ProduceAsync(req, ct);
        if (req.Mode == ProduceMode.Sync) return Ok(res);
        return Accepted(res);
    }
}
