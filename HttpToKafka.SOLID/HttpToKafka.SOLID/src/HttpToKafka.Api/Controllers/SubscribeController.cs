using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;

[ApiController]
[Route("subscribe")]
public class SubscribeController : ControllerBase
{
    private readonly IConsumerService _consumerService;
    private readonly IAsyncSubscriptionManager _asyncManager;

    public SubscribeController(IConsumerService consumerService, IAsyncSubscriptionManager asyncManager)
    {
        _consumerService = consumerService;
        _asyncManager = asyncManager;
    }

    [HttpPost("sync")]
    public async Task<IActionResult> SubscribeSync([FromBody] SubscribeSyncRequest req, CancellationToken ct)
    {
        var res = await _consumerService.SubscribeOnceAsync(req, ct);
        return Ok(res);
    }

    [HttpPost("async")]
    public async Task<IActionResult> SubscribeAsync([FromBody] SubscribeAsyncRequest req, CancellationToken ct)
    {
        var res = await _asyncManager.StartAsync(req, ct);
        return Accepted(res);
    }

    [HttpDelete("{subscriptionId}")]
    public IActionResult Cancel(string subscriptionId)
    {
        var ok = _asyncManager.Cancel(subscriptionId);
        if (ok) return Ok(new CancelSubscriptionResponse { SubscriptionId = subscriptionId, Status = "cancelled" });
        return NotFound(new { error = "not_found" });
    }

    [HttpGet("{subscriptionId}")]
    public ActionResult<SubscriptionInfo> Get(string subscriptionId)
    {
        var info = _asyncManager.Get(subscriptionId);
        if (info is null) return NotFound(new { error = "not_found" });
        return Ok(info);
    }

    [HttpGet]
    public ActionResult<IEnumerable<SubscriptionInfo>> List() => Ok(_asyncManager.List());
}
