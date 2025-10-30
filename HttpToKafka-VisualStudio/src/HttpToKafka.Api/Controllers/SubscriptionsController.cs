using HttpToKafka.Api.Models;
using HttpToKafka.Api.Services.Subscriptions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;

[ApiController]
[Route("subscriptions")]
public class SubscriptionsController : ControllerBase
{
    private readonly ISubscriptionManager _mgr;

    public SubscriptionsController(ISubscriptionManager mgr) => _mgr = mgr;

    [HttpPost]
    [Authorize]
    public ActionResult<SubscribeAsyncResponse> Create([FromBody] SubscribeAsyncRequest req)
    {
        var s = _mgr.Create(req);
        return Ok(new SubscribeAsyncResponse { SubscriptionId = s.Id, ExpiresAtUtc = s.ExpiresUtc });
    }

    [HttpGet("{id}")]
    [Authorize]
    public ActionResult<SubscriptionDto> Get(string id)
        => _mgr.TryGet(id, out var dto) ? Ok(dto) : NotFound();

    [HttpGet]
    [Authorize]
    public ActionResult<IEnumerable<SubscriptionDto>> List()
        => Ok(_mgr.GetAll());

    [HttpPost("{id}/renew")]
    [Authorize]
    public ActionResult<SubscriptionDto> Renew(string id, [FromQuery] int seconds = 300)
        => _mgr.Renew(id, seconds, out var dto) ? Ok(dto) : NotFound();

    [HttpDelete("{id}")]
    [Authorize]
    public IActionResult Stop(string id)
        => _mgr.Stop(id) ? NoContent() : NotFound();
}
