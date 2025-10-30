using HttpToKafka.Api.Contracts;
using HttpToKafka.Api.Subscriptions;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;
[ApiController]
[Route("api/subscriptions")]
public sealed class SubscriptionsController : ControllerBase
{
    private readonly ISubscriptionManager _manager;
    public SubscriptionsController(ISubscriptionManager manager) => _manager = manager;

    [HttpPost]
    [Authorize]
    public ActionResult<SubscribeAsyncResponse> Create([FromBody] SubscribeAsyncRequest req)
    {
        var sub = _manager.Create(req.Topic, req.CallbackUrl, req.DeadLetterUrl, req.HmacSecret, req.StartFromEnd, req.GroupId, TimeSpan.FromSeconds(req.TtlSeconds));
        return Ok(new SubscribeAsyncResponse { SubscriptionId = sub.Id, Status = "Active", ExpiresUtc = sub.ExpiresUtc });
    }

    [HttpGet("{id}")]
    [Authorize]
    public ActionResult<SubscriptionStatusDto> Get(string id)
    {
        if (!_manager.TryGet(id, out var sub) || sub is null) return NotFound();
        return Ok(new SubscriptionStatusDto
        {
            Id = sub.Id,
            Topic = sub.Topic,
            ExpiresUtc = sub.ExpiresUtc,
            Delivered = sub.Metrics.Delivered,
            Failed = sub.Metrics.Failed,
            Active = sub.Active
        });
    }

    [HttpPost("{id}/renew")]
    [Authorize]
    public IActionResult Renew(string id, [FromBody] RenewSubscriptionRequest req)
    {
        return _manager.Renew(id, TimeSpan.FromSeconds(req.ExtendSeconds)) ? Ok() : NotFound();
    }

    [HttpPost("{id}/stop")]
    [Authorize]
    public IActionResult Stop(string id)
    {
        return _manager.Stop(id) ? Ok() : NotFound();
    }
}
