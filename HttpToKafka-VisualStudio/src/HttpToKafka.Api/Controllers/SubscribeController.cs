using Confluent.Kafka;
using HttpToKafka.Api.Models;
using HttpToKafka.Api.Services.Kafka;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;

[ApiController]
[Route("subscribe")]
public class SubscribeController : ControllerBase
{
    private readonly IKafkaConsumerFactory _factory;

    public SubscribeController(IKafkaConsumerFactory factory) => _factory = factory;

    [HttpPost("sync")]
    [Authorize]
    public ActionResult<SubscribeSyncResponse> SubscribeSync([FromBody] SubscribeSyncRequest req, CancellationToken ct)
    {
        using var consumer = _factory.Create(req.GroupId, req.FromNow);
        consumer.Subscribe(req.Topic);

        var timeout = TimeSpan.FromSeconds(req.TimeoutSeconds <= 0 ? 30 : req.TimeoutSeconds);
        var end = DateTime.UtcNow + timeout;

        while (DateTime.UtcNow < end && !ct.IsCancellationRequested)
        {
            var remaining = end - DateTime.UtcNow;
            var cr = consumer.Consume(remaining <= TimeSpan.Zero ? TimeSpan.FromMilliseconds(200) : remaining);
            if (cr == null) continue;

            consumer.Commit(cr);

            var headers = cr.Message.Headers?.ToDictionary(h => h.Key, h => System.Text.Encoding.UTF8.GetString(h.GetValueBytes()!));
            return Ok(new SubscribeSyncResponse
            {
                Received = true,
                Topic = cr.Topic,
                Partition = cr.Partition,
                Offset = cr.Offset,
                Value = cr.Message.Value,
                Headers = headers,
                Message = "Message received."
            });
        }

        return Ok(new SubscribeSyncResponse
        {
            Received = false,
            Message = $"No message within {timeout.TotalSeconds} seconds."
        });
    }
}
