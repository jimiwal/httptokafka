using Confluent.Kafka;
using HttpToKafka.Api.Contracts;
using HttpToKafka.Api.Kafka;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;

namespace HttpToKafka.Api.Controllers;
[ApiController]
[Route("api/subscribe")]
public sealed class SubscribeController : ControllerBase
{
    private readonly IKafkaConsumerFactory _factory;
    public SubscribeController(IKafkaConsumerFactory factory) => _factory = factory;

    [HttpPost("sync")]
    [Authorize]
    public ActionResult<SubscribeSyncResponse> Sync([FromBody] SubscribeSyncRequest req)
    {
        var groupId = string.IsNullOrWhiteSpace(req.GroupId) ? $"httptokafka-sync-{Guid.NewGuid():N}" : req.GroupId!;
        using var consumer = _factory.Create(groupId);
        consumer.Subscribe(req.Topic);
        if (req.StartFromEnd)
        {
            var md = consumer.GetMetadata(req.Topic, TimeSpan.FromSeconds(5));
            foreach (var p in md.Topics.First().Partitions)
            {
                var tpp = new TopicPartition(req.Topic, new Partition(p.PartitionId));
                var end = consumer.QueryWatermarkOffsets(tpp, TimeSpan.FromSeconds(5)).High;
                consumer.Assign(new TopicPartitionOffset(tpp, end));
            }
        }
        var resp = new SubscribeSyncResponse();
        var sw = System.Diagnostics.Stopwatch.StartNew();
        while (sw.Elapsed < TimeSpan.FromSeconds(req.WaitSeconds))
        {
            var cr = consumer.Consume(TimeSpan.FromMilliseconds(100));
            if (cr is not null && cr.Message is not null)
            {
                resp.HasMessage = true;
                resp.KeyBase64 = cr.Message.Key is null ? null : Convert.ToBase64String(cr.Message.Key);
                resp.ValueBase64 = Convert.ToBase64String(cr.Message.Value);
                resp.Partition = cr.Partition.Value.ToString();
                resp.Offset = cr.Offset.Value.ToString();
                return Ok(resp);
            }
        }
        resp.HasMessage = false; resp.Reason = "Timeout";
        return Ok(resp);
    }
}
