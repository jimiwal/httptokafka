using System.Collections.Concurrent;
using HttpToKafka.Api.Models;
using Microsoft.Extensions.Configuration;

namespace HttpToKafka.Api.Services.Subscriptions;

public interface ISubscriptionManager
{
    SubscriptionDto Create(SubscribeAsyncRequest req);
    bool TryGet(string id, out SubscriptionDto dto);
    IEnumerable<SubscriptionDto> GetAll();
    bool Renew(string id, int additionalSeconds, out SubscriptionDto? updated);
    bool Stop(string id);
    void IncrementDelivered(string id, TimeSpan latency);
    void IncrementFailed(string id);
    void Expire(string id);
}

public class SubscriptionManager : ISubscriptionManager
{
    private readonly ConcurrentDictionary<string, SubscriptionDto> _subs = new();
    private readonly int _defaultTtl;

    public SubscriptionManager(IConfiguration cfg)
    {
        _defaultTtl = int.TryParse(cfg["Subscriptions:DefaultTtlSeconds"], out var ttl) ? ttl : 600;
    }

    public SubscriptionDto Create(SubscribeAsyncRequest req)
    {
        var id = Guid.NewGuid().ToString("N");
        var now = DateTime.UtcNow;
        var ttl = req.TtlSeconds.GetValueOrDefault(_defaultTtl);
        var groupId = string.IsNullOrWhiteSpace(req.GroupId) ? $"http-to-kafka-{id}" : req.GroupId!;

        var dto = new SubscriptionDto
        {
            Id = id,
            Topic = req.Topic,
            GroupId = groupId,
            Status = SubscriptionStatus.Active,
            CreatedUtc = now,
            ExpiresUtc = now.AddSeconds(ttl),
            FromNow = req.FromNow,
            CallbackUrl = req.CallbackUrl,
            DeadLetterUrl = req.DeadLetterUrl,
            DeliveredCount = 0,
            FailedCount = 0
        };
        _subs[id] = dto;
        return dto;
    }

    public bool TryGet(string id, out SubscriptionDto dto) => _subs.TryGetValue(id, out dto!);
    public IEnumerable<SubscriptionDto> GetAll() => _subs.Values.OrderBy(v => v.CreatedUtc);

    public bool Renew(string id, int additionalSeconds, out SubscriptionDto? updated)
    {
        updated = null;
        if (!_subs.TryGetValue(id, out var dto)) return false;
        dto.ExpiresUtc = dto.ExpiresUtc.AddSeconds(additionalSeconds);
        updated = dto;
        return true;
    }

    public bool Stop(string id)
    {
        if (!_subs.TryGetValue(id, out var dto)) return false;
        dto.Status = SubscriptionStatus.Stopped;
        return true;
    }

    public void IncrementDelivered(string id, TimeSpan latency)
    {
        if (_subs.TryGetValue(id, out var dto))
        {
            dto.DeliveredCount++;
            dto.AvgDeliveryLatencyMs = dto.AvgDeliveryLatencyMs <= 0
                ? latency.TotalMilliseconds
                : (dto.AvgDeliveryLatencyMs * 0.9 + latency.TotalMilliseconds * 0.1);
        }
    }

    public void IncrementFailed(string id)
    {
        if (_subs.TryGetValue(id, out var dto))
            dto.FailedCount++;
    }

    public void Expire(string id)
    {
        if (_subs.TryGetValue(id, out var dto))
            dto.Status = SubscriptionStatus.Expired;
    }
}
