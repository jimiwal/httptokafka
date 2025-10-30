using HttpToKafka.Api.Subscriptions.Models;
namespace HttpToKafka.Api.Subscriptions;
public sealed class SubscriptionManager : ISubscriptionManager
{
    private readonly Dictionary<string, Subscription> _subs = new();
    private readonly object _gate = new();

    public Subscription Create(string topic, string callbackUrl, string? deadLetterUrl, string? hmacSecret, bool startFromEnd, string? groupId, TimeSpan ttl)
    {
        var sub = new Subscription
        {
            Topic = topic,
            CallbackUrl = callbackUrl,
            DeadLetterUrl = deadLetterUrl,
            HmacSecret = hmacSecret,
            StartFromEnd = startFromEnd,
            GroupId = string.IsNullOrWhiteSpace(groupId) ? $"httptokafka-{Guid.NewGuid():N}" : groupId!,
            ExpiresUtc = DateTime.UtcNow.Add(ttl),
            Active = true
        };
        lock (_gate) _subs[sub.Id] = sub;
        return sub;
    }
    public bool TryGet(string id, out Subscription? sub) { lock (_gate) return _subs.TryGetValue(id, out sub); }
    public IEnumerable<Subscription> All() { lock (_gate) return _subs.Values.ToList(); }
    public bool Renew(string id, TimeSpan extend)
    {
        lock (_gate)
        {
            if (!_subs.TryGetValue(id, out var s)) return false;
            s.ExpiresUtc = s.ExpiresUtc.Add(extend); s.Active = true; return true;
        }
    }
    public bool Stop(string id) { lock (_gate) { if (!_subs.TryGetValue(id, out var s)) return false; s.Active = false; return true; } }
}
