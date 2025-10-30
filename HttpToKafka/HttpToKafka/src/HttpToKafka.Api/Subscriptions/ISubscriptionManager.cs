using HttpToKafka.Api.Subscriptions.Models;
namespace HttpToKafka.Api.Subscriptions;
public interface ISubscriptionManager
{
    Subscription Create(string topic, string callbackUrl, string? deadLetterUrl, string? hmacSecret, bool startFromEnd, string? groupId, TimeSpan ttl);
    bool TryGet(string id, out Subscription? sub);
    IEnumerable<Subscription> All();
    bool Renew(string id, TimeSpan extend);
    bool Stop(string id);
}
