using HttpToKafka.Core.Models;

namespace HttpToKafka.Core.Abstractions;

public interface IProducerService
{
    Task<ProduceResponse> ProduceAsync(ProduceRequest request, CancellationToken ct);
}

public interface IConsumerService
{
    Task<SubscribeSyncResponse> SubscribeOnceAsync(SubscribeSyncRequest request, CancellationToken ct);
}

public interface IAsyncSubscriptionManager
{
    Task<SubscribeAsyncResponse> StartAsync(SubscribeAsyncRequest request, CancellationToken ct);
    bool Cancel(string subscriptionId);
    SubscriptionInfo? Get(string subscriptionId);
    IEnumerable<SubscriptionInfo> List();
}

public interface IAdminService
{
    IEnumerable<TopicInfo> ListTopics(TimeSpan? timeout = null);
    TopicInfo? GetTopicDetails(string topic, TimeSpan? timeout = null);
}

public interface ICallbackSigner
{
    IDictionary<string,string> BuildHeaders(string body, SubscribeAsyncRequest request, long? fixedTimestampSeconds = null);
}

public interface ICallbackSender
{
    Task<bool> SendAsync(string url, string body, IDictionary<string,string> headers, CancellationToken ct);
    Task<bool> SendDeadLetterAsync(string url, string body, IDictionary<string,string>? headers, CancellationToken ct);
}

public interface IBackoffStrategy
{
    int NextDelayMs(int attempt);
}

public interface IClock
{
    long UnixSeconds();
    DateTimeOffset UtcNow();
}
