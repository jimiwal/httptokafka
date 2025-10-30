using System.ComponentModel.DataAnnotations;

namespace HttpToKafka.Core.Models;

public record SubscribeSyncRequest
{
    [Required] public string Topic { get; init; } = default!;
    public MessageFormat Format { get; init; } = MessageFormat.Bytes;
    public string? GroupId { get; init; }
    public bool AutoCommit { get; init; } = true;
    public int TimeoutMs { get; init; } = 30000;
    public bool StartFromNow { get; init; } = false;
}

public record SubscribeSyncResponse
{
    public string Outcome { get; init; } = "message";
    public string Topic { get; init; } = "";
    public int Partition { get; init; }
    public long Offset { get; init; }
    public DateTimeOffset Timestamp { get; init; }
    public Dictionary<string,string> Headers { get; init; } = new();
    public object? Value { get; init; }
}

public record SubscribeAsyncRequest
{
    [Required] public string Topic { get; init; } = default!;
    public MessageFormat Format { get; init; } = MessageFormat.Bytes;
    public string? GroupId { get; init; }
    public bool AutoCommit { get; init; } = true;
    public bool StartFromNow { get; init; } = false;

    [Required] public string CallbackUrl { get; init; } = default!;
    public Dictionary<string,string>? CallbackHeaders { get; init; }

    public string? CallbackHmacSecret { get; init; }
    public string CallbackSignatureHeaderName { get; init; } = "X-HTK-Signature";
    public bool CallbackIncludeTimestamp { get; init; } = true;
    public string CallbackTimestampHeaderName { get; init; } = "X-HTK-Timestamp";

    public int? MaxMessages { get; init; }
    public int? MaxSeconds { get; init; }
    public int MaxCallbackRetries { get; init; } = 0;
    public int InitialRetryDelayMs { get; init; } = 1000;
    public string? DeadLetterUrl { get; init; }
    public Dictionary<string,string>? DeadLetterHeaders { get; init; }
}

public record SubscribeAsyncResponse
{
    public string SubscriptionId { get; init; } = "";
    public string Status { get; init; } = "running";
    public string Topic { get; init; } = "";
    public bool StartFromNow { get; init; }
    public string CallbackUrl { get; init; } = "";
    public DateTimeOffset ExpiresAt { get; init; }
}

public record CancelSubscriptionResponse
{
    public string SubscriptionId { get; init; } = "";
    public string Status { get; init; } = "cancelled";
}

public record SubscriptionInfo
{
    public string SubscriptionId { get; init; } = "";
    public string Topic { get; init; } = "";
    public string? GroupId { get; init; }
    public string Format { get; init; } = "";
    public bool AutoCommit { get; init; }
    public bool StartFromNow { get; init; }
    public string CallbackUrl { get; init; } = "";
    public DateTimeOffset StartedAt { get; init; }
    public DateTimeOffset? ExpiresAt { get; init; }
    public DateTimeOffset? LastMessageAt { get; init; }
    public int DeliveredCount { get; init; }
    public int FailedCount { get; init; }
    public string Status { get; init; } = "running";
}
