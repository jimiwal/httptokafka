using System.ComponentModel.DataAnnotations;

namespace HttpToKafka.Api.Models;

public class SubscribeSyncRequest
{
    [Required] public string Topic { get; set; } = default!;
    public string? GroupId { get; set; }
    public int TimeoutSeconds { get; set; } = 30;
    public bool FromNow { get; set; } = false;
}

public class SubscribeAsyncRequest
{
    [Required] public string Topic { get; set; } = default!;
    public string? GroupId { get; set; }
    [Required] public Uri CallbackUrl { get; set; } = default!;
    public Uri? DeadLetterUrl { get; set; }
    public int? TtlSeconds { get; set; }
    public bool FromNow { get; set; } = true;
    public bool SignWithHmac { get; set; } = true;
}

public class SubscribeSyncResponse
{
    public bool Received { get; set; }
    public string? Topic { get; set; }
    public int? Partition { get; set; }
    public long? Offset { get; set; }
    public byte[]? Value { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
    public string Message { get; set; } = "";
}

public class SubscribeAsyncResponse
{
    public string SubscriptionId { get; set; } = default!;
    public DateTime ExpiresAtUtc { get; set; }
}
