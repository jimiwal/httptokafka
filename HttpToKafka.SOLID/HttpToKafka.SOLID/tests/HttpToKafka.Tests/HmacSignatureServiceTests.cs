using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using HttpToKafka.Core.Security;
using Xunit;

namespace HttpToKafka.Tests;

public class HmacSignatureServiceTests
{
    [Fact]
    public void BuildsHeaders_WithSecret_AddsSignatureAndTimestamp()
    {
        var clock = new FixedClock(1730200000);
        var svc = new HmacSignatureService(clock);
        var req = new SubscribeAsyncRequest
        {
            Topic = "t",
            CallbackUrl = "http://example",
            CallbackHmacSecret = "secret",
            CallbackSignatureHeaderName = "X-Sig",
            CallbackTimestampHeaderName = "X-Ts",
            CallbackIncludeTimestamp = true
        };
        var headers = svc.BuildHeaders("{\"a\":1}", req);
        Assert.True(headers.ContainsKey("X-Sig"));
        Assert.True(headers.ContainsKey("X-Ts"));
        Assert.Equal("1730200000", headers["X-Ts"]);
        var expected = "sha256=2ec03d0bf93d0a15791dd6d0908a2d795b9b977f82d21a9e2f29487d8aa38d8c";
        Assert.Equal(expected, headers["X-Sig"]);
    }

    private class FixedClock : IClock
    {
        private readonly long _ts;
        public FixedClock(long ts) => _ts = ts;
        public long UnixSeconds() => _ts;
        public DateTimeOffset UtcNow() => DateTimeOffset.FromUnixTimeSeconds(_ts);
    }
}
