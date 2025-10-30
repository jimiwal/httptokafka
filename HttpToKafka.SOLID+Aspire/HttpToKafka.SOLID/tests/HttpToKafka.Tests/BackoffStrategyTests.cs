using HttpToKafka.Core.Utils;
using Xunit;

namespace HttpToKafka.Tests;

public class BackoffStrategyTests
{
    [Fact]
    public void ExponentialBackoff_CapsAtMax()
    {
        var b = new ExponentialBackoffStrategy(initialMs: 500, maxMs: 5000);
        Assert.Equal(500, b.NextDelayMs(1));
        Assert.Equal(1000, b.NextDelayMs(2));
        Assert.Equal(2000, b.NextDelayMs(3));
        Assert.Equal(4000, b.NextDelayMs(4));
        Assert.Equal(5000, b.NextDelayMs(5));
        Assert.Equal(5000, b.NextDelayMs(10));
    }
}
