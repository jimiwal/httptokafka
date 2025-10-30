using HttpToKafka.Api.Security;
using Xunit;
using FluentAssertions;

public class HmacSignerTests
{
    [Fact]
    public void SignsDeterministically()
    {
        var s = new HmacSigner();
        var a = s.Sign("secret", "body");
        var b = s.Sign("secret", "body");
        a.Should().Be(b);
    }
}
