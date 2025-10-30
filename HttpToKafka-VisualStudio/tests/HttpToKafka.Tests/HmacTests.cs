using HttpToKafka.Api.Services.Webhooks;
using Microsoft.Extensions.Configuration;
using Xunit;

public class HmacTests
{
    [Fact]
    public void HmacSignature_HasExpectedFormat()
    {
        var cfg = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string,string?>
        {
            ["Subscriptions:Secret"] = "abc123"
        }).Build();
        var signer = new HmacSigner(cfg);

        var body = System.Text.Encoding.UTF8.GetBytes("{"x":1}");
        var (sig1, alg1, ts1) = signer.Sign(body);

        Assert.StartsWith("v1=", sig1);
        Assert.Equal("HMACSHA256", alg1);
        Assert.True(long.Parse(ts1) > 0);
    }
}
