using HttpToKafka.Api.Models;
using HttpToKafka.Api.Services.Subscriptions;
using Microsoft.Extensions.Configuration;
using Xunit;

public class SubscriptionTests
{
    [Fact]
    public void Create_and_Renew_Subscription_Works()
    {
        var cfg = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string,string?>{
            ["Subscriptions:DefaultTtlSeconds"] = "10"
        }).Build();

        var mgr = new SubscriptionManager(cfg);
        var dto = mgr.Create(new SubscribeAsyncRequest{
            Topic = "orders",
            CallbackUrl = new Uri("https://client/endpoint"),
            FromNow = true
        });

        Assert.Equal(SubscriptionStatus.Active, dto.Status);
        var old = dto.ExpiresUtc;

        mgr.Renew(dto.Id, 20, out var upd);
        Assert.NotNull(upd);
        Assert.True(upd!.ExpiresUtc > old);
    }
}
