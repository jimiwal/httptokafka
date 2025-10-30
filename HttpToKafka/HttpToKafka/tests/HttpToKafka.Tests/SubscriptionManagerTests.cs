using HttpToKafka.Api.Subscriptions;
using Xunit;
using FluentAssertions;
using System;

public class SubscriptionManagerTests
{
    [Fact]
    public void CreateAndRenew()
    {
        var mgr = new SubscriptionManager();
        var s = mgr.Create("topic","http://cb",null,null,false,null,TimeSpan.FromSeconds(1));
        mgr.Renew(s.Id, TimeSpan.FromSeconds(10)).Should().BeTrue();
    }
}
