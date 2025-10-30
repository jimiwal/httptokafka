using HttpToKafka.Api.Controllers;
using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Xunit;

namespace HttpToKafka.Tests;

public class SubscribeControllerTests
{
    [Fact]
    public async Task SubscribeSync_PassesStartFromNowToService()
    {
        var consumer = new Mock<IConsumerService>();
        var asyncMgr = new Mock<IAsyncSubscriptionManager>();
        var req = new SubscribeSyncRequest { Topic = "t", Format = MessageFormat.Bytes, StartFromNow = true };
        consumer.Setup(c => c.SubscribeOnceAsync(It.Is<SubscribeSyncRequest>(r => r.StartFromNow), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new SubscribeSyncResponse { Outcome = "timeout", Topic = "t" });

        var ctrl = new SubscribeController(consumer.Object, asyncMgr.Object);
        var res = await ctrl.SubscribeSync(req, CancellationToken.None);

        consumer.VerifyAll();
        Assert.IsType<OkObjectResult>(res);
    }
}
