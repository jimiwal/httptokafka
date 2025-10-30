using HttpToKafka.Api.Controllers;
using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Xunit;

namespace HttpToKafka.Tests;

public class ProduceControllerTests
{
    [Fact]
    public async Task Produce_Sync_ReturnsOk()
    {
        var mock = new Mock<IProducerService>();
        mock.Setup(m => m.ProduceAsync(It.IsAny<ProduceRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ProduceResponse { Topic = "t", Mode = "sync", Status = "delivered" });

        var ctrl = new ProduceController(mock.Object);
        var req = new ProduceRequest { Topic = "t", Mode = ProduceMode.Sync, Format = MessageFormat.Bytes, Value = "hi" };
        var result = await ctrl.Produce(req, CancellationToken.None);

        Assert.IsType<OkObjectResult>(result);
    }

    [Fact]
    public async Task Produce_Async_ReturnsAccepted()
    {
        var mock = new Mock<IProducerService>();
        mock.Setup(m => m.ProduceAsync(It.IsAny<ProduceRequest>(), It.IsAny<CancellationToken>()))
            .ReturnsAsync(new ProduceResponse { Topic = "t", Mode = "async", Status = "queued" });

        var ctrl = new ProduceController(mock.Object);
        var req = new ProduceRequest { Topic = "t", Mode = ProduceMode.Async, Format = MessageFormat.Bytes, Value = "hi" };
        var result = await ctrl.Produce(req, CancellationToken.None);

        Assert.IsType<AcceptedResult>(result);
    }
}
