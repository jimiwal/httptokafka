using HttpToKafka.Api.Contracts;
using HttpToKafka.Api.Controllers;
using HttpToKafka.Api.Kafka;
using Microsoft.AspNetCore.Mvc;
using Moq;
using Xunit;
using System.Collections.Generic;
using System.Threading.Tasks;

public class ProducerControllerTests
{
    [Fact]
    public async Task RejectsEmptyMessages()
    {
        var mock = new Mock<IKafkaProducerService>();
        var ctl = new ProduceController(mock.Object);
        var res = await ctl.Produce(new ProduceRequest{ Topic="t", Messages = new List<MessageItem>() }, default);
        Assert.IsType<BadRequestObjectResult>(res.Result);
    }
}
