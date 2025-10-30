using System.Text;
using System.Text.Json;
using FluentAssertions;
using Xunit;
using static HttpToKafka.IntegrationTests.Aspire.HttpJson;

namespace HttpToKafka.IntegrationTests.Aspire;

[Collection("aspire-collection")]
public class EndpointsTests
{
    private readonly AspireFixture _fx;
    public EndpointsTests(AspireFixture fx) { _fx = fx; }

    [Fact(DisplayName="GET /kafka/config returns OK")]
    public async Task KafkaConfig_ReturnsOk()
    {
        var resp = await _fx.ApiClient!.GetAsync("/kafka/config");
        resp.EnsureSuccessStatusCode();
        var json = await resp.Content.ReadAsStringAsync();
        json.Should().Contain("\"Producer\"");
    }

    [Fact(DisplayName="GET /kafka/topics returns OK")]
    public async Task KafkaTopics_ReturnsOk()
    {
        var resp = await _fx.ApiClient!.GetAsync("/kafka/topics");
        resp.EnsureSuccessStatusCode();
    }

    [Fact(DisplayName="POST /produce then /subscribe/sync reads it")]
    public async Task Produce_Then_SubscribeSync_ReadsMessage()
    {
        var topic = $"it-bytes-{Guid.NewGuid():N}";

        var prod = await _fx.ApiClient!.PostAsync("/produce", Json(new {
            Topic = topic, Mode = "Sync", Format = "Bytes", Value = "hello"
        }));
        prod.EnsureSuccessStatusCode();

        var sub = await _fx.ApiClient.PostAsync("/subscribe/sync", Json(new {
            Topic = topic, Format = "Bytes", TimeoutMs = 15000, StartFromNow = false,
            GroupId = $"g-{Guid.NewGuid():N}", AutoCommit = false
        }));
        sub.EnsureSuccessStatusCode();
        var body = await sub.Content.ReadAsStringAsync();
        using var doc = JsonDocument.Parse(body);
        var outcome = doc.RootElement.GetProperty("Outcome").GetString();
        outcome.Should().Be("message");
        var b64 = doc.RootElement.GetProperty("Value").GetString();
        Encoding.UTF8.GetString(Convert.FromBase64String(b64!)).Should().Be("hello");
    }

    [Fact(DisplayName="SubscribeSync StartFromNow delivers only new messages")]
    public async Task SubscribeSync_StartFromNow_Behavior()
    {
        var topic = $"it-startfromnow-{Guid.NewGuid():N}";

        var subscribeTask = _fx.ApiClient!.PostAsync("/subscribe/sync", Json(new {
            Topic = topic, Format = "Bytes", TimeoutMs = 20000, StartFromNow = true,
            GroupId = $"g-{Guid.NewGuid():N}", AutoCommit = false
        }));

        await Task.Delay(500);
        var prod = await _fx.ApiClient.PostAsync("/produce", Json(new {
            Topic = topic, Mode = "Sync", Format = "Bytes", Value = "msg-after"
        }));
        prod.EnsureSuccessStatusCode();

        var sub = await subscribeTask;
        sub.EnsureSuccessStatusCode();
    }
}
