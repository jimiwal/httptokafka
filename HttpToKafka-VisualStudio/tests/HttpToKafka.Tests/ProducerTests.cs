using Xunit;

public class ProducerTests
{
    [Fact(Skip = "Integration test requires running Kafka via AppHost")]
    public void Produce_In_Order_Sync()
    {
        // This test is intentionally skipped by default.
    }
}
