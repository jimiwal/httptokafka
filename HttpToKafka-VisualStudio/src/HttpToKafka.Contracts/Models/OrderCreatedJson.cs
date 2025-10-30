namespace HttpToKafka.Contracts;

public class OrderCreatedJson
{
    public string orderId { get; set; } = default!;
    public double amount { get; set; }
    public string currency { get; set; } = default!;
}
