using HttpToKafka.Api.Contracts;
namespace HttpToKafka.Api.Kafka;
public interface IKafkaProducerService
{
    Task<ProduceResponse> ProduceAsync(ProduceRequest request, CancellationToken ct);
}
