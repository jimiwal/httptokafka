using Confluent.Kafka;
namespace HttpToKafka.Api.Kafka;
public interface IKafkaConsumerFactory
{
    IConsumer<byte[], byte[]> Create(string groupId);
}
