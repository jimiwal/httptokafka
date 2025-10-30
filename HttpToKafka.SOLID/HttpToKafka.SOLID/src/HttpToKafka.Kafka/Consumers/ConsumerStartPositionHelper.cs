using Confluent.Kafka;

namespace HttpToKafka.Kafka.Consumers;

internal static class ConsumerStartPositionHelper
{
    public static void AssignToEnd<TKey,TValue>(IConsumer<TKey,TValue> consumer, string topic)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(5);
        var assignment = consumer.Assignment;
        while (assignment.Count == 0 && DateTime.UtcNow < deadline)
        {
            consumer.Consume(TimeSpan.FromMilliseconds(30));
            assignment = consumer.Assignment;
        }
        if (assignment.Count > 0)
        {
            consumer.Assign(assignment.Select(tp => new TopicPartitionOffset(tp, Offset.End)));
            return;
        }
        var md = consumer.GetMetadata(topic, TimeSpan.FromSeconds(5));
        var t = md.Topics.FirstOrDefault();
        if (t != null)
        {
            var end = t.Partitions.Select(p => new TopicPartitionOffset(new TopicPartition(topic, p.PartitionId), Offset.End));
            consumer.Assign(end);
        }
    }
}
