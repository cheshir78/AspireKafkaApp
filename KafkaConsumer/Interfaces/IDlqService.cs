using Confluent.Kafka;

namespace KafkaConsumer.Interfaces
{
    public interface IDlqService
    {
        Task SendToDlqAsync(ConsumeResult<string, string> consumeResult, string reason, Exception? exception = null);
    }
}
