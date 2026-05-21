using Confluent.Kafka;

namespace CommonModels.Contracts
{
    public interface IDlqService
    {
        Task SendToDlqAsync(ConsumeResult<string, string> consumeResult, string reason, Exception? exception = null);
    }
}
