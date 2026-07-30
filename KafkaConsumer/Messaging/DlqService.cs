using CommonModels.Constants;
using Confluent.Kafka;
using KafkaConsumer.Interfaces;
using System.Text;

namespace KafkaConsumer.Messaging
{
    public class DlqService : IDlqService
    {
        private readonly IProducer<string, string> _producer;

        public DlqService(IProducer<string, string> producer)
        {
            _producer = producer;
        }

        public async Task SendToDlqAsync(ConsumeResult<string, string> consumeResult, string reason, Exception? exception = null)
        {
            // Copy headers
            var headers = consumeResult.Message.Headers ?? new Headers();

            // Add metadata about exception into Kafka headers
            headers.Add("dlq-reason", Encoding.UTF8.GetBytes(reason));
            headers.Add("dlq-exception", Encoding.UTF8.GetBytes(exception?.Message ?? "None"));
            headers.Add("dlq-original-partition", Encoding.UTF8.GetBytes(consumeResult.Partition.Value.ToString()));
            headers.Add("dlq-original-offset", Encoding.UTF8.GetBytes(consumeResult.Offset.Value.ToString()));

            var dlqMessage = new Message<string, string>
            {
                Key = consumeResult.Message.Key,
                Value = consumeResult.Message.Value, // copy original message
                Headers = headers
            };

            // send into DLQ topic
            await _producer.ProduceAsync(BrokerNames.DLQ_CUSTOMER_EMPLOYEE, dlqMessage);
        }
    }
}
