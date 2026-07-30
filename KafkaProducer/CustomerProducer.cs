using CommonModels.BusEntity;
using CommonModels.Constants;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using System.Text.Json;

namespace KafkaProducer
{
    public class CustomerProducer
    {
        private readonly ILogger<CustomerProducer> _logger;
        private readonly IProducer<string, string> _producer;

        public CustomerProducer(ILogger<CustomerProducer> logger, IProducer<string, string> producer)
        {
            _logger = logger;
            _producer = producer;
        }

        public async Task RunAsync(CancellationToken cancellationToken = default)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                // produce Employee
                var contact = new ContactEmployee("12345678")
                {
                    IsEmployee = true
                };
                var transferObject = new BusObject<ContactEmployee>(DateTime.UtcNow, contact);
                var dr = await _producer.ProduceAsync(BrokerNames.CUSTOMER_EMPLOYEE, new Message<string, string> { Value = JsonSerializer.Serialize(transferObject) });
                _logger.LogInformation("Delivered employee '{Value}' to '{TopicPartitionOffset}'", dr.Value, dr.TopicPartitionOffset);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);

                // produce Customer
                var contactCustomer = new ContactCustomer("123456789")
                {
                    IsEmployee = false,
                    IsMono = true
                };
                var transferObjectCustomer = new BusObject<ContactCustomer>(DateTime.UtcNow, contactCustomer);
                var dr1 = await _producer.ProduceAsync(BrokerNames.CUSTOMER_EMPLOYEE, new Message<string, string> { Value = JsonSerializer.Serialize(transferObjectCustomer) });
                _logger.LogInformation("Delivered customer '{Value}' to '{TopicPartitionOffset}'", dr1.Value, dr1.TopicPartitionOffset);
                await Task.Delay(TimeSpan.FromSeconds(5), cancellationToken);
            }
        }
    }
}
