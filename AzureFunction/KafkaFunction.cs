using CommonModels.Constants;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;

namespace AzureFunction
{
    public class KafkaFunction
    {
        private readonly ILogger<KafkaFunction> _logger;

        public KafkaFunction(ILogger<KafkaFunction> logger)
        {
            _logger = logger;
        }

        [Function(nameof(KafkaFunction))]
        public async Task Run([KafkaTrigger("%BrokerList%", BrokerNames.CUSTOMER_EMPLOYEE, ConsumerGroup = BrokerNames.CUSTOMER_EMPLOYEE_GROUP1, 
            AuthenticationMode = BrokerAuthenticationMode.Plain)] string input, FunctionContext context)
        {
            //var logger = context.GetLogger(nameof(KafkaFunction));
            _logger.LogInformation($"Received: {input}"); 
            await Task.Delay(TimeSpan.FromSeconds(2));
        }
    }
}
