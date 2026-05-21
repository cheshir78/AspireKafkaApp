using CommonModels.Constants;
using CommonModels.Contracts;
using Confluent.Kafka;
using KafkaConsumer.Handlers;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System.Text.Json;

public class CustomerConsumerService : BackgroundService
{
    private readonly ILogger<CustomerConsumerService> _logger;
    private readonly IConsumer<string, string> _consumer;
    private readonly IBusObjectHandlerFactory _handlerFactory;
    private readonly IDlqService _dlqService; 

    public CustomerConsumerService(IConsumer<string, string> consumer, IDlqService dlqService, IBusObjectHandlerFactory handlerFactory, ILogger<CustomerConsumerService> logger)
    {
        _logger = logger;
        _consumer = consumer;
        _dlqService = dlqService;
        _handlerFactory = handlerFactory;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _consumer.Subscribe(BrokerNames.CUSTOMER_EMPLOYEE);
        _logger.LogInformation($"Subscribed to topic: {BrokerNames.CUSTOMER_EMPLOYEE}");

        // Start loop in separate thread
        await Task.Run(() => ConsumeLoop(stoppingToken), stoppingToken);
    }

    private async Task ConsumeLoop(CancellationToken stoppingToken)
    {
        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<string, string>? consumeResult = null;
            try
            {
                // Wait message (timeout 1 sec to check stoppingToken)
                consumeResult = _consumer.Consume(TimeSpan.FromSeconds(1));

                if (consumeResult == null || consumeResult.IsPartitionEOF) continue;

                _logger.LogInformation($"Received message from partition {consumeResult.Partition} with offset {consumeResult.Offset}");

                await ProcessBusinessLogic(consumeResult.Message.Value);

                _consumer.Commit(consumeResult);

                _logger.LogInformation($"Successfully processed and committed offset: {consumeResult.Offset}");
            }
            catch (ConsumeException e)
            {
                _logger.LogError($"Kafka consume error: {e.Error.Reason}");
            }
            catch (JsonException ex)
            {
                _logger.LogError($"JSON Deserialization error: {ex.Message}. Message moved to DLQ.");
                if (consumeResult != null)
                {
                    await _dlqService.SendToDlqAsync(consumeResult, "[DLQ] Deserrialization error (JSON).", ex);

                    _consumer.Commit(consumeResult);
                }
            }
            catch (NotSupportedException ex)
            {
                _logger.LogWarning($"Skip unknown message: {ex.Message}");
                if (consumeResult != null)
                {
                    await _dlqService.SendToDlqAsync(consumeResult, "[DLQ] Unsupported message type", ex);

                    _consumer.Commit(consumeResult);
                }
            }
            catch (Exception e)
            {
                _logger.LogError($"Critical error during business processing: {e.Message}");
                await Task.Delay(2000, stoppingToken);
            }
        }
    }

    private async Task ProcessBusinessLogic(string rawJson)
    {
        if (string.IsNullOrEmpty(rawJson))
        {
            _logger.LogError($"Processing client data is null");
            return;
        }

        var handler = _handlerFactory.GetHandler(rawJson);

        await handler.HandleAsync(rawJson);

        _logger.LogInformation($"Processing client data created at: {rawJson}");
    }

    public override void Dispose()
    {
        _consumer.Close();
        _consumer.Dispose();
        base.Dispose();
    }
}

