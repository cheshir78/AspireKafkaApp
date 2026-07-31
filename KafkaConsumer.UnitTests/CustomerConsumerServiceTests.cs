using Confluent.Kafka;
using CommonModels.Constants;
using CommonModels.Contracts;
using FluentAssertions;
using KafkaConsumer.Interfaces;
using Microsoft.Extensions.Logging;
using Moq;

namespace KafkaConsumer.UnitTests;

public class CustomerConsumerServiceTests
{
    private readonly Mock<IConsumer<string, string>> _consumerMock = new();
    private readonly Mock<IDlqService> _dlqServiceMock = new();
    private readonly Mock<IBusObjectHandlerFactory> _factoryMock = new();
    private readonly Mock<ILogger<CustomerConsumerService>> _loggerMock = new();

    private CustomerConsumerService CreateSut() =>
        new(_consumerMock.Object, _dlqServiceMock.Object, _factoryMock.Object, _loggerMock.Object);

    [Fact]
    public async Task StartAsync_SubscribesToCustomerEmployeeTopic()
    {
        // Arrange
        var sut = CreateSut();
        _consumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
            .Returns(() => { Thread.Sleep(20); return null!; });

        // Act
        await sut.StartAsync(CancellationToken.None);
        await Task.Delay(100);
        await sut.StopAsync(CancellationToken.None);

        // Assert
        _consumerMock.Verify(c => c.Subscribe(BrokerNames.CUSTOMER_EMPLOYEE), Times.Once);
    }

    [Fact]
    public async Task ConsumeLoop_SuccessfulMessage_CallsHandlerAndCommits()
    {
        // Arrange
        const string json = """{"MessageType":"ContactEmployee","OnCreate":"2024-01-01T00:00:00Z","KafkaObject":{"Telephone1":"12345","IsEmployee":true}}""";
        var consumeResult = CreateConsumeResult(json);
        var handlerMock = new Mock<IBusObjectHandler>();
        handlerMock.Setup(h => h.HandleAsync(json)).Returns(Task.CompletedTask);
        _factoryMock.Setup(f => f.GetHandler(json)).Returns(handlerMock.Object);

        var commitTcs = new TaskCompletionSource<bool>();
        _consumerMock.Setup(c => c.Commit(consumeResult)).Callback(() => commitTcs.TrySetResult(true));

        int callCount = 0;
        _consumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(() =>
        {
            if (++callCount == 1) return consumeResult;
            Thread.Sleep(20);
            return null!;
        });

        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act - wait for commit signal
        var committed = await Task.WhenAny(commitTcs.Task, Task.Delay(TimeSpan.FromSeconds(5))) == commitTcs.Task;
        await sut.StopAsync(CancellationToken.None);

        // Assert
        committed.Should().BeTrue("message should have been processed and committed");
        handlerMock.Verify(h => h.HandleAsync(json), Times.Once);
        _consumerMock.Verify(c => c.Commit(consumeResult), Times.Once);
    }

    [Fact]
    public async Task ConsumeLoop_JsonException_SendsToDlqAndCommits()
    {
        // Arrange
        const string invalidJson = "not-valid-json {{";
        var consumeResult = CreateConsumeResult(invalidJson);

        var dlqTcs = new TaskCompletionSource<bool>();
        _dlqServiceMock
            .Setup(d => d.SendToDlqAsync(consumeResult, It.IsAny<string>(), It.IsAny<Exception>()))
            .Callback(() => dlqTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        _factoryMock.Setup(f => f.GetHandler(invalidJson)).Throws<System.Text.Json.JsonException>();

        int callCount = 0;
        _consumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(() =>
        {
            if (++callCount == 1) return consumeResult;
            Thread.Sleep(20);
            return null!;
        });

        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act
        var sentToDlq = await Task.WhenAny(dlqTcs.Task, Task.Delay(TimeSpan.FromSeconds(5))) == dlqTcs.Task;
        await sut.StopAsync(CancellationToken.None);

        // Assert
        sentToDlq.Should().BeTrue("bad JSON message should be routed to DLQ");
        _dlqServiceMock.Verify(d => d.SendToDlqAsync(consumeResult, It.IsAny<string>(), It.IsAny<Exception>()), Times.Once);
        _consumerMock.Verify(c => c.Commit(consumeResult), Times.Once);
    }

    [Fact]
    public async Task ConsumeLoop_NotSupportedException_SendsToDlqAndCommits()
    {
        // Arrange
        const string json = """{"MessageType":"UnknownType"}""";
        var consumeResult = CreateConsumeResult(json);

        var dlqTcs = new TaskCompletionSource<bool>();
        _dlqServiceMock
            .Setup(d => d.SendToDlqAsync(consumeResult, It.IsAny<string>(), It.IsAny<Exception>()))
            .Callback(() => dlqTcs.TrySetResult(true))
            .Returns(Task.CompletedTask);

        _factoryMock.Setup(f => f.GetHandler(json)).Throws<NotSupportedException>();

        int callCount = 0;
        _consumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(() =>
        {
            if (++callCount == 1) return consumeResult;
            Thread.Sleep(20);
            return null!;
        });

        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);

        // Act
        var sentToDlq = await Task.WhenAny(dlqTcs.Task, Task.Delay(TimeSpan.FromSeconds(5))) == dlqTcs.Task;
        await sut.StopAsync(CancellationToken.None);

        // Assert
        sentToDlq.Should().BeTrue("unsupported message type should be routed to DLQ");
        _dlqServiceMock.Verify(d => d.SendToDlqAsync(consumeResult, It.IsAny<string>(), It.IsAny<Exception>()), Times.Once);
        _consumerMock.Verify(c => c.Commit(consumeResult), Times.Once);
    }

    [Fact]
    public async Task ConsumeLoop_NullResult_IsSkippedWithoutProcessing()
    {
        // Arrange
        int consumeCallCount = 0;
        _consumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>())).Returns(() =>
        {
            consumeCallCount++;
            Thread.Sleep(20);
            return null!;
        });

        var sut = CreateSut();
        await sut.StartAsync(CancellationToken.None);
        await Task.Delay(200);
        await sut.StopAsync(CancellationToken.None);

        // Assert - handler should never be called for null results
        _factoryMock.Verify(f => f.GetHandler(It.IsAny<string>()), Times.Never);
        _consumerMock.Verify(c => c.Commit(It.IsAny<ConsumeResult<string, string>>()), Times.Never);
    }

    [Fact]
    public async Task Dispose_ClosesAndDisposesConsumer()
    {
        var sut = CreateSut();
        _consumerMock.Setup(c => c.Consume(It.IsAny<TimeSpan>()))
            .Returns(() => { Thread.Sleep(20); return null!; });

        await sut.StartAsync(CancellationToken.None);
        await sut.StopAsync(CancellationToken.None);
        sut.Dispose();

        _consumerMock.Verify(c => c.Close(), Times.Once);
        _consumerMock.Verify(c => c.Dispose(), Times.Once);
    }

    private static ConsumeResult<string, string> CreateConsumeResult(string value) =>
        new()
        {
            Message = new Message<string, string>
            {
                Key = "test-key",
                Value = value,
                Headers = new Headers()
            },
            TopicPartitionOffset = new TopicPartitionOffset(BrokerNames.CUSTOMER_EMPLOYEE, 0, 1)
        };
}
