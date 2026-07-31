using Confluent.Kafka;
using FluentAssertions;
using KafkaConsumer.Messaging;
using Moq;
using System.Text;

namespace KafkaConsumer.UnitTests;

public class DlqServiceTests
{
    private readonly Mock<IProducer<string, string>> _producerMock = new();
    private Message<string, string>? _capturedMessage;

    public DlqServiceTests()
    {
        _producerMock
            .Setup(p => p.ProduceAsync(
                It.IsAny<string>(),
                It.IsAny<Message<string, string>>(),
                It.IsAny<CancellationToken>()))
            .Callback<string, Message<string, string>, CancellationToken>((_, msg, _) => _capturedMessage = msg)
            .ReturnsAsync(new DeliveryResult<string, string> { Status = PersistenceStatus.Persisted });
    }

    private DlqService CreateSut() => new(_producerMock.Object);

    [Fact]
    public async Task SendToDlqAsync_ProducesToDlqTopic()
    {
        var sut = CreateSut();
        var consumeResult = CreateConsumeResult("key", "value", 0, 1);

        await sut.SendToDlqAsync(consumeResult, "test reason");

        _producerMock.Verify(p => p.ProduceAsync(
            CommonModels.Constants.BrokerNames.DLQ_CUSTOMER_EMPLOYEE,
            It.IsAny<Message<string, string>>(),
            It.IsAny<CancellationToken>()), Times.Once);
    }

    [Fact]
    public async Task SendToDlqAsync_AddsReasonHeader()
    {
        const string reason = "deserialization error";
        var sut = CreateSut();

        await sut.SendToDlqAsync(CreateConsumeResult("key", "value", 0, 1), reason);

        GetHeader("dlq-reason").Should().Be(reason);
    }

    [Fact]
    public async Task SendToDlqAsync_AddsExceptionMessageHeader()
    {
        var exception = new InvalidOperationException("oops");
        var sut = CreateSut();

        await sut.SendToDlqAsync(CreateConsumeResult("key", "value", 0, 1), "reason", exception);

        GetHeader("dlq-exception").Should().Be(exception.Message);
    }

    [Fact]
    public async Task SendToDlqAsync_WhenNoException_AddsNoneToExceptionHeader()
    {
        var sut = CreateSut();

        await sut.SendToDlqAsync(CreateConsumeResult("key", "value", 0, 1), "reason");

        GetHeader("dlq-exception").Should().Be("None");
    }

    [Fact]
    public async Task SendToDlqAsync_AddsOriginalPartitionHeader()
    {
        var sut = CreateSut();

        await sut.SendToDlqAsync(CreateConsumeResult("key", "value", partition: 2, offset: 77), "reason");

        GetHeader("dlq-original-partition").Should().Be("2");
    }

    [Fact]
    public async Task SendToDlqAsync_AddsOriginalOffsetHeader()
    {
        var sut = CreateSut();

        await sut.SendToDlqAsync(CreateConsumeResult("key", "value", partition: 2, offset: 77), "reason");

        GetHeader("dlq-original-offset").Should().Be("77");
    }

    [Fact]
    public async Task SendToDlqAsync_PreservesOriginalMessageValue()
    {
        const string original = """{"MessageType":"ContactEmployee"}""";
        var sut = CreateSut();

        await sut.SendToDlqAsync(CreateConsumeResult("key", original, 0, 1), "reason");

        _capturedMessage!.Value.Should().Be(original);
    }

    [Fact]
    public async Task SendToDlqAsync_PreservesOriginalMessageKey()
    {
        var sut = CreateSut();

        await sut.SendToDlqAsync(CreateConsumeResult("original-key", "value", 0, 1), "reason");

        _capturedMessage!.Key.Should().Be("original-key");
    }

    private static ConsumeResult<string, string> CreateConsumeResult(string key, string value, int partition, long offset) =>
        new()
        {
            Message = new Message<string, string>
            {
                Key = key,
                Value = value,
                Headers = new Headers()
            },
            TopicPartitionOffset = new TopicPartitionOffset(
                CommonModels.Constants.BrokerNames.CUSTOMER_EMPLOYEE, partition, offset)
        };

    private string GetHeader(string key)
    {
        var header = _capturedMessage?.Headers.FirstOrDefault(h => h.Key == key);
        return header is not null ? Encoding.UTF8.GetString(header.GetValueBytes()) : string.Empty;
    }
}
