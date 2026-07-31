using CommonModels.Contracts;
using FluentAssertions;
using KafkaConsumer.Handlers;
using Moq;

namespace KafkaConsumer.UnitTests;

public class BusObjectHandlerFactoryTests
{
    private readonly Mock<IBusObjectHandler> _employeeHandlerMock;
    private readonly BusObjectHandlerFactory _factory;

    public BusObjectHandlerFactoryTests()
    {
        _employeeHandlerMock = new Mock<IBusObjectHandler>();
        _employeeHandlerMock.Setup(h => h.CanHandle("ContactEmployee")).Returns(true);
        _employeeHandlerMock.Setup(h => h.CanHandle(It.Is<string>(s => s != "ContactEmployee"))).Returns(false);

        _factory = new BusObjectHandlerFactory(new[] { _employeeHandlerMock.Object });
    }

    [Fact]
    public void GetHandler_KnownMessageType_ReturnsMatchingHandler()
    {
        var json = """{"MessageType":"ContactEmployee","OnCreate":"2024-01-01T00:00:00Z"}""";

        var handler = _factory.GetHandler(json);

        handler.Should().Be(_employeeHandlerMock.Object);
    }

    [Fact]
    public void GetHandler_UnknownMessageType_ThrowsNotSupportedException()
    {
        var json = """{"MessageType":"UnknownType"}""";

        var act = () => _factory.GetHandler(json);

        act.Should().Throw<NotSupportedException>()
           .WithMessage("*No suitable handler*");
    }

    [Fact]
    public void GetHandler_MissingMessageTypeField_ThrowsNotSupportedException()
    {
        var json = """{"SomeOtherField":"value"}""";

        var act = () => _factory.GetHandler(json);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void GetHandler_EmptyMessageType_ThrowsNotSupportedException()
    {
        var json = """{"MessageType":""}""";

        var act = () => _factory.GetHandler(json);

        act.Should().Throw<NotSupportedException>();
    }

    [Fact]
    public void GetHandler_InvalidJson_ThrowsJsonException()
    {
        var act = () => _factory.GetHandler("not valid json {{");

        act.Should().Throw<System.Text.Json.JsonException>();
    }
}
