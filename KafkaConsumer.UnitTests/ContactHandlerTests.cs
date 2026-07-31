using CommonModels.BusEntity;
using FluentAssertions;
using KafkaConsumer.Handlers;
using Microsoft.Extensions.Logging;
using Moq;
using System.Text.Json;

namespace KafkaConsumer.UnitTests;

public class ContactHandlerTests
{
    [Theory]
    [InlineData("ContactEmployee", true)]
    [InlineData("contactemployee", true)]
    [InlineData("CONTACTEMPLOYEE", true)]
    [InlineData("ContactCustomer", false)]
    [InlineData("", false)]
    [InlineData("Unknown", false)]
    public void ContactEmployeeHandler_CanHandle_ReturnsExpected(string messageType, bool expected)
    {
        var handler = new ContactEmployeeHandler(Mock.Of<ILogger<ContactEmployeeHandler>>());

        handler.CanHandle(messageType).Should().Be(expected);
    }

    [Theory]
    [InlineData("ContactCustomer", true)]
    [InlineData("contactcustomer", true)]
    [InlineData("CONTACTCUSTOMER", true)]
    [InlineData("ContactEmployee", false)]
    [InlineData("", false)]
    public void ContactPartnerHandler_CanHandle_ReturnsExpected(string messageType, bool expected)
    {
        var handler = new ContactPartnerHandler(Mock.Of<ILogger<ContactPartnerHandler>>());

        handler.CanHandle(messageType).Should().Be(expected);
    }

    [Fact]
    public async Task ContactEmployeeHandler_HandleAsync_DoesNotThrow()
    {
        var handler = new ContactEmployeeHandler(Mock.Of<ILogger<ContactEmployeeHandler>>());
        var busObject = new BusObject<ContactEmployee>(DateTime.UtcNow, new ContactEmployee("12345") { IsEmployee = true });
        var json = JsonSerializer.Serialize(busObject);

        var act = async () => await handler.HandleAsync(json);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ContactPartnerHandler_HandleAsync_DoesNotThrow()
    {
        var handler = new ContactPartnerHandler(Mock.Of<ILogger<ContactPartnerHandler>>());
        var busObject = new BusObject<ContactCustomer>(DateTime.UtcNow, new ContactCustomer("987654") { IsMono = true });
        var json = JsonSerializer.Serialize(busObject);

        var act = async () => await handler.HandleAsync(json);

        await act.Should().NotThrowAsync();
    }

    [Fact]
    public async Task ContactEmployeeHandler_HandleAsync_InvalidJson_ThrowsJsonException()
    {
        var handler = new ContactEmployeeHandler(Mock.Of<ILogger<ContactEmployeeHandler>>());

        var act = async () => await handler.HandleAsync("{not valid");

        await act.Should().ThrowAsync<JsonException>();
    }
}
