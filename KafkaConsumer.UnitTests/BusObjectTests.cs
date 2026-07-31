using CommonModels.BusEntity;
using FluentAssertions;

namespace KafkaConsumer.UnitTests;

public class BusObjectTests
{
    [Fact]
    public void Constructor_SetsOnCreate()
    {
        var now = DateTime.UtcNow;
        var contact = new ContactEmployee("123");

        var busObject = new BusObject<ContactEmployee>(now, contact);

        busObject.OnCreate.Should().Be(now);
    }

    [Fact]
    public void Constructor_SetsKafkaObject()
    {
        var contact = new ContactEmployee("456") { IsEmployee = true };

        var busObject = new BusObject<ContactEmployee>(DateTime.UtcNow, contact);

        busObject.KafkaObject.Should().Be(contact);
        busObject.KafkaObject!.Telephone1.Should().Be("456");
    }

    [Fact]
    public void Constructor_SetsMessageTypeToTypeName()
    {
        var busObject = new BusObject<ContactEmployee>(DateTime.UtcNow, new ContactEmployee("1"));

        busObject.MessageType.Should().Be("ContactEmployee");
    }

    [Fact]
    public void Constructor_MessageType_ReflectsGenericArgument()
    {
        var busObject = new BusObject<ContactCustomer>(DateTime.UtcNow, new ContactCustomer("2") { IsMono = true });

        busObject.MessageType.Should().Be("ContactCustomer");
    }
}
