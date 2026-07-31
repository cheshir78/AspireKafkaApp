using Confluent.Kafka;
using CommonModels.BusEntity;
using CommonModels.Constants;
using FluentAssertions;
using System.Text;
using System.Text.Json;
using Testcontainers.Kafka;

namespace AspireKafkaApp.IntegrationTests;

/// <summary>
/// End-to-end integration tests that spin up a real Kafka broker via Testcontainers.
/// These tests verify producer serialization, consumer deserialization, and DLQ routing.
/// </summary>
[Trait("Category", "Integration")]
public class KafkaProducerConsumerIntegrationTests : IAsyncLifetime
{
    private readonly KafkaContainer _kafka = new KafkaBuilder()
        .WithImage("confluentinc/cp-kafka:7.4.0")
        .Build();

    public Task InitializeAsync() => _kafka.StartAsync();

    public Task DisposeAsync() => _kafka.DisposeAsync().AsTask();

    // ------------------------------------------------------------------ //
    // Producer → Consumer round-trip
    // ------------------------------------------------------------------ //

    [Fact]
    public async Task ProducedContactEmployee_IsConsumedAndDeserialized()
    {
        const string topic = "it-employee-topic";
        var (producer, consumer) = CreateClients(topic, "it-employee-group");

        var contact = new ContactEmployee("12345678") { IsEmployee = true };
        var busObject = new BusObject<ContactEmployee>(DateTime.UtcNow, contact);
        var json = JsonSerializer.Serialize(busObject);

        // Produce
        var delivery = await producer.ProduceAsync(topic, new Message<string, string> { Value = json });
        delivery.Status.Should().Be(PersistenceStatus.Persisted);

        // Consume
        var result = consumer.Consume(TimeSpan.FromSeconds(15));
        result.Should().NotBeNull();

        var deserialized = JsonSerializer.Deserialize<BusObject<ContactEmployee>>(result.Message.Value);
        deserialized.Should().NotBeNull();
        deserialized!.MessageType.Should().Be("ContactEmployee");
        deserialized.KafkaObject!.Telephone1.Should().Be("12345678");
        deserialized.KafkaObject.IsEmployee.Should().BeTrue();
    }

    [Fact]
    public async Task ProducedContactCustomer_IsConsumedAndDeserialized()
    {
        const string topic = "it-customer-topic";
        var (producer, consumer) = CreateClients(topic, "it-customer-group");

        var contact = new ContactCustomer("987654321") { IsMono = true, IsEmployee = false };
        var busObject = new BusObject<ContactCustomer>(DateTime.UtcNow, contact);
        var json = JsonSerializer.Serialize(busObject);

        await producer.ProduceAsync(topic, new Message<string, string> { Value = json });

        var result = consumer.Consume(TimeSpan.FromSeconds(15));
        result.Should().NotBeNull();

        var deserialized = JsonSerializer.Deserialize<BusObject<ContactCustomer>>(result.Message.Value);
        deserialized.Should().NotBeNull();
        deserialized!.MessageType.Should().Be("ContactCustomer");
        deserialized.KafkaObject!.Telephone1.Should().Be("987654321");
        deserialized.KafkaObject.IsMono.Should().BeTrue();
        deserialized.KafkaObject.IsEmployee.Should().BeFalse();
    }

    // ------------------------------------------------------------------ //
    // Ordering
    // ------------------------------------------------------------------ //

    [Fact]
    public async Task MultipleMessages_AreConsumedInProducedOrder()
    {
        const string topic = "it-ordered-topic";
        var (producer, consumer) = CreateClients(topic, "it-ordered-group");

        var phones = new[] { "111", "222", "333" };

        foreach (var phone in phones)
        {
            var obj = new BusObject<ContactEmployee>(DateTime.UtcNow, new ContactEmployee(phone));
            await producer.ProduceAsync(topic, new Message<string, string> { Value = JsonSerializer.Serialize(obj) });
        }

        var received = new List<string>();
        for (int i = 0; i < phones.Length; i++)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(15));
            result.Should().NotBeNull();
            var deserialized = JsonSerializer.Deserialize<BusObject<ContactEmployee>>(result.Message.Value);
            received.Add(deserialized!.KafkaObject!.Telephone1);
        }

        received.Should().Equal(phones);
    }

    // ------------------------------------------------------------------ //
    // DLQ routing simulation
    // ------------------------------------------------------------------ //

    [Fact]
    public async Task InvalidMessage_CanBeForwardedToDlqWithHeaders()
    {
        const string mainTopic = "it-dlq-main";
        const string dlqTopic = BrokerNames.DLQ_CUSTOMER_EMPLOYEE;
        var (producer, mainConsumer) = CreateClients(mainTopic, "it-dlq-main-group");
        var (_, dlqConsumer) = CreateClients(dlqTopic, "it-dlq-consumer-group");

        const string brokenJson = "this-is-not-valid-json";
        const string dlqReason = "[DLQ] Deserialization error (JSON).";

        // Produce broken message to main topic
        await producer.ProduceAsync(mainTopic, new Message<string, string> { Value = brokenJson });

        // "Consumer" reads it and forwards to DLQ (simulating DlqService behaviour)
        var badMsg = mainConsumer.Consume(TimeSpan.FromSeconds(15));
        badMsg.Should().NotBeNull();

        var dlqHeaders = new Headers();
        dlqHeaders.Add("dlq-reason", Encoding.UTF8.GetBytes(dlqReason));
        dlqHeaders.Add("dlq-exception", Encoding.UTF8.GetBytes("JsonException"));
        dlqHeaders.Add("dlq-original-partition", Encoding.UTF8.GetBytes(badMsg.Partition.Value.ToString()));
        dlqHeaders.Add("dlq-original-offset", Encoding.UTF8.GetBytes(badMsg.Offset.Value.ToString()));

        await producer.ProduceAsync(dlqTopic, new Message<string, string>
        {
            Key = badMsg.Message.Key,
            Value = badMsg.Message.Value,
            Headers = dlqHeaders
        });

        // Assert DLQ consumer receives the message with headers
        var dlqResult = dlqConsumer.Consume(TimeSpan.FromSeconds(15));
        dlqResult.Should().NotBeNull();
        dlqResult.Message.Value.Should().Be(brokenJson);

        var reasonHeader = dlqResult.Message.Headers.FirstOrDefault(h => h.Key == "dlq-reason");
        reasonHeader.Should().NotBeNull();
        Encoding.UTF8.GetString(reasonHeader!.GetValueBytes()).Should().Be(dlqReason);
    }

    // ------------------------------------------------------------------ //
    // MessageType discriminator
    // ------------------------------------------------------------------ //

    [Fact]
    public async Task BusObject_MessageType_CorrectlyDiscriminatesType()
    {
        const string topic = "it-discriminator-topic";
        var (producer, consumer) = CreateClients(topic, "it-discriminator-group");

        var employee = JsonSerializer.Serialize(
            new BusObject<ContactEmployee>(DateTime.UtcNow, new ContactEmployee("111")));
        var customer = JsonSerializer.Serialize(
            new BusObject<ContactCustomer>(DateTime.UtcNow, new ContactCustomer("222")));

        await producer.ProduceAsync(topic, new Message<string, string> { Value = employee });
        await producer.ProduceAsync(topic, new Message<string, string> { Value = customer });

        var results = new List<string>();
        for (int i = 0; i < 2; i++)
        {
            var result = consumer.Consume(TimeSpan.FromSeconds(15));
            result.Should().NotBeNull();
            using var doc = System.Text.Json.JsonDocument.Parse(result.Message.Value);
            results.Add(doc.RootElement.GetProperty("MessageType").GetString()!);
        }

        results.Should().BeEquivalentTo(new[] { "ContactEmployee", "ContactCustomer" },
            opts => opts.WithStrictOrdering());
    }

    // ------------------------------------------------------------------ //
    // Helpers
    // ------------------------------------------------------------------ //

    private (IProducer<string, string> producer, IConsumer<string, string> consumer) CreateClients(
        string topic, string groupId)
    {
        var bootstrapServers = _kafka.GetBootstrapAddress();

        var producer = new ProducerBuilder<string, string>(
            new ProducerConfig { BootstrapServers = bootstrapServers })
            .Build();

        var consumer = new ConsumerBuilder<string, string>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = groupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false
        }).Build();

        consumer.Subscribe(topic);
        return (producer, consumer);
    }
}
