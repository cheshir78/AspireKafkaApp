using CommonModels.Constants;
using CommonModels.Contracts;
using Confluent.Kafka;
using KafkaConsumer.Handlers;
using KafkaConsumer.Interfaces;
using KafkaConsumer.Messaging;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

Console.WriteLine("Consumer Service Starting...");

var builder = Host.CreateApplicationBuilder(args);

// 1. Configuration KafkaConsumer with garantee delivery

builder.AddKafkaConsumer<string, string>("kafka", options =>
{
    // 1. Unique groupId
    options.Config.GroupId = BrokerNames.CUSTOMER_EMPLOYEE_GROUP2;

    // 2. Disable autocommit
    options.Config.EnableAutoCommit = false;

    // 3. Read from first messages
    options.Config.AutoOffsetReset = AutoOffsetReset.Earliest;

    options.Config.EnablePartitionEof = true;
});

builder.AddKafkaProducer<string, string>("kafka");

// Register service
builder.Services.AddHostedService<CustomerConsumerService>();

builder.Services.AddTransient<IBusObjectHandler, ContactEmployeeHandler>();
builder.Services.AddTransient<IBusObjectHandler, ContactPartnerHandler>();

// Register HandlerFactory and DLQProducer
builder.Services.AddSingleton<IBusObjectHandlerFactory, BusObjectHandlerFactory>();
builder.Services.AddSingleton<IDlqService, DlqService>();

using var host = builder.Build();
await host.RunAsync();
