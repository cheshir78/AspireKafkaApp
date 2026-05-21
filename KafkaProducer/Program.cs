using KafkaProducer;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

using var loggerFactory = LoggerFactory.Create(builder =>
{
    builder.AddConsole();
});

Console.WriteLine("Hello, World!");
var builder = Host.CreateApplicationBuilder(args);

builder.AddKafkaProducer<string, string>("kafka");

builder.Services.AddTransient<CustomerProducer>();

using var host = builder.Build();

var runner = host.Services.GetRequiredService<CustomerProducer>();
await runner.RunAsync();
