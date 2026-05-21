var builder = DistributedApplication.CreateBuilder(args);


/*var kafka = builder.AddKafka("kafka", 9092)
.WithDataVolume()
.WithKafkaUI();

builder.AddProject("demoproject")
.WithReference(kafka)
.WithEnvironment("BrokerList", "localhost:9092");*/

var kafka = builder.AddKafka("kafka", 9092)
.WithDataVolume()
.WithKafkaUI();

builder.AddAzureFunctionsProject<Projects.AzureFunction>("azurefunction")
    .WithReference(kafka)
.WithEnvironment("BrokerList", "localhost:9092"); ;

var producer = builder.AddProject<Projects.KafkaProducer>("kafka-producer")
    .WithReference(kafka);

var consumer = builder.AddProject<Projects.KafkaConsumer>("kafka-consumer")
    .WithReference(kafka);

/*var kafka = builder.AddKafka("kafka", 9092)
.WithDataVolume()
.WithKafkaUI();

builder.AddProject("demoproject")
.WithReference(kafka)
.WithEnvironment("BrokerList", "localhost:9092");*/

builder.Build().Run();
