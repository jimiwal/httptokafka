using Aspire.Hosting;
using Aspire.Hosting.Kafka;

var builder = DistributedApplication.CreateBuilder(args);

var kafka = builder.AddKafka("kafka")
                   .WithKafkaUI();

builder.AddProject<Projects.HttpToKafka_Api>("httptokafka-api")
       .WithReference(kafka)
       .WithEnvironment("Kafka__SecurityProtocol","")
       .WithEnvironment("Kafka__SaslMechanism","")
       .WithEnvironment("Kafka__SaslUsername","")
       .WithEnvironment("Kafka__SaslPassword","");

builder.Build().Run();
