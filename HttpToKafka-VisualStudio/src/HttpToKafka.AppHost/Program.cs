using Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

// Zookeeper
var zk = builder.AddContainer("zookeeper", "confluentinc/cp-zookeeper:7.6.1")
    .WithEnvironment("ZOOKEEPER_CLIENT_PORT", "2181")
    .WithEndpoint(targetPort: 2181, name: "zk");

// Kafka
var kafka = builder.AddContainer("kafka", "confluentinc/cp-kafka:7.6.1")
    .WithEnvironment("KAFKA_BROKER_ID","1")
    .WithEnvironment("KAFKA_ZOOKEEPER_CONNECT","zookeeper:2181")
    .WithEnvironment("KAFKA_ADVERTISED_LISTENERS","PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:9092")
    .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP","PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT")
    .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME","PLAINTEXT")
    .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR","1")
    .WithReference(zk)
    .WithEndpoint(targetPort: 9092, name: "kafka");

// Schema Registry
var sr = builder.AddContainer("schema-registry", "confluentinc/cp-schema-registry:7.6.1")
    .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schema-registry")
    .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://kafka:9092")
    .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    .WithReference(kafka)
    .WithEndpoint(targetPort: 8081, name: "sr");

// Kafka UI
builder.AddContainer("kafka-ui", "provectuslabs/kafka-ui:latest")
    .WithEnvironment("KAFKA_CLUSTERS_0_NAME","local")
    .WithEnvironment("KAFKA_CLUSTERS_0_BOOTSTRAPSERVERS","kafka:9092")
    .WithEnvironment("KAFKA_CLUSTERS_0_SCHEMAREGISTRY","http://schema-registry:8081")
    .WithReference(kafka).WithReference(sr)
    .WithEndpoint(targetPort: 8080, name: "ui");

// API
builder.AddProject<Projects.HttpToKafka_Api>("httpToKafkaApi")
    .WithEnvironment("Kafka__BootstrapServers","localhost:9092")
    .WithEnvironment("Kafka__SecurityProtocol","PLAINTEXT")
    .WithEnvironment("SchemaRegistry__Url","http://localhost:8081")
    .WithReference(kafka).WithReference(sr);

builder.Build().Run();
