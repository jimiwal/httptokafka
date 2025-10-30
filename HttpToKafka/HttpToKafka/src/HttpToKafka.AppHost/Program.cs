using Aspire.Hosting;

var builder = DistributedApplication.CreateBuilder(args);

var network = builder.AddNetwork("devnet");

var redpanda = builder.AddContainer("redpanda", "redpandadata/redpanda:latest")
    .WithNetwork(network)
    .WithEndpoint(targetPort: 19092, containerPort: 19092, name: "kafka")
    .WithArgs("start", "--overprovisioned", "--smp", "1", "--reserve-memory", "0M", "--node-id", "0",
              "--kafka-addr", "PLAINTEXT://0.0.0.0:19092", "--advertise-kafka-addr", "PLAINTEXT://redpanda:19092")
    .WithEnvironment("REDPANDA_AUTO_CREATE_TOPICS", "true");

var schemaRegistry = builder.AddContainer("schemaregistry", "confluentinc/cp-schema-registry:7.6.0")
    .WithNetwork(network)
    .WithEndpoint(targetPort: 18081, containerPort: 8081, name: "sr")
    .WithEnvironment("SCHEMA_REGISTRY_HOST_NAME", "schemaregistry")
    .WithEnvironment("SCHEMA_REGISTRY_LISTENERS", "http://0.0.0.0:8081")
    .WithEnvironment("SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS", "PLAINTEXT://redpanda:19092");

var api = builder.AddProject<Projects.HttpToKafka_Api>("api")
    .WithReference(redpanda)
    .WithReference(schemaRegistry)
    .WithEnvironment("Kafka__BootstrapServers", "redpanda:19092")
    .WithEnvironment("Kafka__SchemaRegistryUrl", "http://schemaregistry:8081")
    .WithEndpoint(targetPort: 5088, name: "http");

builder.Build().Run();
