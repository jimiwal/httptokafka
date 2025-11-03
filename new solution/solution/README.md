# KafkaOverHttp (Visual Studio Solution)

## Wymagania
- .NET 8 SDK
- Confluent Kafka broker (BootstrapServers w `appsettings.json`)
- (Opcjonalnie) Confluent Schema Registry dla Avro

## Uruchomienie (Visual Studio)
1. Otwórz `KafkaOverHttp.sln` w Visual Studio 2022+.
2. Ustaw projekt `KafkaOverHttp` jako startowy.
3. Zaktualizuj `src/KafkaOverHttp/appsettings.json` dla swojej instancji Kafki.
4. Uruchom (F5). Swagger pod `/swagger`.

## CLI
```
dotnet restore
dotnet build
dotnet run --project src/KafkaOverHttp/KafkaOverHttp.csproj
```


## Docker
docker run -d --name kafka \
  --network kafka-net \
  -p 9092:9092 \
  -e KAFKA_NODE_ID=1 \
  -e CLUSTER_ID=mkClusterId-000000000000 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://localhost:9092 \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT \
  confluentinc/cp-kafka:7.6.1
