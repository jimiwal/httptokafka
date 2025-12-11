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


## Yaml
services:
  broker:
    image: apache/kafka:latest
    hostname: broker
    container_name: broker
    ports:
      - 9092:9092
    environment:
      KAFKA_BROKER_ID: 1
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT,CONTROLLER:PLAINTEXT
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://broker:29092,PLAINTEXT_HOST://localhost:9092
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_NODE_ID: 1
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@broker:29093
      KAFKA_LISTENERS: PLAINTEXT://broker:29092,CONTROLLER://broker:29093,PLAINTEXT_HOST://0.0.0.0:9092
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk






      # Ścieżka do katalogu
# Ścieżka do katalogu głównego
$folder = "C:\Twoj\Katalog"

# Whitelist rozszerzeń
$whitelist = @("xml", "json", "css", "txt")

Get-ChildItem -Path $folder -File -Recurse | Where-Object {
    $_.Extension.TrimStart(".") -in $whitelist
} | ForEach-Object {
    
    $originalContent = Get-Content $_.FullName -Raw
    $singleLine = $originalContent -replace "`r`n|`n|`r", ""

    # Sprawdzenie, czy coś się zmieniło
    if ($originalContent -ne $singleLine) {
        Set-Content -Path $_.FullName -Value $singleLine
        Write-Host "[ZMIENIONO] $($_.FullName)"
    }
}


