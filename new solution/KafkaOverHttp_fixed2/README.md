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
