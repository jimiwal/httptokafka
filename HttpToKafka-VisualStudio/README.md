# HttpToKafka (Visual Studio Solution)

Kompletna solucja .NET 8: Web API + integracja z Confluent Kafka + Schema Registry + subskrypcje (webhook z HMAC) + UTesty + projekt Aspire (Kafka/ZK/SR/Kafka UI).

## Wymagania
- Visual Studio 2022 (17.9+) z .NET 8 SDK
- Docker Desktop
- (opcjonalnie) .NET Aspire: `dotnet workload install aspire`

## Jak uruchomić (dev)
1. Otwórz `HttpToKafka.sln` w Visual Studio.
2. Ustaw projekt startowy:
   - `HttpToKafka.AppHost` (Aspire) – uruchomi lokalnie Kafka + SR + UI + API
   - lub bez Aspire: włącz ręcznie klastry (np. docker-compose) i uruchom `HttpToKafka.Api`.
3. `F5` – po starcie:
   - Swagger API: https://localhost:7170/swagger
   - Kafka UI:   http://localhost:8080
   - SchemaRegistry: http://localhost:8081

> Uwaga: Endpointy są chronione JWT (OIDC). W dev możesz tymczasowo usunąć atrybut `[Authorize]` z kontrolerów lub skonfigurować Authority/Audience.

## Kluczowe endpointy
- `POST /produce` – publikowanie (Sync/Async), wsad, Bytes/Avro/JSON/Protobuf.
- `POST /subscribe/sync` – czekanie na 1 wiadomość (z `fromNow`).
- `POST /subscriptions` – subskrypcja async (webhook + HMAC + dead‑letter).
- `GET /subscriptions/{id}` – status + metryki.
- `GET /kafka/config` – konfiguracja (z redakcją sekretów).
- `GET /kafka/topics`, `GET /kafka/topics/{topic}` – lista/opis.

## Testy
Uruchom `Test Explorer`. Test integracyjny producenta jest **wyłączony** (wymaga uruchomionej Kafki przez AppHost).

## Konfiguracja
Zmieniaj w `appsettings.json` (BootstrapServers, Schema Registry, sekrety HMAC, OIDC).

## Uwaga o kolejności
Kafka gwarantuje kolejność **per partycja**. Aby zachować kolejność wsadową, ustawiaj klucz (`Key`) tak, by wszystkie wiadomości trafiły do tej samej partycji.
