using Microsoft.AspNetCore.Mvc;

var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

// Endpoint odbierający wiadomości push z KafkaOverHttp
app.MapPost("/kafka-hook", async ([FromBody] object message) =>
{
    Console.WriteLine("📨 Odebrano wiadomość z KafkaOverHttp:");
    Console.WriteLine(System.Text.Json.JsonSerializer.Serialize(
        message, new System.Text.Json.JsonSerializerOptions { WriteIndented = true }));

    await Task.CompletedTask;
    return Results.Ok();
});

app.MapGet("/", () => "KafkaOverHttp Client działa. Endpoint: POST /kafka-hook");

app.Run("http://localhost:6000");
