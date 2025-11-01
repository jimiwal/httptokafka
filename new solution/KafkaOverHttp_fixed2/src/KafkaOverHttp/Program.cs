using Confluent.Kafka;
using Confluent.SchemaRegistry;
using KafkaOverHttp.Models;
using KafkaOverHttp.Services;
using Microsoft.AspNetCore.Mvc;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

// --- Konfiguracja z appsettings ---
var kafkaSection = builder.Configuration.GetSection("Kafka");

builder.Services.Configure<ProducerConfig>(options =>
{
    options.BootstrapServers = kafkaSection["BootstrapServers"];
    options.ClientId = kafkaSection["ClientId"] ?? "KafkaOverHttp";
    options.Acks = Acks.All;
    if (Enum.TryParse<SecurityProtocol>(kafkaSection["SecurityProtocol"], true, out var sp)) options.SecurityProtocol = sp;
    if (!string.IsNullOrEmpty(kafkaSection["SaslMechanism"])) options.SaslMechanism = Enum.Parse<SaslMechanism>(kafkaSection["SaslMechanism"], true);
    options.SaslUsername = kafkaSection["SaslUsername"];
    options.SaslPassword = kafkaSection["SaslPassword"];
});

builder.Services.Configure<ConsumerConfig>(options =>
{
    options.BootstrapServers = kafkaSection["BootstrapServers"];
    options.GroupId = kafkaSection["GroupId"] ?? "KafkaOverHttp-Consumers";
    options.AutoOffsetReset = Enum.TryParse<AutoOffsetReset>(kafkaSection["AutoOffsetReset"], true, out var aor) ? aor : AutoOffsetReset.Earliest;
    options.EnableAutoCommit = bool.TryParse(kafkaSection["EnableAutoCommit"], out var eac) && eac;
    if (Enum.TryParse<SecurityProtocol>(kafkaSection["SecurityProtocol"], true, out var sp)) options.SecurityProtocol = sp;
    if (!string.IsNullOrEmpty(kafkaSection["SaslMechanism"])) options.SaslMechanism = Enum.Parse<SaslMechanism>(kafkaSection["SaslMechanism"], true);
    options.SaslUsername = kafkaSection["SaslUsername"];
    options.SaslPassword = kafkaSection["SaslPassword"];
});

builder.Services.Configure<AdminClientConfig>(options =>
{
    options.BootstrapServers = kafkaSection["BootstrapServers"];
});

builder.Services.Configure<SchemaRegistryConfig>(options =>
{
    options.Url = kafkaSection.GetSection("SchemaRegistry")["Url"];
    options.BasicAuthUserInfo = kafkaSection.GetSection("SchemaRegistry")["BasicAuthUserInfo"];
});

builder.Services.Configure<PushOptions>(builder.Configuration.GetSection("Push"));

builder.Services.AddHttpClient(nameof(SubscriptionManager))
    .SetHandlerLifetime(TimeSpan.FromMinutes(5));

// --- DI serwisów ---
builder.Services.AddSingleton<AvroSerializerFactory>();
builder.Services.AddSingleton<KafkaProducerService>();
builder.Services.AddSingleton<KafkaConsumerService>();
builder.Services.AddSingleton<KafkaAdminService>();
builder.Services.AddSingleton<SubscriptionManager>();
builder.Services.AddHostedService(sp => sp.GetRequiredService<SubscriptionManager>());

// --- Swagger ---
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "KafkaOverHttp", Version = "v1" });
});

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();
app.MapGet("/", () => Results.Redirect("/swagger"));

// --- Endpointy info/admin ---
app.MapGet("/info/topics", ([FromServices] KafkaAdminService admin) =>
{
    var topics = admin.ListTopics();
    return Results.Ok(topics);
});

app.MapGet("/info/cluster", ([FromServices] KafkaAdminService admin) =>
{
    var info = admin.ClusterInfo();
    return Results.Ok(info);
});

// --- Produce: potwierdzony (czeka na zapis) ---
app.MapPost("/produce/confirmed", async (
    [FromServices] KafkaProducerService svc,
    [FromBody] ProduceRequest req,
    CancellationToken ct) =>
{
    if (req.Serialization == SerializationKind.ByteArray)
    {
        var dr = await svc.ProduceByteArrayAsync(req, awaitDelivery: true, ct);
        return Results.Ok(new { dr.Topic, dr.Partition, dr.Offset, dr.Status, persisted = true });
    }
    else
    {
        var dr = await svc.ProduceAvroAsync(req, awaitDelivery: true, ct);
        return Results.Ok(new { dr.Topic, dr.Partition, dr.Offset, dr.Status, persisted = true });
    }
});

// --- Produce: szybki (fire-and-forget) ---
app.MapPost("/produce/fast", async (
    [FromServices] KafkaProducerService svc,
    [FromBody] ProduceRequest req,
    CancellationToken ct) =>
{
    if (req.Serialization == SerializationKind.ByteArray)
    {
        var dr = await svc.ProduceByteArrayAsync(req, awaitDelivery: false, ct);
        return Results.Ok(new { dr.Topic, dr.Partition, dr.Offset, persisted = false });
    }
    else
    {
        var dr = await svc.ProduceAvroAsync(req, awaitDelivery: false, ct);
        return Results.Ok(new { dr.Topic, dr.Partition, dr.Offset, persisted = false });
    }
});

// --- Consume: jedna wiadomoœæ (pull z timeoutem) ---
app.MapPost("/consume/one", async (
    [FromServices] KafkaConsumerService svc,
    [FromBody] ConsumeOneRequest req,
    CancellationToken ct) =>
{
    var msg = await svc.ConsumeOneAsync(req, ct);
    return msg is null ? Results.NoContent() : Results.Ok(msg);
});

// --- Subscribe: push (webhook) ---
app.MapPost("/subscribe/push", (
    [FromBody] SubscribePushRequest req,
    [FromServices] SubscriptionManager manager) =>
{
    var ok = manager.Register(req);
    return ok
        ? Results.Accepted($"/subscribe/status/{req.SubscriptionId}", new { req.SubscriptionId })
        : Results.Conflict(new { message = "Subscription already exists", req.SubscriptionId });
});

// --- Cancel subscription ---
app.MapPost("/subscribe/cancel", (
    [FromBody] CancelSubscriptionRequest req,
    [FromServices] SubscriptionManager manager) =>
{
    var ok = manager.Cancel(req.SubscriptionId);
    return ok ? Results.Ok(new { canceled = true }) : Results.NotFound(new { canceled = false });
});

app.Run();
