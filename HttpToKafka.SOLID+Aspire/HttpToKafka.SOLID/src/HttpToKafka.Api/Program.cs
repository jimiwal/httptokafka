using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Options;
using HttpToKafka.Core.Security;
using HttpToKafka.Core.Utils;
using HttpToKafka.Kafka.Admin;
using HttpToKafka.Kafka.Async;
using HttpToKafka.Kafka.Consumers;
using HttpToKafka.Kafka.SchemaRegistry;
using HttpToKafka.Kafka.Services;
using HttpToKafka.Kafka.Producer;
using Microsoft.OpenApi.Models;

var builder = WebApplication.CreateBuilder(args);

// Map Aspire connection string "ConnectionStrings:kafka" => Kafka:BootstrapServers (local dev)
var csKafka = builder.Configuration.GetConnectionString("kafka");
if (!string.IsNullOrWhiteSpace(csKafka))
{
    builder.Configuration["Kafka:BootstrapServers"] = csKafka;
    builder.Configuration["Kafka:SecurityProtocol"] = "";
    builder.Configuration["Kafka:SaslMechanism"] = "";
    builder.Configuration["Kafka:SaslUsername"] = "";
    builder.Configuration["Kafka:SaslPassword"] = "";
}

// Options
builder.Services.Configure<KafkaSettings>(builder.Configuration.GetSection("Kafka"));
builder.Services.Configure<SchemaRegistrySettings>(builder.Configuration.GetSection("SchemaRegistry"));

// Http client for callbacks
builder.Services.AddHttpClient("callbacks").SetHandlerLifetime(TimeSpan.FromMinutes(5));

// Core services
builder.Services.AddSingleton<IClock, SystemClock>();
builder.Services.AddSingleton<ICallbackSigner, HmacSignatureService>();
builder.Services.AddSingleton<IBackoffStrategy>(_ => new ExponentialBackoffStrategy());

// Kafka infrastructure
builder.Services.AddSingleton<ProducerPool>();
builder.Services.AddSingleton<KafkaConsumerFactory>();
builder.Services.AddSingleton<SchemaRegistryClientFactory>();

// App services
builder.Services.AddSingleton<IProducerService, KafkaProducerService>();
builder.Services.AddSingleton<IConsumerService, KafkaConsumerService>();
builder.Services.AddSingleton<IAdminService, KafkaAdminService>();
builder.Services.AddSingleton<ICallbackSender, HttpCallbackSender>();
builder.Services.AddSingleton<IAsyncSubscriptionManager, KafkaAsyncSubscriptionManager>();

// MVC + Swagger
builder.Services.AddControllers().AddJsonOptions(o => o.JsonSerializerOptions.PropertyNamingPolicy = null);
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c => c.SwaggerDoc("v1", new OpenApiInfo { Title = "HttpToKafka SOLID", Version = "v1" }));

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI(c => c.SwaggerEndpoint("/swagger/v1/swagger.json", "HttpToKafka SOLID v1"));

app.MapControllers();

app.Run();
