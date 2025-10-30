using Confluent.SchemaRegistry;
using HttpToKafka.Api.Services.Kafka;
using HttpToKafka.Api.Services.Serialization;
using HttpToKafka.Api.Services.Subscriptions;
using HttpToKafka.Api.Services.Webhooks;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.OpenApi.Models;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddControllers();
builder.Services.AddEndpointsApiExplorer();
builder.Services.AddSwaggerGen(c =>
{
    c.SwaggerDoc("v1", new OpenApiInfo { Title = "HttpToKafka", Version = "v1" });
});

// Auth (OIDC/JWT)
builder.Services
    .AddAuthentication(JwtBearerDefaults.AuthenticationScheme)
    .AddJwtBearer(opt =>
    {
        opt.Authority = builder.Configuration["Authentication:Authority"];
        opt.Audience  = builder.Configuration["Authentication:Audience"];
        if (bool.TryParse(builder.Configuration["Authentication:RequireHttpsMetadata"], out var req))
            opt.RequireHttpsMetadata = req;
    });
builder.Services.AddAuthorization();

builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddAspNetCoreInstrumentation().AddRuntimeInstrumentation())
    .WithTracing(t => t.AddAspNetCoreInstrumentation());

builder.Services.AddHttpClient<IWebhookClient, WebhookClient>()
    .SetHandlerLifetime(TimeSpan.FromMinutes(5));

builder.Services.AddSingleton<HmacSigner>();
builder.Services.AddSingleton<OAuthBearerTokenProvider>();
builder.Services.AddSingleton<IKafkaProducer, KafkaProducerService>();
builder.Services.AddSingleton<IKafkaConsumerFactory, KafkaConsumerFactory>();
builder.Services.AddSingleton<IKafkaAdminService, KafkaAdminService>();

builder.Services.AddSingleton<ISchemaRegistryClient>(sp =>
{
    var cfg = new SchemaRegistryConfig { Url = builder.Configuration["SchemaRegistry:Url"] };
    var user = builder.Configuration["SchemaRegistry:BasicAuth:Username"];
    var pass = builder.Configuration["SchemaRegistry:BasicAuth:Password"];
    if (!string.IsNullOrWhiteSpace(user))
    {
        cfg.BasicAuthCredentialsSource = "USER_INFO";
        cfg.BasicAuthUserInfo = $"{user}:{pass}";
    }
    return new CachedSchemaRegistryClient(cfg);
});

builder.Services.AddSingleton<SerializerFactory>();
builder.Services.AddSingleton<ISubscriptionManager, SubscriptionManager>();
builder.Services.AddHostedService<SubscriptionWorker>();

var app = builder.Build();

app.UseSwagger();
app.UseSwaggerUI();

app.UseAuthentication();
app.UseAuthorization();

app.MapControllers();

app.Run();
