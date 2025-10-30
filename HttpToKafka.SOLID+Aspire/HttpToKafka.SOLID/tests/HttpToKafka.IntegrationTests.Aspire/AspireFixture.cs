using Aspire.Hosting;
using Aspire.Hosting.Testing;

namespace HttpToKafka.IntegrationTests.Aspire;

public class AspireFixture : IAsyncLifetime
{
    private IDistributedApplicationTestingBuilder? _builder;
    public DistributedApplication? App { get; private set; }
    public HttpClient? ApiClient { get; private set; }

    public async Task InitializeAsync()
    {
        _builder = await DistributedApplicationTestingBuilder.CreateAsync<Projects.HttpToKafka_Aspire_AppHost>();
        _builder.Services.ConfigureHttpClientDefaults(b => b.AddStandardResilienceHandler());

        App = await _builder.BuildAsync();
        await App.StartAsync();

        using var cts = new CancellationTokenSource(TimeSpan.FromMinutes(2));
        await App.ResourceNotifications.WaitForResourceHealthyAsync("kafka", cts.Token);
        await App.ResourceNotifications.WaitForResourceHealthyAsync("httptokafka-api", cts.Token);

        ApiClient = App.CreateHttpClient("httptokafka-api");
    }

    public async Task DisposeAsync()
    {
        if (App is IAsyncDisposable d) await d.DisposeAsync();
    }
}

[CollectionDefinition("aspire-collection")]
public class AspireCollection : ICollectionFixture<AspireFixture> { }
