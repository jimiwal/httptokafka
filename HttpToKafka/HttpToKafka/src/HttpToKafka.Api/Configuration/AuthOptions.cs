namespace HttpToKafka.Api.Configuration;
public sealed class AuthOptions
{
    public string Authority { get; set; } = string.Empty;
    public string Audience { get; set; } = string.Empty;
}
