namespace HttpToKafka.Api.Security;

public static class Redaction
{
    public static string? Mask(string? value)
        => string.IsNullOrEmpty(value) ? value : new string('*', Math.Min(8, value.Length));
}
