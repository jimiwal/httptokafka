using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Configuration;

namespace HttpToKafka.Api.Services.Webhooks;

public class HmacSigner
{
    private readonly IConfiguration _cfg;
    public HmacSigner(IConfiguration cfg) => _cfg = cfg;

    public (string signature, string alg, string timestamp) Sign(ReadOnlySpan<byte> body)
    {
        var secret = _cfg["Subscriptions:Secret"] ?? throw new InvalidOperationException("Subscriptions:Secret not set.");
        var ts = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        var toSign = Encoding.UTF8.GetBytes($"{ts}\n").Concat(body.ToArray()).ToArray();
        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var hash = hmac.ComputeHash(toSign);
        var sig = "v1=" + Convert.ToHexString(hash).ToLowerInvariant();
        return (sig, "HMACSHA256", ts);
    }
}
