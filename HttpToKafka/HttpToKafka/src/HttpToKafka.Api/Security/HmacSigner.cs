using System.Security.Cryptography;
using System.Text;
namespace HttpToKafka.Api.Security;
public sealed class HmacSigner
{
    public string Sign(string secret, string body)
    {
        using var h = new HMACSHA256(Encoding.UTF8.GetBytes(secret));
        var sig = h.ComputeHash(Encoding.UTF8.GetBytes(body));
        return Convert.ToHexString(sig).ToLowerInvariant();
    }
}
