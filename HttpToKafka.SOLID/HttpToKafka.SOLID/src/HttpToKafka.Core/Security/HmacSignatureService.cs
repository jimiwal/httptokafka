using System.Security.Cryptography;
using System.Text;
using HttpToKafka.Core.Abstractions;
using HttpToKafka.Core.Models;

namespace HttpToKafka.Core.Security;

public class HmacSignatureService : ICallbackSigner
{
    private readonly IClock _clock;
    public HmacSignatureService(IClock clock) => _clock = clock;

    public IDictionary<string, string> BuildHeaders(string body, SubscribeAsyncRequest request, long? fixedTimestampSeconds = null)
    {
        var headers = new Dictionary<string,string>();
        if (string.IsNullOrEmpty(request.CallbackHmacSecret)) return headers;

        var ts = (fixedTimestampSeconds ?? _clock.UnixSeconds()).ToString();
        if (request.CallbackIncludeTimestamp)
        {
            headers[string.IsNullOrEmpty(request.CallbackTimestampHeaderName) ? "X-HTK-Timestamp" : request.CallbackTimestampHeaderName] = ts;
        }

        using var hmac = new HMACSHA256(Encoding.UTF8.GetBytes(request.CallbackHmacSecret));
        var sig = hmac.ComputeHash(Encoding.UTF8.GetBytes(ts + "." + body));
        var hex = Convert.ToHexString(sig).ToLowerInvariant();
        var headerName = string.IsNullOrEmpty(request.CallbackSignatureHeaderName) ? "X-HTK-Signature" : request.CallbackSignatureHeaderName;
        headers[headerName] = "sha256=" + hex;
        return headers;
    }
}
