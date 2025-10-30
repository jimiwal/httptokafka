using HttpToKafka.Core.Abstractions;

namespace HttpToKafka.Core.Utils;

public class ExponentialBackoffStrategy : IBackoffStrategy
{
    private readonly int _initialMs;
    private readonly int _maxMs;

    public ExponentialBackoffStrategy(int initialMs = 1000, int maxMs = 30000)
    {
        _initialMs = Math.Max(1, initialMs);
        _maxMs = Math.Max(_initialMs, maxMs);
    }

    public int NextDelayMs(int attempt)
    {
        if (attempt <= 1) return _initialMs;
        try
        {
            checked
            {
                var val = _initialMs * (int)Math.Pow(2, attempt - 1);
                return Math.Min(val, _maxMs);
            }
        }
        catch
        {
            return _maxMs;
        }
    }
}

public class SystemClock : IClock
{
    public long UnixSeconds() => DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    public DateTimeOffset UtcNow() => DateTimeOffset.UtcNow;
}
