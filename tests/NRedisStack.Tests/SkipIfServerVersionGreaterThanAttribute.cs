using Xunit;
using StackExchange.Redis;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class SkipIfRedisVersionGteAttribute : FactAttribute
{
    private readonly string _minVersion;
    private readonly string DefaultRedisConnectionString = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";

    public SkipIfRedisVersionGteAttribute(string minVersion)
    {
        _minVersion = minVersion;
    }

    public override string Skip
    {
        get
        {
            using (var connection = ConnectionMultiplexer.Connect(DefaultRedisConnectionString))
            {
                var serverVersion = connection.GetServer(connection.GetEndPoints()[0]).Version;

                if (serverVersion >= new Version(_minVersion))
                {
                    return $"Test skipped because Redis server version ({serverVersion}) is >= {_minVersion}.";
                }

                return null;
            }
        }
    }
}