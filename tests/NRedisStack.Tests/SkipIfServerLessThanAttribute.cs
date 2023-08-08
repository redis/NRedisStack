using System;
using Xunit;
using Xunit.Sdk;
using StackExchange.Redis;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false)]
public class SkipIfRedisVersionLessThanAttribute : FactAttribute
{
    private readonly string _minVersion;
    private string DefaultRedisConnectionString = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";

    public SkipIfRedisVersionLessThanAttribute(string minVersion)
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

                if (serverVersion < new Version(_minVersion))
                {
                    return $"Test skipped because Redis server version ({serverVersion}) is less than {_minVersion}.";
                }

                return null;
            }
        }
    }
}
