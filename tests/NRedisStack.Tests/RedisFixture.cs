using StackExchange.Redis;

namespace NRedisStack.Tests
{
    public class RedisFixture : IDisposable
    {


        // Set the enviroment variable to specify your own alternet host and port:
        string redis = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";
        public RedisFixture() => Redis = ConnectionMultiplexer.Connect($"{redis}");

        public void Dispose()
        {
            Redis.Close();
        }

        public ConnectionMultiplexer Redis { get; private set; }

        public Version ServerVersion => Redis.GetServer(redis).Version;


        public string? SkipIfServerLessThan(string minVersion)
        {
            if (ServerVersion < new Version(minVersion))
            {
                return $"Test skipped because Redis server version ({ServerVersion}) is less than {minVersion}.";
            }

            return null;
        }

        public string? SkipIfServerGreaterThan(string maxVersion)
        {
            if (ServerVersion > new Version(maxVersion))
            {
                return $"Test skipped because Redis server version ({ServerVersion}) is greater than {maxVersion}.";
            }

            return null;
        }
    }
}