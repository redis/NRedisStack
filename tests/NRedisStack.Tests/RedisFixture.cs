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
    }
}