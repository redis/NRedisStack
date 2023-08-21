using StackExchange.Redis;

namespace NRedisStack.Tests
{
    public class RedisFixture : IDisposable
    {
        public RedisFixture()
        {
            // Set the enviroment variable to specify your own alternet host and port:
            var redisConnectionString = Environment.GetEnvironmentVariable("REDIS") ?? "localhost:6379";
            Redis = ConnectionMultiplexer.Connect(redisConnectionString);
        }

        public void Dispose()
        {
            Redis.Close();
        }

        public ConnectionMultiplexer Redis { get; }
    }
}