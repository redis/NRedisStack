using StackExchange.Redis;

namespace NRedisStack.Tests
{
    public class RedisFixture : IDisposable
    {

        string server = Environment.GetEnvironmentVariable("REDIS_SERVER") ?? "localhost";
        string port = Environment.GetEnvironmentVariable("REDIS_PORT") ?? "6379";
        public RedisFixture() => Redis = ConnectionMultiplexer.Connect($"{server}:{port}");

        public void Dispose()
        {
            Redis.Close();
        }

        public ConnectionMultiplexer Redis { get; private set; }
    }
}