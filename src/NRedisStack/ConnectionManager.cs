using StackExchange.Redis;

namespace NRedisStack
{
    public static class ConnectionManager
    {
        public static IConnectionMultiplexer Connect(string redisConnectionString)
        {
            return Connect(Configuration.Parse(redisConnectionString));
        }

        public static async Task<IConnectionMultiplexer> ConnectAsync(string redisConnectionString)
        {
            return await ConnectAsync(Configuration.Parse(redisConnectionString));
        }

        public static IConnectionMultiplexer Connect(Configuration options)
        {
            return ConnectionMultiplexer.Connect(options.GetOptions());
        }

        public static async Task<IConnectionMultiplexer> ConnectAsync(Configuration options)
        {
            return await ConnectionMultiplexer.ConnectAsync(options.GetOptions());
        }

        public static IConnectionMultiplexer Connect(ConfigurationOptions options)
        {
            return ConnectionMultiplexer.Connect(options);
        }

        public static async Task<IConnectionMultiplexer> ConnectAsync(ConfigurationOptions options)
        {
            return await ConnectionMultiplexer.ConnectAsync(options);
        }
    }
}