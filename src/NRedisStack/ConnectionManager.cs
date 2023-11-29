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

        public static IConnectionMultiplexer Connect(Configuration configuration)
        {
            return ConnectionMultiplexer.Connect(configuration.Options);
        }

        public static async Task<IConnectionMultiplexer> ConnectAsync(Configuration configuration)
        {
            return await ConnectionMultiplexer.ConnectAsync(configuration.Options);
        }

        public static IConnectionMultiplexer Connect(ConfigurationOptions options)
        {
            return Connect(Configuration.Parse(options));
        }

        public static async Task<IConnectionMultiplexer> ConnectAsync(ConfigurationOptions options)
        {
            return await ConnectAsync(Configuration.Parse(options));
        }
    }
}