using StackExchange.Redis;

namespace NRedisStack
{
    public static class NRedisStackConnectionMultiplexer // check if I can use this name to ConnectionMultiplexer
    {
        public static ConnectionMultiplexer Connect(string redisConnectionString)
        {
            var options = new NRedisStackConfigurationOptions(redisConnectionString);
            return Connect(options);
        }

        public static Task<ConnectionMultiplexer> ConnectAsync(string redisConnectionString)
        {
            var options = new NRedisStackConfigurationOptions(redisConnectionString);
            return ConnectAsync(options);
        }
        public static ConnectionMultiplexer Connect(NRedisStackConfigurationOptions options)
        {
            return ConnectionMultiplexer.Connect(options.GetConfigurationOptions());
        }
        public static Task<ConnectionMultiplexer> ConnectAsync(NRedisStackConfigurationOptions options)
        {
            return ConnectionMultiplexer.ConnectAsync(options.GetConfigurationOptions());
        }
    }
}