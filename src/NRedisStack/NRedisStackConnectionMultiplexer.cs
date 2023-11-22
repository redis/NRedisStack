using StackExchange.Redis;

namespace NRedisStack
{
    public static class NRedisStackConnectionMultiplexer // check if I can use this name to ConnectionMultiplexer
    {
        public static ConnectionMultiplexer Connect(string redisConnectionString)
        {
            var options = RedisUriParser.ParseConfigFromUri(redisConnectionString);
            return Connect(options);
        }

        public static Task<ConnectionMultiplexer> ConnectAsync(string redisConnectionString)
        {
            var options = RedisUriParser.ParseConfigFromUri(redisConnectionString);
            return ConnectAsync(options);
        }
        public static ConnectionMultiplexer Connect(ConfigurationOptions options)
        {
            SetLibName(options);
            // TODO: set here the library version when it will be available
            return ConnectionMultiplexer.Connect(options);
        }
        public static Task<ConnectionMultiplexer> ConnectAsync(ConfigurationOptions options)
        {
            SetLibName(options);
            // TODO: set here the library version when it will be available
            return ConnectionMultiplexer.ConnectAsync(options);
        }

        private static void SetLibName(ConfigurationOptions options)
        {
            if (options.LibraryName != null) // the user set his own the library name
                options.LibraryName = $"NRedisStack({options.LibraryName});.NET-{Environment.Version})";
            else // the default library name and version sending
                options.LibraryName = $"NRedisStack;.NET-{Environment.Version}";

        }
    }
}