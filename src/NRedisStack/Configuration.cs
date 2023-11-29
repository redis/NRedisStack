using System.Dynamic;
using StackExchange.Redis;

namespace NRedisStack
{
    public class Configuration
    {
        public ConfigurationOptions Options { get; set; } = new ConfigurationOptions();

        public static Configuration Parse(string redisConnectionString) =>
            new Configuration().DoParse(redisConnectionString);

        public static Configuration Parse(ConfigurationOptions options) =>
            new Configuration().DoParse(options);

        private Configuration DoParse(string redisConnectionString)
        {
            try // Redis URI parsing
            {
                Options = RedisUriParser.FromUri(redisConnectionString);
            }
            catch (UriFormatException) // StackExchange.Redis connection string parsing
            {
                Options = ConfigurationOptions.Parse(redisConnectionString);
            }
            SetLibName(Options);
            return this;
        }

        private Configuration DoParse(ConfigurationOptions options)
        {
            Options = options;
            SetLibName(Options);
            return this;
        }

        internal static void SetLibName(ConfigurationOptions options)
        {
            if (options.LibraryName != null) // the user set his own the library name
                options.LibraryName = $"NRedisStack({options.LibraryName};.NET_v{Environment.Version})";
            else // the default library name and version sending
                options.LibraryName = $"NRedisStack(.NET_v{Environment.Version})";
        }

    }
}