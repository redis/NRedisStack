using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack
{
    public class Configuration
    {
        private ConfigurationOptions _options = new ConfigurationOptions();

        public static Configuration Parse(string redisConnectionString) =>
            new Configuration().DoParse(redisConnectionString);

        public Configuration DoParse(string redisConnectionString)
        {
            try // Redis URI parsing
            {
                _options = RedisUriParser.FromUri(redisConnectionString);
            }
            catch (UriFormatException) // StackExchange.Redis connection string parsing
            {
                _options = ConfigurationOptions.Parse(redisConnectionString);
            }
            SetLibName(_options);
            return this;
        }


        Configuration()
        {
            SetLibName(_options);
        }

        public ConfigurationOptions GetOptions()
        {
            return _options;
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