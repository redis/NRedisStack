using StackExchange.Redis;

namespace NRedisStack
{
    public class Configuration
    {
        private ConfigurationOptions _options = new ConfigurationOptions();

        public static Configuration Parse(string redisConnectionString) =>
            new Configuration().DoParse(redisConnectionString);

        public static Configuration Parse(ConfigurationOptions options) =>
            new Configuration().DoParse(options);

        private Configuration DoParse(string redisConnectionString)
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

        private Configuration DoParse(ConfigurationOptions options)
        {
            _options = options;
            SetLibName(_options);
            return this;
        }

        public ConfigurationOptions GetOptions()
        {
            return _options;
        }

        internal static void SetLibName(ConfigurationOptions options)
        {
            if (options.LibraryName != null) // the user set his own the library name
                options.LibraryName = $"NRedisStack({options.LibraryName});.NET-{Environment.Version})";
            else // the default library name and version sending
                options.LibraryName = $"NRedisStack;.NET-{Environment.Version}";
        }

    }
}