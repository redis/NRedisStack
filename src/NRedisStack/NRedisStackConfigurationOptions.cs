using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack
{
    public class NRedisStackConfigurationOptions
    {
        private readonly ConfigurationOptions _configurationOptions = new ConfigurationOptions();

        public NRedisStackConfigurationOptions(string redisConnectionString)
        {
            _configurationOptions = RedisUriParser.FromUri(redisConnectionString);
            SetLibName(_configurationOptions);
        }


        NRedisStackConfigurationOptions()
        {
            SetLibName(_configurationOptions);
        }

        public ConfigurationOptions GetConfigurationOptions()
        {
            return _configurationOptions;
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