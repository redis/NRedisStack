using StackExchange.Redis;

namespace NRedisStack.Search;

public class ProfilingInformation
{
    public RedisResult Info { get; private set; }
    public ProfilingInformation(RedisResult info)
    {
        Info = info;
    }

}