using StackExchange.Redis;

namespace NRedisStack.Core
{
    public static class Auxiliary
    {
        public static List<object> MergeArgs(RedisKey key, RedisValue[] items)
        {
            var args = new List<object> { key };
            foreach (var item in items) args.Add(item);
            return args;
        }
    }
}