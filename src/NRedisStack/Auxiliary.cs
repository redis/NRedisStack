using StackExchange.Redis;

namespace NRedisStack
{
    public static class Auxiliary
    {
        public static List<object> MergeArgs(RedisKey key, params RedisValue[] items)
        {
            var args = new List<object>(items.Length + 1) { key };
            foreach (var item in items) args.Add(item);
            return args;
        }
    }
}