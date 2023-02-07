using NRedisStack.TopK.Literals;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack
{
    public static class TopKCommandBuilder
    {
        public static SerializedCommand Add(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);
            return new SerializedCommand(TOPK.ADD, args);
        }

        public static SerializedCommand Count(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);
            return new SerializedCommand(TOPK.COUNT, args);
        }

        public static SerializedCommand IncrBy(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements)
        {
            if (itemIncrements.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
            foreach (var pair in itemIncrements)
            {
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return new SerializedCommand(TOPK.INCRBY, args);
        }

        public static SerializedCommand Info(RedisKey key)
        {
             return new SerializedCommand(TOPK.INFO, key);
        }

        public static SerializedCommand List(RedisKey key, bool withcount = false)
        {
            return (withcount) ? new SerializedCommand(TOPK.LIST, key, "WITHCOUNT")
                                     : new SerializedCommand(TOPK.LIST, key);
        }

        public static SerializedCommand Query(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);

            return new SerializedCommand(TOPK.QUERY, args);
        }

        public static SerializedCommand Reserve(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9)
        {
            return new SerializedCommand(TOPK.RESERVE, key, topk, width, depth, decay);
        }
    }
}
