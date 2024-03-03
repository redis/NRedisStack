using NRedisStack.CountMinSketch.Literals;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack;

public static class CmsCommandBuilder
{
    public static SerializedCommand IncrBy(RedisKey key, RedisValue item, long increment)
    {
        return new SerializedCommand(CMS.INCRBY, key, item, increment);
    }

    public static SerializedCommand IncrBy(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
    {
        if (itemIncrements.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
        foreach (var pair in itemIncrements)
        {
            args.Add(pair.Item1);
            args.Add(pair.Item2);
        }
        return new SerializedCommand(CMS.INCRBY, args);
    }

    public static SerializedCommand Info(RedisKey key)
    {
        var info = new SerializedCommand(CMS.INFO, key);
        return info;
    }

    public static SerializedCommand InitByDim(RedisKey key, long width, long depth)
    {
        return new SerializedCommand(CMS.INITBYDIM, key, width, depth);
    }

    public static SerializedCommand InitByProb(RedisKey key, double error, double probability)
    {
        return new SerializedCommand(CMS.INITBYPROB, key, error, probability);
    }

    public static SerializedCommand Merge(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
    {
        if (source.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(source));

        List<object> args = [destination, numKeys];
        args.AddRange(source.Cast<object>());

        if (weight is not { Length: >= 1 }) return new SerializedCommand(CMS.MERGE, args);
        args.Add(CmsArgs.WEIGHTS);
        args.AddRange(weight.Cast<object>());

        return new SerializedCommand(CMS.MERGE, args);
    }

    public static SerializedCommand Query(RedisKey key, params RedisValue[] items)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = new List<object> { key };
        foreach (var item in items) args.Add(item);

        return new SerializedCommand(CMS.QUERY, args);
    }
}