using NRedisStack.Bloom.Literals;
using StackExchange.Redis;

namespace NRedisStack;

public static class BloomAux
{
    public static List<object> BuildInsertArgs(RedisKey key, IEnumerable<RedisValue> items, int? capacity,
        double? error, int? expansion, bool nocreate, bool nonscaling)
    {
        var args = new List<object> { key };
        args.AddCapacity(capacity);
        args.AddError(error);
        args.AddExpansion(expansion);
        args.AddNoCreate(nocreate);
        args.AddNoScaling(nonscaling);
        args.AddItems(items);

        return args;
    }

    private static void AddItems(this List<object> args, IEnumerable<RedisValue> items)
    {
        args.Add(BloomArgs.ITEMS);
        args.AddRange(items.Cast<object>());
    }

    private static void AddNoScaling(this ICollection<object> args, bool nonScaling)
    {
        if (nonScaling)
        {
            args.Add(BloomArgs.NONSCALING);
        }
    }

    private static void AddNoCreate(this ICollection<object> args, bool nocreate)
    {
        if (nocreate)
        {
            args.Add(BloomArgs.NOCREATE);
        }
    }

    private static void AddExpansion(this ICollection<object> args, int? expansion)
    {
        if (expansion == null) return;
        args.Add(BloomArgs.EXPANSION);
        args.Add(expansion);
    }

    private static void AddError(this ICollection<object> args, double? error)
    {
        if (error == null) return;
        args.Add(BloomArgs.ERROR);
        args.Add(error);
    }

    private static void AddCapacity(this ICollection<object> args, int? capacity)
    {
        if (capacity == null) return;
        args.Add(BloomArgs.CAPACITY);
        args.Add(capacity);
    }
}