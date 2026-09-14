using NRedisStack.CountMinSketch.Literals;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack;

public static class CmsCommandBuilder
{
    public static SerializedCommand IncrBy(RedisKey key, RedisValue item, long increment)
    {
        return new(CommandCategories.WriteAccumulating, CMS.INCRBY, key, item, increment);
    }

    public static SerializedCommand IncrBy(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
    {
        if (itemIncrements.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(itemIncrements));

        List<object> args = [key];
        foreach (var pair in itemIncrements)
        {
            args.Add(pair.Item1);
            args.Add(pair.Item2);
        }

        return new(CommandCategories.WriteAccumulating, CMS.INCRBY, args);
    }

    public static SerializedCommand Info(RedisKey key)
    {
        var info = new SerializedCommand(CommandCategories.ReadOnly, CMS.INFO, key);
        return info;
    }

    public static SerializedCommand InitByDim(RedisKey key, long width, long depth, int? cellSize = null)
    {
        List<object> args = [key, width, depth];
        AddCellSize(args, cellSize);

        return new(CommandCategories.WriteAccumulating, CMS.INITBYDIM, args);
    }

    public static SerializedCommand InitByProb(RedisKey key, double error, double probability, int? cellSize = null)
    {
        List<object> args = [key, error, probability];
        AddCellSize(args, cellSize);

        return new(CommandCategories.WriteAccumulating, CMS.INITBYPROB, args);
    }

    private static void AddCellSize(List<object> args, int? cellSize)
    {
        if (cellSize is null) return;

        if (cellSize is not (1 or 2 or 4 or 8))
            throw new ArgumentOutOfRangeException(nameof(cellSize), cellSize, "Cell size must be 1, 2, 4, or 8.");

        args.Add(CmsArgs.CELL_SIZE);
        args.Add(cellSize.Value);
    }

    public static SerializedCommand Merge(RedisValue destination, long numKeys, RedisValue[] source,
        long[]? weight = null)
    {
        if (source.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(source));

        List<object> args = [destination, numKeys];

        foreach (var s in source) args.Add(s);

        if (weight != null && weight.Length >= 1)
        {
            args.Add(CmsArgs.WEIGHTS);
            foreach (var w in weight) args.Add(w);
        }

        return new(CommandCategories.WriteAccumulating, CMS.MERGE, args);
    }

    public static SerializedCommand Query(RedisKey key, params RedisValue[] items)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];
        foreach (var item in items) args.Add(item);

        return new(CommandCategories.ReadOnly, CMS.QUERY, args);
    }
}