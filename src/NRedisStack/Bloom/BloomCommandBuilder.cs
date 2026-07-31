using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using NRedisStack.Bloom.Literals;
namespace NRedisStack;

public static class BloomCommandBuilder
{
    public static SerializedCommand Add(RedisKey key, RedisValue item)
    {
        return new(CommandCategories.WriteChecked, BF.ADD, key, item);
    }

    public static SerializedCommand Card(RedisKey key)
    {
        return new(CommandCategories.ReadOnly, BF.CARD, key);
    }

    public static SerializedCommand Exists(RedisKey key, RedisValue item)
    {
        return new(CommandCategories.ReadOnly, BF.EXISTS, key, item);
    }

    public static SerializedCommand Info(RedisKey key)
    {
        return new(CommandCategories.ReadOnly, BF.INFO, key);
    }

    public static SerializedCommand Insert(RedisKey key, RedisValue[] items, int? capacity = null,
        double? error = null, int? expansion = null,
        bool nocreate = false, bool nonscaling = false)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        var args = BloomAux.BuildInsertArgs(key, items, capacity, error, expansion, nocreate, nonscaling);

        return new(CommandCategories.WriteChecked, BF.INSERT, args);
    }

    public static SerializedCommand LoadChunk(RedisKey key, long iterator, Byte[] data)
    {
        return new(CommandCategories.WriteAccumulating, BF.LOADCHUNK, key, iterator, data);
    }

    public static SerializedCommand MAdd(RedisKey key, params RedisValue[] items)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];
        args.AddRange(items.Cast<object>());

        return new(CommandCategories.WriteChecked, BF.MADD, args);
    }

    public static SerializedCommand MExists(RedisKey key, RedisValue[] items)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];
        args.AddRange(items.Cast<object>());

        return new(CommandCategories.ReadOnly, BF.MEXISTS, args);

    }

    public static SerializedCommand Reserve(RedisKey key, double errorRate, long capacity,
        int? expansion = null, bool nonscaling = false)
    {
        List<object> args = [key, errorRate, capacity];

        if (expansion != null)
        {
            args.Add(expansion);
        }

        if (nonscaling)
        {
            args.Add(BloomArgs.NONSCALING);
        }

        return new(CommandCategories.WriteAccumulating, BF.RESERVE, args);
    }

    public static SerializedCommand ScanDump(RedisKey key, long iterator)
    {
        return new(CommandCategories.ReadOnly | CommandCategories.ServerSpecific, BF.SCANDUMP, key, iterator);
    }
}