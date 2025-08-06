using NRedisStack.Bloom.Literals;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
namespace NRedisStack;

public static class BloomCommandBuilder
{
    public static SerializedCommand Add(RedisKey key, RedisValue item)
    {
        return new(BF.ADD, key, item);
    }

    public static SerializedCommand Card(RedisKey key)
    {
        return new(BF.CARD, key);
    }

    public static SerializedCommand Exists(RedisKey key, RedisValue item)
    {
        return new(BF.EXISTS, key, item);
    }

    public static SerializedCommand Info(RedisKey key)
    {
        return new(BF.INFO, key);
    }

    public static SerializedCommand Insert(RedisKey key, RedisValue[] items, int? capacity = null,
        double? error = null, int? expansion = null,
        bool nocreate = false, bool nonscaling = false)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        var args = BloomAux.BuildInsertArgs(key, items, capacity, error, expansion, nocreate, nonscaling);

        return new(BF.INSERT, args);
    }

    public static SerializedCommand LoadChunk(RedisKey key, long iterator, Byte[] data)
    {
        return new(BF.LOADCHUNK, key, iterator, data);
    }

    public static SerializedCommand MAdd(RedisKey key, params RedisValue[] items)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];
        args.AddRange(items.Cast<object>());

        return new(BF.MADD, args);
    }

    public static SerializedCommand MExists(RedisKey key, RedisValue[] items)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];
        args.AddRange(items.Cast<object>());

        return new(BF.MEXISTS, args);

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

        return new(BF.RESERVE, args);
    }

    public static SerializedCommand ScanDump(RedisKey key, long iterator)
    {
        return new(BF.SCANDUMP, key, iterator);
    }
}