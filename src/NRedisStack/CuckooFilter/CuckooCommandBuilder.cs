using NRedisStack.CuckooFilter.Literals;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
namespace NRedisStack;

public static class CuckooCommandBuilder
{

    public static SerializedCommand Add(RedisKey key, RedisValue item)
    {
        return new(CF.ADD, key, item);
    }

    public static SerializedCommand AddNX(RedisKey key, RedisValue item)
    {
        return new(CF.ADDNX, key, item);
    }

    public static SerializedCommand Count(RedisKey key, RedisValue item)
    {
        return new(CF.COUNT, key, item);
    }

    public static SerializedCommand Del(RedisKey key, RedisValue item)
    {
        return new(CF.DEL, key, item);
    }

    public static SerializedCommand Exists(RedisKey key, RedisValue item)
    {
        return new(CF.EXISTS, key, item);
    }

    public static SerializedCommand Info(RedisKey key)
    {
        var info = new SerializedCommand(CF.INFO, key);
        return info;
    }

    public static SerializedCommand Insert(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];

        if (capacity != null)
        {
            args.Add(CuckooArgs.CAPACITY);
            args.Add(capacity);
        }

        if (nocreate)
        {
            args.Add(CuckooArgs.NOCREATE);
        }

        args.Add(CuckooArgs.ITEMS);
        foreach (var item in items)
        {
            args.Add(item);
        }

        return new(CF.INSERT, args);
    }

    public static SerializedCommand InsertNX(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];

        if (capacity != null)
        {
            args.Add(CuckooArgs.CAPACITY);
            args.Add(capacity);
        }

        if (nocreate)
        {
            args.Add(CuckooArgs.NOCREATE);
        }

        args.Add(CuckooArgs.ITEMS);
        foreach (var item in items)
        {
            args.Add(item);
        }

        return new(CF.INSERTNX, args);
    }

    public static SerializedCommand LoadChunk(RedisKey key, long iterator, Byte[] data)
    {
        return new(CF.LOADCHUNK, key, iterator, data);
    }

    public static SerializedCommand MExists(RedisKey key, params RedisValue[] items)
    {
        if (items.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(items));

        List<object> args = [key];

        foreach (var item in items)
        {
            args.Add(item);
        }

        return new(CF.MEXISTS, args);
    }

    public static SerializedCommand Reserve(RedisKey key, long capacity,
        long? bucketSize = null, int? maxIterations = null, int? expansion = null)
    {
        List<object> args = [key, capacity];

        if (bucketSize != null)
        {
            args.Add(CuckooArgs.BUCKETSIZE);
            args.Add(bucketSize);
        }

        if (maxIterations != null)
        {
            args.Add(CuckooArgs.MAXITERATIONS);
            args.Add(maxIterations);
        }

        if (expansion != null)
        {
            args.Add(CuckooArgs.EXPANSION);
            args.Add(expansion);
        }

        return new(CF.RESERVE, args);
    }

    public static SerializedCommand ScanDump(RedisKey key, long iterator)
    {
        return new(CF.SCANDUMP, key, iterator);
    }
}