using NRedisStack.CuckooFilter.DataTypes;
using StackExchange.Redis;
namespace NRedisStack;

public class CuckooCommands : CuckooCommandsAsync, ICuckooCommands
{
    readonly IDatabase _db;
    public CuckooCommands(IDatabase db) : base(db)
    {
        _db = db;
    }

    /// <inheritdoc/>
    public bool Add(RedisKey key, RedisValue item)
    {
        return _db.Execute(CuckooCommandBuilder.Add(key, item)).ToString() == "1";
    }

    /// <inheritdoc/>
    public bool AddNX(RedisKey key, RedisValue item)
    {
        return _db.Execute(CuckooCommandBuilder.AddNX(key, item)).ToString() == "1";
    }

    /// <inheritdoc/>
    public long Count(RedisKey key, RedisValue item)
    {
        return _db.Execute(CuckooCommandBuilder.Count(key, item)).ToLong();
    }

    /// <inheritdoc/>
    public bool Del(RedisKey key, RedisValue item)
    {
        return _db.Execute(CuckooCommandBuilder.Del(key, item)).ToString() == "1";
    }

    /// <inheritdoc/>
    public bool Exists(RedisKey key, RedisValue item)
    {
        return _db.Execute(CuckooCommandBuilder.Exists(key, item)).ToString() == "1";
    }

    /// <inheritdoc/>
    public CuckooInformation Info(RedisKey key)
    {
        return _db.Execute(CuckooCommandBuilder.Info(key)).ToCuckooInfo();
    }

    /// <inheritdoc/>
    public bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
    {
        return _db.Execute(CuckooCommandBuilder.Insert(key, items, capacity, nocreate)).ToBooleanArray();
    }

    /// <inheritdoc/>
    public bool[] InsertNX(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
    {
        return _db.Execute(CuckooCommandBuilder.InsertNX(key, items, capacity, nocreate)).ToBooleanArray();
    }

    /// <inheritdoc/>
    public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
    {
        return _db.Execute(CuckooCommandBuilder.LoadChunk(key, iterator, data)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool[] MExists(RedisKey key, params RedisValue[] items)
    {
        return _db.Execute(CuckooCommandBuilder.MExists(key, items)).ToBooleanArray();
    }

    /// <inheritdoc/>
    public bool Reserve(RedisKey key, long capacity,
        long? bucketSize = null, int? maxIterations = null, int? expansion = null)
    {
        return _db.Execute(CuckooCommandBuilder.Reserve(key, capacity, bucketSize, maxIterations, expansion)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator)
    {
        return _db.Execute(CuckooCommandBuilder.ScanDump(key, iterator)).ToScanDumpTuple();
    }
}