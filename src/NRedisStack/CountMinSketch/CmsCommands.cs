using NRedisStack.CountMinSketch.DataTypes;
using StackExchange.Redis;

namespace NRedisStack;

public class CmsCommands : CmsCommandsAsync, ICmsCommands
{
    private readonly IDatabase _db;

    public CmsCommands(IDatabase db) : base(db)
    {
        _db = db;
    }

    /// <inheritdoc/>
    public long IncrBy(RedisKey key, RedisValue item, long increment)
    {
        return _db.Execute(CmsCommandBuilder.IncrBy(key, item, increment)).ToLong();
    }

    /// <inheritdoc/>
    public long[] IncrBy(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
    {
        return _db.Execute(CmsCommandBuilder.IncrBy(key, itemIncrements)).ToLongArray();
    }

    /// <inheritdoc/>
    public CmsInformation Info(RedisKey key)
    {
        var info = _db.Execute(CmsCommandBuilder.Info(key));
        return info.ToCmsInfo();
    }

    /// <inheritdoc/>
    public bool InitByDim(RedisKey key, long width, long depth)
    {
        return _db.Execute(CmsCommandBuilder.InitByDim(key, width, depth)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool InitByProb(RedisKey key, double error, double probability)
    {
        return _db.Execute(CmsCommandBuilder.InitByProb(key, error, probability)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Merge(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
    {
        return _db.Execute(CmsCommandBuilder.Merge(destination, numKeys, source, weight)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public long[] Query(RedisKey key, params RedisValue[] items)
    {
        return _db.Execute(CmsCommandBuilder.Query(key, items)).ToLongArray();
    }
}