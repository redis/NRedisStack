using NRedisStack.CountMinSketch.DataTypes;
using StackExchange.Redis;
namespace NRedisStack;

public class CmsCommands(IDatabase db) : CmsCommandsAsync(db), ICmsCommands
{
    /// <inheritdoc/>
    public long IncrBy(RedisKey key, RedisValue item, long increment)
    {
        return db.Execute(CmsCommandBuilder.IncrBy(key, item, increment)).ToLong();
    }

    /// <inheritdoc/>
    public long[] IncrBy(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
    {
        return db.Execute(CmsCommandBuilder.IncrBy(key, itemIncrements)).ToLongArray();
    }

    /// <inheritdoc/>
    public CmsInformation Info(RedisKey key)
    {
        var info = db.Execute(CmsCommandBuilder.Info(key));
        return info.ToCmsInfo();
    }

    /// <inheritdoc/>
    public bool InitByDim(RedisKey key, long width, long depth)
    {
        return db.Execute(CmsCommandBuilder.InitByDim(key, width, depth)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool InitByProb(RedisKey key, double error, double probability)
    {
        return db.Execute(CmsCommandBuilder.InitByProb(key, error, probability)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Merge(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
    {
        return db.Execute(CmsCommandBuilder.Merge(destination, numKeys, source, weight)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public long[] Query(RedisKey key, params RedisValue[] items)
    {
        return db.Execute(CmsCommandBuilder.Query(key, items)).ToLongArray();
    }
}