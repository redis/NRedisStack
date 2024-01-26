using NRedisStack.CountMinSketch.DataTypes;
using StackExchange.Redis;
namespace NRedisStack;

public class CmsCommandsAsync(IDatabaseAsync db) : ICmsCommandsAsync
{
    /// <inheritdoc/>
    public async Task<long> IncrByAsync(RedisKey key, RedisValue item, long increment)
    {
        return (await db.ExecuteAsync(CmsCommandBuilder.IncrBy(key, item, increment))).ToLong();
    }

    /// <inheritdoc/>
    public async Task<long[]> IncrByAsync(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
    {
        return (await db.ExecuteAsync(CmsCommandBuilder.IncrBy(key, itemIncrements))).ToLongArray();

    }

    /// <inheritdoc/>
    public async Task<CmsInformation> InfoAsync(RedisKey key)
    {
        var info = await db.ExecuteAsync(CmsCommandBuilder.Info(key));
        return info.ToCmsInfo();
    }

    /// <inheritdoc/>
    public async Task<bool> InitByDimAsync(RedisKey key, long width, long depth)
    {
        return (await db.ExecuteAsync(CmsCommandBuilder.InitByDim(key, width, depth))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> InitByProbAsync(RedisKey key, double error, double probability)
    {
        return (await db.ExecuteAsync(CmsCommandBuilder.InitByProb(key, error, probability))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> MergeAsync(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
    {
        return (await db.ExecuteAsync(CmsCommandBuilder.Merge(destination, numKeys, source, weight))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<long[]> QueryAsync(RedisKey key, params RedisValue[] items)
    {
        return (await db.ExecuteAsync(CmsCommandBuilder.Query(key, items))).ToLongArray();
    }
}