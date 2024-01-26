using NRedisStack.Bloom.DataTypes;
using StackExchange.Redis;
namespace NRedisStack;

public class BloomCommandsAsync(IDatabaseAsync db) : IBloomCommandsAsync
{
    /// <inheritdoc/>
    public async Task<bool> AddAsync(RedisKey key, RedisValue item)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.Add(key, item))).ToString() == "1";
    }


    /// <inheritdoc/>
    public async Task<long> CardAsync(RedisKey key)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.Card(key))).ToLong();
    }

    /// <inheritdoc/>
    public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.Exists(key, item))).ToString() == "1";
    }

    /// <inheritdoc/>
    public async Task<BloomInformation> InfoAsync(RedisKey key)
    {
        var info = (await db.ExecuteAsync(BloomCommandBuilder.Info(key)));
        return info.ToBloomInfo();
    }

    /// <inheritdoc/>
    public async Task<bool[]> InsertAsync(RedisKey key, RedisValue[] items, int? capacity = null,
        double? error = null, int? expansion = null,
        bool nocreate = false, bool nonscaling = false)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.Insert(key, items, capacity, error, expansion, nocreate, nonscaling))).ToBooleanArray();
    }

    /// <inheritdoc/>
    public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.LoadChunk(key, iterator, data))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool[]> MAddAsync(RedisKey key, params RedisValue[] items)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.MAdd(key, items))).ToBooleanArray();
    }

    /// <inheritdoc/>
    public async Task<bool[]> MExistsAsync(RedisKey key, RedisValue[] items)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.MExists(key, items))).ToBooleanArray();
    }

    /// <inheritdoc/>
    public async Task<bool> ReserveAsync(RedisKey key, double errorRate, long capacity,
        int? expansion = null, bool nonscaling = false)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.Reserve(key, errorRate, capacity, expansion, nonscaling))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<Tuple<long, Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
    {
        return (await db.ExecuteAsync(BloomCommandBuilder.ScanDump(key, iterator))).ToScanDumpTuple();
    }
}