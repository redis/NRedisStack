using NRedisStack.CountMinSketch.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{

    public class CmsCommands : ICmsCommands
    {
        IDatabase _db;
        public CmsCommands(IDatabase db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public long IncrBy(RedisKey key, RedisValue item, long increment)
        {
            return _db.Execute(CmsCommandBuilder.IncrBy(key, item, increment)).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> IncrByAsync(RedisKey key, RedisValue item, long increment)
        {
            return (await _db.ExecuteAsync(CmsCommandBuilder.IncrBy(key, item, increment))).ToLong();
        }

        /// <inheritdoc/>
        public long[] IncrBy(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
        {
            return _db.Execute(CmsCommandBuilder.IncrBy(key, itemIncrements)).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> IncrByAsync(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
        {
            return (await _db.ExecuteAsync(CmsCommandBuilder.IncrBy(key, itemIncrements))).ToLongArray();

        }

        /// <inheritdoc/>
        public CmsInformation Info(RedisKey key)
        {
            var info = _db.Execute(CmsCommandBuilder.Info(key));
            return info.ToCmsInfo();
        }

        /// <inheritdoc/>
        public async Task<CmsInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(CmsCommandBuilder.Info(key));
            return info.ToCmsInfo();
        }

        /// <inheritdoc/>
        public bool InitByDim(RedisKey key, long width, long depth)
        {
            return _db.Execute(CmsCommandBuilder.InitByDim(key, width, depth)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> InitByDimAsync(RedisKey key, long width, long depth)
        {
            return (await _db.ExecuteAsync(CmsCommandBuilder.InitByDim(key, width, depth))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool InitByProb(RedisKey key, double error, double probability)
        {
            return _db.Execute(CmsCommandBuilder.InitByProb(key, error, probability)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> InitByProbAsync(RedisKey key, double error, double probability)
        {
            return (await _db.ExecuteAsync(CmsCommandBuilder.InitByProb(key, error, probability))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool Merge(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
        {
            return _db.Execute(CmsCommandBuilder.Merge(destination, numKeys, source, weight)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> MergeAsync(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
        {
            return (await _db.ExecuteAsync(CmsCommandBuilder.Merge(destination, numKeys, source, weight))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public long[] Query(RedisKey key, params RedisValue[] items)
        {
            return _db.Execute(CmsCommandBuilder.Query(key, items)).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> QueryAsync(RedisKey key, params RedisValue[] items)
        {
            return (await _db.ExecuteAsync(CmsCommandBuilder.Query(key, items))).ToLongArray();
        }
    }
}