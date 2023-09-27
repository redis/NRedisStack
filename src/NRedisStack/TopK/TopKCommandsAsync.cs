using NRedisStack.TopK.DataTypes;
using StackExchange.Redis;
namespace NRedisStack

{
    public class TopKCommandsAsync : ITopKCommandsAsync
    {
        IDatabaseAsync _db;
        public TopKCommandsAsync(IDatabaseAsync db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]?> AddAsync(RedisKey key, params RedisValue[] items)
        {
            return (RedisResult[]?)await _db.ExecuteAsync(TopKCommandBuilder.Add(key, items));
        }

        /// <inheritdoc/>
        public async Task<long[]> CountAsync(RedisKey key, params RedisValue[] items)
        {
            return (await _db.ExecuteAsync(TopKCommandBuilder.Count(key, items))).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> IncrByAsync(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements)
        {
            return (await _db.ExecuteAsync(TopKCommandBuilder.IncrBy(key, itemIncrements))).ToArray();
        }

        /// <inheritdoc/>
        public async Task<TopKInformation> InfoAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TopKCommandBuilder.Info(key))).ToTopKInfo();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> ListAsync(RedisKey key, bool withcount = false)
        {
            return (await _db.ExecuteAsync(TopKCommandBuilder.List(key, withcount))).ToArray();
        }

        /// <inheritdoc/>
        public async Task<bool> QueryAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(TopKCommandBuilder.Query(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool[]> QueryAsync(RedisKey key, params RedisValue[] items)
        {
            return (await _db.ExecuteAsync(TopKCommandBuilder.Query(key, items))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool> ReserveAsync(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9)
        {
            return (await _db.ExecuteAsync(TopKCommandBuilder.Reserve(key, topk, width, depth, decay))).OKtoBoolean();
        }
    }
}