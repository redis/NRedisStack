using NRedisStack.TopK.DataTypes;
using StackExchange.Redis;
namespace NRedisStack

{
    public class TopKCommands : TopKCommandsAsync, ITopKCommands
    {
        IDatabase _db;
        public TopKCommands(IDatabase db) : base(db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public RedisResult[]? Add(RedisKey key, params RedisValue[] items)
        {
            return (RedisResult[]?)_db.Execute(TopKCommandBuilder.Add(key, items));
        }

        /// <inheritdoc/>
        public long[] Count(RedisKey key, params RedisValue[] items)
        {
            return _db.Execute(TopKCommandBuilder.Count(key, items)).ToLongArray();
        }

        /// <inheritdoc/>
        public RedisResult[] IncrBy(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements)
        {
            return _db.Execute(TopKCommandBuilder.IncrBy(key, itemIncrements)).ToArray();
        }

        /// <inheritdoc/>
        public TopKInformation Info(RedisKey key)
        {
            return _db.Execute(TopKCommandBuilder.Info(key)).ToTopKInfo();
        }

        /// <inheritdoc/>
        public RedisResult[] List(RedisKey key, bool withcount = false)
        {
            return _db.Execute(TopKCommandBuilder.List(key, withcount)).ToArray();
        }

        /// <inheritdoc/>
        public bool Query(RedisKey key, RedisValue item)
        {
            return _db.Execute(TopKCommandBuilder.Query(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public bool[] Query(RedisKey key, params RedisValue[] items)
        {
            return _db.Execute(TopKCommandBuilder.Query(key, items)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool Reserve(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9)
        {
            return _db.Execute(TopKCommandBuilder.Reserve(key, topk, width, depth, decay)).OKtoBoolean();
        }
    }
}
