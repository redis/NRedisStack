using NRedisStack.TopK.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack

{
    public class TopKCommands : ITopKCommands
    {
        IDatabase _db;
        public TopKCommands(IDatabase db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public RedisResult[]? Add(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);
            return (RedisResult[]?)_db.Execute(TOPK.ADD, args);
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]?> AddAsync(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);

            var result = await _db.ExecuteAsync(TOPK.ADD, args);
            return (RedisResult[]?)result;
        }

        /// <inheritdoc/>
        public long[] Count(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);
            return _db.Execute(TOPK.COUNT, args).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> CountAsync(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);
            var result = await _db.ExecuteAsync(TOPK.COUNT, args);
            return result.ToLongArray();
        }


        /// <inheritdoc/>
        public RedisResult[] IncrBy(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements)
        {
            if (itemIncrements.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
            foreach (var pair in itemIncrements)
            {
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return _db.Execute(TOPK.INCRBY, args).ToArray();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> IncrByAsync(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements)
        {
            if (itemIncrements.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
            foreach (var pair in itemIncrements)
            {
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }

            var result = await _db.ExecuteAsync(TOPK.INCRBY, args);
            return result.ToArray();
        }

        /// <inheritdoc/>
        public TopKInformation Info(RedisKey key)
        {
            var info = _db.Execute(TOPK.INFO, key);
            return info.ToTopKInfo();
        }

        /// <inheritdoc/>
        public async Task<TopKInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(TOPK.INFO, key);
            return info.ToTopKInfo();
        }

        /// <inheritdoc/>
        public RedisResult[] List(RedisKey key, bool withcount = false)
        {
            var result = (withcount) ? _db.Execute(TOPK.LIST, key, "WITHCOUNT")
                                     : _db.Execute(TOPK.LIST, key);
            return result.ToArray();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> ListAsync(RedisKey key, bool withcount = false)
        {
            var result = await((withcount) ? _db.ExecuteAsync(TOPK.LIST, key, "WITHCOUNT")
                                            : _db.ExecuteAsync(TOPK.LIST, key));
            return result.ToArray();
        }

        /// <inheritdoc/>
        public bool Query(RedisKey key, RedisValue item)
        {
            return _db.Execute(TOPK.QUERY, key, item).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> QueryAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(TOPK.QUERY, key, item);
            return result.ToString() == "1";
        }

        /// <inheritdoc/>
        public bool[] Query(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);

            return _db.Execute(TOPK.QUERY, args).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> QueryAsync(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);

            var result = await _db.ExecuteAsync(TOPK.QUERY, args);
            return result.ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool Reserve(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9)
        {
            return _db.Execute(TOPK.RESERVE, key, topk, width, depth, decay).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReserveAsync(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9)
        {
            var result = await _db.ExecuteAsync(TOPK.RESERVE, key, topk, width, depth, decay);
            return result.OKtoBoolean();
        }
    }
}
