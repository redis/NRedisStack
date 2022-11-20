using NRedisStack.CountMinSketch.DataTypes;
using NRedisStack.Literals;
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
            return _db.Execute(CMS.INCRBY, key, item, increment).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> IncrByAsync(RedisKey key, RedisValue item, long increment)
        {
            var result = await _db.ExecuteAsync(CMS.INCRBY, key, item, increment);
            return result.ToLong();
        }

        /// <inheritdoc/>
        public long[] IncrBy(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
        {
            if (itemIncrements.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
            foreach (var pair in itemIncrements)
            {
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return _db.Execute(CMS.INCRBY, args).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> IncrByAsync(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
        {
            if (itemIncrements.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
            foreach (var pair in itemIncrements)
            {
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }

            var result = await _db.ExecuteAsync(CMS.INCRBY, args);
            return result.ToLongArray();
        }

        /// <inheritdoc/>
        public CmsInformation Info(RedisKey key)
        {
            var info = _db.Execute(CMS.INFO, key);
            return info.ToCmsInfo();
        }

        /// <inheritdoc/>
        public async Task<CmsInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(CMS.INFO, key);
            return info.ToCmsInfo();
        }

        /// <inheritdoc/>
        public bool InitByDim(RedisKey key, long width, long depth)
        {
            return _db.Execute(CMS.INITBYDIM, key, width, depth).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> InitByDimAsync(RedisKey key, long width, long depth)
        {
            var result = await _db.ExecuteAsync(CMS.INITBYDIM, key, width, depth);
            return result.OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool InitByProb(RedisKey key, double error, double probability)
        {
            return _db.Execute(CMS.INITBYPROB, key, error, probability).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> InitByProbAsync(RedisKey key, double error, double probability)
        {
            var result = await _db.ExecuteAsync(CMS.INITBYPROB, key, error, probability);
            return result.OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool Merge(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
        {
            if (source.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(source));

            List<object> args = new List<object> { destination, numKeys };

            foreach (var s in source) args.Add(s);

            if (weight != null && weight.Length >= 1)
            {
                args.Add(CmsArgs.WEIGHTS);
                foreach (var w in weight) args.Add(w);
            }

            return _db.Execute(CMS.MERGE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> MergeAsync(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
        {
            if (source.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(source));

            List<object> args = new List<object> { destination, numKeys };

            foreach (var s in source) args.Add(s);

            if (weight != null && weight.Length >= 1)
            {
                args.Add(CmsArgs.WEIGHTS);
                foreach (var w in weight) args.Add(w);
            }

            var result = await _db.ExecuteAsync(CMS.MERGE, args);
            return result.OKtoBoolean();
        }

        /// <inheritdoc/>
        public long[] Query(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };
            foreach (var item in items) args.Add(item);

            return _db.Execute(CMS.QUERY, args).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> QueryAsync(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };
            foreach (var item in items) args.Add(item);

            var result = await _db.ExecuteAsync(CMS.QUERY, args);
            return result.ToLongArray();
        }
    }
}
