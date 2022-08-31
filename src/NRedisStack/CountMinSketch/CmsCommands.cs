using NRedisStack.CountMinSketch.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack
{

    public class CmsCommands
    {
        IDatabase _db;
        public CmsCommands(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="item">The item which counter is to be increased.</param>
        /// <param name="increment">Amount by which the item counter is to be increased.</param>
        /// <returns>Count of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
        public long IncrBy(RedisKey key, RedisValue item, long increment)
        {
            return _db.Execute(CMS.INCRBY, key, item, increment).ToLong();
        }

        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="item">The item which counter is to be increased.</param>
        /// <param name="increment">Amount by which the item counter is to be increased.</param>
        /// <returns>Count of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
        public async Task<long> IncrByAsync(RedisKey key, RedisValue item, long increment)
        {
            var result = await _db.ExecuteAsync(CMS.INCRBY, key, item, increment);
            return result.ToLong();
        }

        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="itemIncrements">Tuple of The items which counter is to be increased
        /// and the Amount by which the item counter is to be increased.</param>
        /// <returns>Count of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
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

        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="itemIncrements">Tuple of The items which counter is to be increased
        /// and the Amount by which the item counter is to be increased.</param>
        /// <returns>Count of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
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

        /// <summary>
        /// Return information about a sketch.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the sketch.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.info"/></remarks>
        public CmsInformation Info(RedisKey key)
        {
            var info = _db.Execute(CMS.INFO, key);
            return info.ToCmsInfo();
        }

        /// <summary>
        /// Return information about a sketch.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the sketch.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.info"/></remarks>
        public async Task<CmsInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(CMS.INFO, key);
            return info.ToCmsInfo();
        }

        /// <summary>
        /// Initializes a Count-Min Sketch to dimensions specified by user.
        /// </summary>
        /// <param name="key">TThe name of the sketch.</param>
        /// <param name="width">Number of counters in each array. Reduces the error size.</param>
        /// <param name="depth">Number of counter-arrays. Reduces the probability for an error
        /// of a certain size (percentage of total count).</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.initbydim"/></remarks>
        public bool InitByDim(RedisKey key, long width, long depth)
        {
            return _db.Execute(CMS.INITBYDIM, key, width, depth).OKtoBoolean();
        }

        /// <summary>
        /// Initializes a Count-Min Sketch to dimensions specified by user.
        /// </summary>
        /// <param name="key">TThe name of the sketch.</param>
        /// <param name="width">Number of counters in each array. Reduces the error size.</param>
        /// <param name="depth">Number of counter-arrays. Reduces the probability for an error
        /// of a certain size (percentage of total count).</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.initbydim"/></remarks>
        public async Task<bool> InitByDimAsync(RedisKey key, long width, long depth)
        {
            var result = await _db.ExecuteAsync(CMS.INITBYDIM, key, width, depth);
            return result.OKtoBoolean();
        }

        /// <summary>
        /// Initializes a Count-Min Sketch to accommodate requested tolerances.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="error">Estimate size of error.</param>
        /// <param name="probability">The desired probability for inflated count.</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.initbyprob"/></remarks>
        public bool InitByProb(RedisKey key, double error, double probability)
        {
            return _db.Execute(CMS.INITBYPROB, key, error, probability).OKtoBoolean();
        }

        /// <summary>
        /// Initializes a Count-Min Sketch to accommodate requested tolerances.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="error">Estimate size of error.</param>
        /// <param name="probability">The desired probability for inflated count.</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.initbyprob"/></remarks>
        public async Task<bool> InitByProbAsync(RedisKey key, double error, double probability)
        {
            var result = await _db.ExecuteAsync(CMS.INITBYPROB, key, error, probability);
            return result.OKtoBoolean();
        }

        /// <summary>
        /// Merges several sketches into one sketch.
        /// </summary>
        /// <param name="destination">The name of destination sketch. Must be initialized</param>
        /// <param name="numKeys">Number of sketches to be merged.</param>
        /// <param name="source">Names of source sketches to be merged.</param>
        /// <param name="weight">Multiple of each sketch. Default = 1.</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.merge"/></remarks>
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

        /// <summary>
        /// Merges several sketches into one sketch.
        /// </summary>
        /// <param name="destination">The name of destination sketch. Must be initialized</param>
        /// <param name="numKeys">Number of sketches to be merged.</param>
        /// <param name="source">Names of source sketches to be merged.</param>
        /// <param name="weight">Multiple of each sketch. Default = 1.</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.merge"/></remarks>
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

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="items">One or more items for which to return the count.</param>
        /// <returns>Array with a min-count of each of the items in the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.query"/></remarks>
        public long[] Query(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };
            foreach (var item in items) args.Add(item);

            return _db.Execute(CMS.QUERY, args).ToLongArray();
        }

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="items">One or more items for which to return the count.</param>
        /// <returns>Array with a min-count of each of the items in the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.query"/></remarks>
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
