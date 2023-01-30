using NRedisStack.CountMinSketch.DataTypes;
using StackExchange.Redis;

namespace NRedisStack
{
    public interface ICmsCommandsAsync
    {
        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="item">The item which counter is to be increased.</param>
        /// <param name="increment">Amount by which the item counter is to be increased.</param>
        /// <returns>Count of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
        Task<long> IncrByAsync(RedisKey key, RedisValue item, long increment);

        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="itemIncrements">Tuple of The items which counter is to be increased
        /// and the Amount by which the item counter is to be increased.</param>
        /// <returns>Count of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
        Task<long[]> IncrByAsync(RedisKey key, Tuple<RedisValue, long>[] itemIncrements);

        /// <summary>
        /// Return information about a sketch.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the sketch.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.info"/></remarks>
        Task<CmsInformation> InfoAsync(RedisKey key);

        /// <summary>
        /// Initializes a Count-Min Sketch to dimensions specified by user.
        /// </summary>
        /// <param name="key">TThe name of the sketch.</param>
        /// <param name="width">Number of counters in each array. Reduces the error size.</param>
        /// <param name="depth">Number of counter-arrays. Reduces the probability for an error
        /// of a certain size (percentage of total count).</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.initbydim"/></remarks>
        Task<bool> InitByDimAsync(RedisKey key, long width, long depth);

        /// <summary>
        /// Initializes a Count-Min Sketch to accommodate requested tolerances.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="error">Estimate size of error.</param>
        /// <param name="probability">The desired probability for inflated count.</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.initbyprob"/></remarks>
        Task<bool> InitByProbAsync(RedisKey key, double error, double probability);

        /// <summary>
        /// Merges several sketches into one sketch.
        /// </summary>
        /// <param name="destination">The name of destination sketch. Must be initialized</param>
        /// <param name="numKeys">Number of sketches to be merged.</param>
        /// <param name="source">Names of source sketches to be merged.</param>
        /// <param name="weight">Multiple of each sketch. Default = 1.</param>
        /// <returns><see langword="true"/> if if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.merge"/></remarks>
        Task<bool> MergeAsync(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null);

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="items">One or more items for which to return the count.</param>
        /// <returns>Array with a min-count of each of the items in the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/cms.query"/></remarks>
        Task<long[]> QueryAsync(RedisKey key, params RedisValue[] items);
    }
}