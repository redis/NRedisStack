using NRedisStack.Tdigest.DataTypes;
using StackExchange.Redis;
namespace NRedisStack;

public interface ITdigestCommandsAsync
{
    /// <summary>
    /// Adds one or more observations to a t-digest sketch.
    /// </summary>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="values">The value of the observation.</param>
    /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
    Task<bool> AddAsync(RedisKey key, params double[] values);

    /// <summary>
    /// Estimate the fraction of all observations added which are &lt;= value.
    /// </summary>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="values">upper limit of observation value.</param>
    /// <returns>double-reply - estimation of the fraction of all observations added which are &lt;= value</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
    Task<double[]> CDFAsync(RedisKey key, params double[] values);

    /// <summary>
    /// Allocate memory and initialize a t-digest sketch.
    /// </summary>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="compression">The compression parameter.</param>
    /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.create"/></remarks>
    Task<bool> CreateAsync(RedisKey key, long compression = 100);
    /// <summary>
    /// Returns information about a sketch.
    /// </summary>
    /// <param name="key">The name of the sketch.</param>
    /// <returns>information about a sketch</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.info"/></remarks>
    Task<TdigestInformation> InfoAsync(RedisKey key);

    /// <summary>
    /// Get the maximum observation value from the sketch.
    /// </summary>
    /// <param name="key">The name of the sketch.</param>
    /// <returns>the maximum observation value from the sketch</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.max"/></remarks>
    Task<double> MaxAsync(RedisKey key);

    /// <summary>
    /// Get the minimum observation value from the sketch.
    /// </summary>
    /// <param name="key">The name of the sketch.</param>
    /// <returns>the minimum observation value from the sketch</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.min"/></remarks>
    Task<double> MinAsync(RedisKey key);

    /// <summary>
    /// Merges all of the values from 'from' keys to 'destination-key' sketch
    /// </summary>
    /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
    /// <param name="compression">The compression parameter.</param>
    /// <param name="overide">If destination already exists, it is overwritten.</param>
    /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
    /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
    Task<bool> MergeAsync(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys);

    /// <summary>
    /// Returns estimates of one or more cutoffs such that a specified fraction of the observations
    ///added to this t-digest would be less than or equal to each of the specified cutoffs.
    /// </summary>
    /// <param name="key">The name of the sketch (a t-digest data structure).</param>
    /// <param name="quantile">The desired fraction (between 0 and 1 inclusively).</param>
    /// <returns>An  array of results populated with quantile_1, cutoff_1, quantile_2, cutoff_2, ..., quantile_N, cutoff_N.</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.quantile"/></remarks>
    Task<double[]> QuantileAsync(RedisKey key, params double[] quantile);

    /// <summary>
    /// Retrieve the estimated rank of value (the number of observations in the sketch
    /// that are smaller than value + half the number of observations that are equal to value).
    /// </summary>
    /// <param name="key">The name of the sketch (a t-digest data structure).</param>
    /// <param name="values">input value, for which the rank will be determined.</param>
    /// <returns>an array of results populated with rank_1, rank_2, ..., rank_N.</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.rank"/></remarks>
    Task<long[]> RankAsync(RedisKey key, params long[] values);

    /// <summary>
    /// Retrieve the estimated rank of value (the number of observations in the sketch
    /// that are larger than value + half the number of observations that are equal to value).
    /// </summary>
    /// <param name="key">The name of the sketch (a t-digest data structure).</param>
    /// <param name="values">input value, for which the rank will be determined.</param>
    /// <returns>an array of results populated with rank_1, rank_2, ..., rank_N.</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.revrank"/></remarks>
    Task<long[]> RevRankAsync(RedisKey key, params long[] values);

    /// <summary>
    /// Retrieve an estimation of the value with the given the rank.
    /// </summary>
    /// <param name="key">The name of the sketch (a t-digest data structure).</param>
    /// <param name="ranks">input rank, for which the value will be determined.</param>
    /// <returns>an array of results populated with value_1, value_2, ..., value_N.</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.byrank"/></remarks>
    Task<double[]> ByRankAsync(RedisKey key, params long[] ranks);

    /// <summary>
    /// Retrieve an estimation of the value with the given the reverse rank.
    /// </summary>
    /// <param name="key">The name of the sketch (a t-digest data structure).</param>
    /// <param name="ranks">input reverse rank, for which the value will be determined.</param>
    /// <returns>an array of results populated with value_1, value_2, ..., value_N.</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.byrevrank"/></remarks>
    Task<double[]> ByRevRankAsync(RedisKey key, params long[] ranks);

    /// <summary>
    /// Reset the sketch - empty the sketch and re-initialize it
    /// </summary>
    /// <param name="key">The name of the sketch (a t-digest data structure).</param>
    /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
    Task<bool> ResetAsync(RedisKey key);

    /// <summary>
    /// Reset the sketch - empty the sketch and re-initialize it
    /// </summary>
    /// <param name="key">The name of the sketch (a t-digest data structure).</param>
    /// <param name="lowCutQuantile">Exclude observation values lower than this quantile.</param>
    /// <param name="highCutQuantile">Exclude observation values higher than this quantile.</param>
    /// <returns>estimation of the mean value. Will return NaN if the sketch is empty.</returns>
    /// <remarks><seealso href="https://redis.io/commands/tdigest.trimmed_mean"/></remarks>
    Task<double> TrimmedMeanAsync(RedisKey key, double lowCutQuantile, double highCutQuantile);
}