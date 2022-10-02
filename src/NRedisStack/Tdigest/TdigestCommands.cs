using NRedisStack.Tdigest.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack
{

    public class TdigestCommands
    {
        IDatabase _db;
        public TdigestCommands(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="value">The value of the observation.</param>
        /// <param name="weight">The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public bool Add(RedisKey key, double item, long weight)
        {
            if (weight < 0) throw new ArgumentOutOfRangeException(nameof(weight));

            return _db.Execute(TDIGEST.ADD, key, item, weight).OKtoBoolean();
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="value">The value of the observation.</param>
        /// <param name="weight">The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public async Task<bool> AddAsync(RedisKey key, double item, int weight)
        {
            if (weight < 0) throw new ArgumentOutOfRangeException(nameof(weight));

            var result = await _db.ExecuteAsync(TDIGEST.ADD, key, item, weight);
            return result.OKtoBoolean();
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="valueWeight">Tuple of the value of the observation and The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public bool Add(RedisKey key, params Tuple<double, long>[] valueWeight)
        {
            if (valueWeight.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(valueWeight));

            var args = new List<object> { key };

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentOutOfRangeException(nameof(pair.Item2));
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return _db.Execute(TDIGEST.ADD, args).OKtoBoolean();
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="valueWeight">Tuple of the value of the observation and The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public async Task<bool> AddAsync(RedisKey key, params Tuple<double, long>[] valueWeight)
        {
            if (valueWeight.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(valueWeight));

            var args = new List<object> { key };

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentOutOfRangeException(nameof(pair.Item2));
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return (await _db.ExecuteAsync(TDIGEST.ADD, args)).OKtoBoolean();
        }

        /// <summary>
        /// Estimate the fraction of all observations added which are <= value.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="values">upper limit of observation value.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
        public double[] CDF(RedisKey key, params double[] values)
        {
            var args = new List<object>(values.Length +1) { key };
            foreach(var value in values) args.Add(value);
            return _db.Execute(TDIGEST.CDF, args).ToDoubleArray();
        }

        /// <summary>
        /// Estimate the fraction of all observations added which are <= value.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="values">upper limit of observation value.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
        public async Task<double[]> CDFAsync(RedisKey key, params double[] values)
        {
            var args = new List<object>(values.Length +1) { key };
            foreach(var value in values) args.Add(value);
            return (await _db.ExecuteAsync(TDIGEST.CDF, args)).ToDoubleArray();
        }

        /// <summary>
        /// Allocate memory and initialize a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.create"/></remarks>
        public bool Create(RedisKey key, long compression = 100)
        {
            return _db.Execute(TDIGEST.CREATE, key, TdigestArgs.COMPRESSION, compression).OKtoBoolean();
        }

        /// <summary>
        /// Allocate memory and initialize a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.create"/></remarks>
        public async Task<bool> CreateAsync(RedisKey key, long compression = 100)
        {
            return (await _db.ExecuteAsync(TDIGEST.CREATE, key, TdigestArgs.COMPRESSION, compression)).OKtoBoolean();
        }

        /// <summary>
        /// Returns information about a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>information about a sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.info"/></remarks>
        public TdigestInformation Info(RedisKey key)
        {
            return _db.Execute(TDIGEST.INFO, key).ToTdigestInfo();
        }

        /// <summary>
        /// Returns information about a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>information about a sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.info"/></remarks>
        public async Task<TdigestInformation> InfoAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TDIGEST.INFO, key)).ToTdigestInfo();
        }


        /// <summary>
        /// Get the maximum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the maximum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.max"/></remarks>
        public double Max(RedisKey key)
        {
            var result = _db.Execute(TDIGEST.MAX, key);
            return result.ToDouble();
        }

        /// <summary>
        /// Get the maximum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the maximum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.max"/></remarks>
        public async Task<double> MaxAsync(RedisKey key)
        {
            var result = await _db.ExecuteAsync(TDIGEST.MAX, key);
            return result.ToDouble();
        }

        /// <summary>
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the minimum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.min"/></remarks>
        public double Min(RedisKey key)
        {
            return _db.Execute(TDIGEST.MIN, key).ToDouble();
        }

        /// <summary>
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the minimum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.min"/></remarks>
        public async Task<double> MinAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TDIGEST.MIN, key)).ToDouble();
        }

        /// <summary>
        /// Merges all of the values from 'from' keys to 'destination-key' sketch
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="compression">The compression parameter.</param>
        /// <param name="overide">If destination already exists, it is overwritten.</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public bool Merge(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentOutOfRangeException(nameof(sourceKeys));

            int numkeys = sourceKeys.Length;
            var args = new List<object>() { destinationKey, numkeys};
            foreach(var key in sourceKeys)
            {
                args.Add(key);
            }

            if (compression != default(long))
            {
                args.Add("COMPRESSION");
                args.Add(compression);
            }

            if(overide)
            {
                args.Add("OVERRIDE");
            }

            return _db.Execute(TDIGEST.MERGE, args).OKtoBoolean();
        }

        /// <summary>
        /// Merges all of the values from 'from' keys to 'destination-key' sketch
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="compression">The compression parameter.</param>
        /// <param name="overide">If destination already exists, it is overwritten.</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public async Task<bool> MergeAsync(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentOutOfRangeException(nameof(sourceKeys));

            int numkeys = sourceKeys.Length;
            var args = new List<object>() { destinationKey, numkeys};
            foreach(var key in sourceKeys)
            {
                args.Add(key);
            }

            if (compression != default(long))
            {
                args.Add("COMPRESSION");
                args.Add(compression);
            }

            if(overide)
            {
                args.Add("OVERRIDE");
            }

            return (await _db.ExecuteAsync(TDIGEST.MERGE, args)).OKtoBoolean();
        }

        /// <summary>
        /// Returns estimates of one or more cutoffs such that a specified fraction of the observations
        /// added to this t-digest would be less than or equal to each of the specified cutoffs.
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="quantile">The desired fraction (between 0 and 1 inclusively).</param>
        /// <returns>An  array of results populated with quantile_1, cutoff_1, quantile_2, cutoff_2, ..., quantile_N, cutoff_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.quantile"/></remarks>
        public double[] Quantile(RedisKey key, params double[] quantile)
        {
            if (quantile.Length < 1) throw new ArgumentOutOfRangeException(nameof(quantile));

            var args = new List<object> { key };
            foreach (var q in quantile) args.Add(q);

            return _db.Execute(TDIGEST.QUANTILE, args).ToDoubleArray();
        }

        /// <summary>
        /// Returns estimates of one or more cutoffs such that a specified fraction of the observations
        ///added to this t-digest would be less than or equal to each of the specified cutoffs.
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="quantile">The desired fraction (between 0 and 1 inclusively).</param>
        /// <returns>An  array of results populated with quantile_1, cutoff_1, quantile_2, cutoff_2, ..., quantile_N, cutoff_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.quantile"/></remarks>
        public async Task<double[]> QuantileAsync(RedisKey key, params double[] quantile)
        {
            if (quantile.Length < 1) throw new ArgumentOutOfRangeException(nameof(quantile));

            var args = new List<object> { key };
            foreach (var q in quantile) args.Add(q);

            return (await _db.ExecuteAsync(TDIGEST.QUANTILE, args)).ToDoubleArray();
        }

        /// <summary>
        /// Retrieve the estimated rank of value (the number of observations in the sketch
        /// that are smaller than value + half the number of observations that are equal to value).
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="values">input value, for which the rank will be determined.</param>
        /// <returns>an array of results populated with rank_1, rank_2, ..., rank_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.rank"/></remarks>
        public long[] Rank(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return _db.Execute(TDIGEST.RANK, args).ToLongArray();
        }

        /// <summary>
        /// Retrieve the estimated rank of value (the number of observations in the sketch
        /// that are smaller than value + half the number of observations that are equal to value).
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="values">input value, for which the rank will be determined.</param>
        /// <returns>an array of results populated with rank_1, rank_2, ..., rank_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.rank"/></remarks>
        public async Task<long[]> RankAsync(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return (await _db.ExecuteAsync(TDIGEST.RANK, args)).ToLongArray();
        }

        /// <summary>
        /// Retrieve the estimated rank of value (the number of observations in the sketch
        /// that are larger than value + half the number of observations that are equal to value).
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="values">input value, for which the rank will be determined.</param>
        /// <returns>an array of results populated with rank_1, rank_2, ..., rank_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.revrank"/></remarks>
        public long[] RevRank(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return _db.Execute(TDIGEST.REVRANK, args).ToLongArray();
        }

        /// <summary>
        /// Retrieve the estimated rank of value (the number of observations in the sketch
        /// that are larger than value + half the number of observations that are equal to value).
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="values">input value, for which the rank will be determined.</param>
        /// <returns>an array of results populated with rank_1, rank_2, ..., rank_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.revrank"/></remarks>
        public async Task<long[]> RevRankAsync(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return ( await _db.ExecuteAsync(TDIGEST.REVRANK, args)).ToLongArray();
        }

        /// <summary>
        /// Retrieve an estimation of the value with the given the rank.
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="ranks">input rank, for which the value will be determined.</param>
        /// <returns>an array of results populated with value_1, value_2, ..., value_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.byrank"/></remarks>
        public double[] ByRank(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return _db.Execute(TDIGEST.BYRANK, args).ToDoubleArray();
        }

        /// <summary>
        /// Retrieve an estimation of the value with the given the rank.
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="ranks">input rank, for which the value will be determined.</param>
        /// <returns>an array of results populated with value_1, value_2, ..., value_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.byrank"/></remarks>
        public async Task<double[]> ByRankAsync(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return (await _db.ExecuteAsync(TDIGEST.BYRANK, args)).ToDoubleArray();
        }

        /// <summary>
        /// Retrieve an estimation of the value with the given the reverse rank.
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="ranks">input reverse rank, for which the value will be determined.</param>
        /// <returns>an array of results populated with value_1, value_2, ..., value_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.byrevrank"/></remarks>
        public double[] ByRevRank(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return _db.Execute(TDIGEST.BYREVRANK, args).ToDoubleArray();
        }

        /// <summary>
        /// Retrieve an estimation of the value with the given the reverse rank.
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="ranks">input reverse rank, for which the value will be determined.</param>
        /// <returns>an array of results populated with value_1, value_2, ..., value_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.byrevrank"/></remarks>
        public async Task<double[]> ByRevRankAsync(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return ( await _db.ExecuteAsync(TDIGEST.BYREVRANK, args)).ToDoubleArray();
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public bool Reset(RedisKey key)
        {
            return _db.Execute(TDIGEST.RESET, key).OKtoBoolean();
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public async Task<bool> ResetAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TDIGEST.RESET, key)).OKtoBoolean();
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="lowCutQuantile">Exclude observation values lower than this quantile.</param>
        /// <param name="highCutQuantile">Exclude observation values higher than this quantile.</param>
        /// <returns>estimation of the mean value. Will return NaN if the sketch is empty.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.trimmed_mean"/></remarks>
        public double TrimmedMean(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return _db.Execute(TDIGEST.TRIMMED_MEAN, key, lowCutQuantile, highCutQuantile).ToDouble();
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="lowCutQuantile">Exclude observation values lower than this quantile.</param>
        /// <param name="highCutQuantile">Exclude observation values higher than this quantile.</param>
        /// <returns>estimation of the mean value. Will return NaN if the sketch is empty.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.trimmed_mean"/></remarks>
        public async Task<double> TrimmedMeanAsync(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return (await _db.ExecuteAsync(TDIGEST.TRIMMED_MEAN, key, lowCutQuantile, highCutQuantile)).ToDouble();
        }
    }
}
