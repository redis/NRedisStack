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
        public bool Add(RedisKey key, double item, double weight)
        {
            if (weight < 0) throw new ArgumentException(nameof(weight));

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
        public async Task<bool> AddAsync(RedisKey key, double item, double weight)
        {
            if (weight < 0) throw new ArgumentException(nameof(weight));

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
        public bool Add(RedisKey key, params Tuple<double, double>[] valueWeight)
        {
            if (valueWeight.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(valueWeight));

            var args = new List<object> { key };

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentException(nameof(pair.Item2));
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
        public async Task<bool> AddAsync(RedisKey key, params Tuple<double, double>[] valueWeight)
        {
            if (valueWeight.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(valueWeight));

            var args = new List<object> { key };

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentException(nameof(pair.Item2));
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return (await _db.ExecuteAsync(TDIGEST.ADD, args)).OKtoBoolean();
        }

        /// <summary>
        /// Estimate the fraction of all observations added which are <= value.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="value">upper limit of observation value.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
        public double CDF(RedisKey key, double value)
        {
            return _db.Execute(TDIGEST.CDF, key, value).ToDouble();
        }

        /// <summary>
        /// Estimate the fraction of all observations added which are <= value.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="value">upper limit of observation value.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
        public async Task<double> CDFAsync(RedisKey key, double value)
        {
            var result = await _db.ExecuteAsync(TDIGEST.CDF, key, value);
            return result.ToDouble();
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
            return _db.Execute(TDIGEST.MAX, key).ToDouble();
        }

        /// <summary>
        /// Get the maximum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the maximum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.max"/></remarks>
        public async Task<double> MaxAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TDIGEST.MAX, key)).ToDouble();
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
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="sourceKey">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public bool Merge(RedisKey destinationKey, RedisKey sourceKey)
        {
            return _db.Execute(TDIGEST.MERGE, destinationKey, sourceKey).OKtoBoolean();
        }

        /// <summary>
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="sourceKey">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public async Task<bool> MergeAsync(RedisKey destinationKey, RedisKey sourceKey)
        {
            var result = await _db.ExecuteAsync(TDIGEST.MERGE, destinationKey, sourceKey);
            return result.OKtoBoolean();
        }

        /// <summary>
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public bool Merge(RedisKey destinationKey, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentException(nameof(sourceKeys));

            var args = sourceKeys.ToList();
            args.Insert(0, destinationKey);

            return _db.Execute(TDIGEST.MERGE, args).OKtoBoolean();
        }

        /// <summary>
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public async Task<bool> MergeAsync(RedisKey destinationKey, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentException(nameof(sourceKeys));

            var args = sourceKeys.ToList();
            args.Insert(0, destinationKey);

            var result = await _db.ExecuteAsync(TDIGEST.MERGE, args);
            return result.OKtoBoolean();
        }

        /// <summary>
        /// Merges all of the values from 'from' keys to 'destination-key' sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="numkeys">Number of sketch(es) to copy observation values from.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.mergestore"/></remarks>
        public bool MergeStore(RedisKey destinationKey, long numkeys, long compression = 100, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentException(nameof(sourceKeys));

            var args = new List<object> { destinationKey, numkeys };
            foreach (var key in sourceKeys) args.Add(key);
            args.Add(TdigestArgs.COMPRESSION);
            args.Add(compression);

            return _db.Execute(TDIGEST.MERGESTORE, args).OKtoBoolean();
        }

        /// <summary>
        /// Merges all of the values from 'from' keys to 'destination-key' sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="numkeys">Number of sketch(es) to copy observation values from.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.mergestore"/></remarks>
        public async Task<bool> MergeStoreAsync(RedisKey destinationKey, long numkeys, long compression = 100, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentException(nameof(sourceKeys));

            var args = new List<object> { destinationKey, numkeys };
            foreach (var key in sourceKeys) args.Add(key);
            args.Add(TdigestArgs.COMPRESSION);
            args.Add(compression);

            var result = await _db.ExecuteAsync(TDIGEST.MERGESTORE, args);
            return result.OKtoBoolean();
        }

        /// <summary>
        /// Returns estimates of one or more cutoffs such that a specified fraction of the observations
        ///added to this t-digest would be less than or equal to each of the specified cutoffs.
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="quantile">The desired fraction (between 0 and 1 inclusively).</param>
        /// <returns>An  array of results populated with quantile_1, cutoff_1, quantile_2, cutoff_2, ..., quantile_N, cutoff_N.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.quantile"/></remarks>
        public double[] Quantile(RedisKey key, params double[] quantile)
        {
            if (quantile.Length < 1) throw new ArgumentException(nameof(quantile));

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
            if (quantile.Length < 1) throw new ArgumentException(nameof(quantile));

            var args = new List<object> { key };
            foreach (var q in quantile) args.Add(q);

            return (await _db.ExecuteAsync(TDIGEST.QUANTILE, args)).ToDoubleArray();
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public bool Reset(RedisKey key, params double[] quantile)
        {
            return _db.Execute(TDIGEST.RESET, key).OKtoBoolean();
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public async Task<bool> ResetAsync(RedisKey key, params double[] quantile)
        {
            return (await _db.ExecuteAsync(TDIGEST.RESET, key)).OKtoBoolean();
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="lowCutQuantile">Exclude observation values lower than this quantile.</param>
        /// <param name="highCutQuantile">Exclude observation values higher than this quantile.</param>
        /// <returns>estimation of the mean value. Will return DBL_MAX if the sketch is empty.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
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
        /// <returns>estimation of the mean value. Will return DBL_MAX if the sketch is empty.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public async Task<double> TrimmedMeanAsync(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return (await _db.ExecuteAsync(TDIGEST.TRIMMED_MEAN, key, lowCutQuantile, highCutQuantile)).ToDouble();
        }













    }
}
