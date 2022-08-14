using NRedisStack.Core.Tdigest.DataTypes;
using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
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

            return ResponseParser.OKtoBoolean(_db.Execute(TDIGEST.ADD, key, item, weight));
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

            var result = await _db.ExecuteAsync(TDIGEST.ADD, key, item);
            return ResponseParser.OKtoBoolean(result);
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="valueWeight">Tuple of the value of the observation and The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public bool Add(RedisKey key, Tuple<double, double>[] valueWeight)
        {
            var args = new List<object> { key };

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentException(nameof(pair.Item2));
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return ResponseParser.OKtoBoolean(_db.Execute(TDIGEST.ADD, args));
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="valueWeight">Tuple of the value of the observation and The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public async Task<bool> AddAsync(RedisKey key, Tuple<double, double>[] valueWeight)
        {
            var args = new List<object> { key };

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentException(nameof(pair.Item2));
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return ResponseParser.OKtoBoolean(await _db.ExecuteAsync(TDIGEST.ADD, args));
        }

        /// <summary>
        /// Estimate the fraction of all observations added which are <= value.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="value">upper limit of observation value.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
        public double CDF(RedisKey key, double item)
        {
            return ResponseParser.ToDouble(_db.Execute(TDIGEST.ADD, key, item));
        }

        /// <summary>
        /// Estimate the fraction of all observations added which are <= value.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="value">upper limit of observation value.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
        public async Task<double> CDF(RedisKey key, double item, double weight)
        {
            var result = await _db.ExecuteAsync(TDIGEST.ADD, key, item);
            return ResponseParser.ToDouble(result);
        }

        /// <summary>
        /// Allocate memory and initialize a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.create"/></remarks>
        public double Create(RedisKey key, long compression = 100)
        {
            return ResponseParser.ToDouble(_db.Execute(TDIGEST.CREATE, key, compression));
        }

        /// <summary>
        /// Allocate memory and initialize a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.create"/></remarks>
        public async Task<double> CreateAsync(RedisKey key, long compression = 100)
        {
            return ResponseParser.ToDouble(await _db.ExecuteAsync(TDIGEST.CREATE, key, compression));
        }

        /// <summary>
        /// Returns information about a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>information about a sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.info"/></remarks>
        public TdigestInformation Info(RedisKey key)
        {
            return ResponseParser.ToTdigestInfo(_db.Execute(TDIGEST.INFO, key));
        }

        /// <summary>
        /// Returns information about a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>information about a sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.info"/></remarks>
        public async Task<TdigestInformation> InfoAsync(RedisKey key)
        {
            return ResponseParser.ToTdigestInfo(await _db.ExecuteAsync(TDIGEST.INFO, key));
        }


        /// <summary>
        /// Get the maximum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the maximum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.max"/></remarks>
        public RedisResult Max(RedisKey key)
        {
            return _db.Execute(TDIGEST.MAX, key);
        }

        /// <summary>
        /// Get the maximum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the maximum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.max"/></remarks>
        public async Task<RedisResult> MaxAsync(RedisKey key)
        {
            return await _db.ExecuteAsync(TDIGEST.MAX, key);
        }

        /// <summary>
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the minimum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.min"/></remarks>
        public RedisResult Min(RedisKey key)
        {
            return _db.Execute(TDIGEST.MIN, key);
        }

        /// <summary>
        /// Get the minimum observation value from the sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>the minimum observation value from the sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.min"/></remarks>
        public async Task<RedisResult> MinAsync(RedisKey key)
        {
            return await _db.ExecuteAsync(TDIGEST.MIN, key);
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
            return ResponseParser.OKtoBoolean(_db.Execute(TDIGEST.MERGE, destinationKey, sourceKey));
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
            return ResponseParser.OKtoBoolean(result);
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

            return ResponseParser.OKtoBoolean(_db.Execute(TDIGEST.MERGE, args));
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
            return ResponseParser.OKtoBoolean(result);
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

            return ResponseParser.OKtoBoolean(_db.Execute(TDIGEST.MERGE, args));
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

            var result = await _db.ExecuteAsync(TDIGEST.MERGE, args);
            return ResponseParser.OKtoBoolean(result);
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

            return ResponseParser.ToDoubleArray(_db.Execute(TDIGEST.QUANTILE, args));
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

            return ResponseParser.ToDoubleArray(await _db.ExecuteAsync(TDIGEST.QUANTILE, args));
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public bool Reset(RedisKey key, params double[] quantile)
        {
            return ResponseParser.OKtoBoolean(_db.Execute(TDIGEST.RESET, key));
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public async Task<bool> ResetAsync(RedisKey key, params double[] quantile)
        {
            return ResponseParser.OKtoBoolean(await _db.ExecuteAsync(TDIGEST.RESET, key));
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="lowCutQuantile">Exclude observation values lower than this quantile.</param>
        /// <param name="highCutQuantile">Exclude observation values higher than this quantile.</param>
        /// <returns>estimation of the mean value. Will return DBL_MAX if the sketch is empty.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public RedisResult TrimmedMean(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return _db.Execute(TDIGEST.RESET, key, lowCutQuantile, highCutQuantile);
        }

        /// <summary>
        /// Reset the sketch - empty the sketch and re-initialize it
        /// </summary>
        /// <param name="key">The name of the sketch (a t-digest data structure).</param>
        /// <param name="lowCutQuantile">Exclude observation values lower than this quantile.</param>
        /// <param name="highCutQuantile">Exclude observation values higher than this quantile.</param>
        /// <returns>estimation of the mean value. Will return DBL_MAX if the sketch is empty.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.reset"/></remarks>
        public async Task<RedisResult> TrimmedMeanAsync(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return await _db.ExecuteAsync(TDIGEST.RESET, key, lowCutQuantile, highCutQuantile);
        }













    }
}
