using NRedisStack.Core.Tdigest.DataTypes;
using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
{

    public class TdigestCommands //TODO: Finish this
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

            return ResponseParser.ParseOKtoBoolean(_db.Execute(TDIGEST.ADD, key, item, weight));
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
            return ResponseParser.ParseOKtoBoolean(result);
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="valueWeight">Tuple of the value of the observation and The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public bool Add(RedisKey key, Tuple<double,double>[] valueWeight)
        {
            var args = new List<object> {key};

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentException(nameof(pair.Item2));
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return ResponseParser.ParseOKtoBoolean(_db.Execute(TDIGEST.ADD, args));
        }

        /// <summary>
        /// Adds one or more observations to a t-digest sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="valueWeight">Tuple of the value of the observation and The weight of this observation.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.add"/></remarks>
        public async Task<bool> AddAsync(RedisKey key, Tuple<double,double>[] valueWeight)
        {
            var args = new List<object> {key};

            foreach (var pair in valueWeight)
            {
                if (pair.Item2 < 0) throw new ArgumentException(nameof(pair.Item2));
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return ResponseParser.ParseOKtoBoolean(await _db.ExecuteAsync(TDIGEST.ADD, args));
        }

        /// <summary>
        /// Estimate the fraction of all observations added which are <= value.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="value">upper limit of observation value.</param>
        /// <returns>double-reply - estimation of the fraction of all observations added which are <= value</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.cdf"/></remarks>
        public double? CDF(RedisKey key, double item)
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
        public async Task<double?> CDF(RedisKey key, double item, double weight)
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
        public double? Create(RedisKey key, long compression = 100)
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
        public async Task<double?> CreateAsync(RedisKey key, long compression = 100)
        {
            return ResponseParser.ToDouble(await _db.ExecuteAsync(TDIGEST.CREATE, key, compression));
        }

        /// <summary>
        /// Returns information about a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>information about a sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.info"/></remarks>
        public TdigestInformation? Info(RedisKey key)
        {
            return ResponseParser.ToTdigestInfo(_db.Execute(TDIGEST.INFO, key));
        }

        /// <summary>
        /// Returns information about a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <returns>information about a sketch</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.info"/></remarks>
        public async Task<TdigestInformation?> InfoAsync(RedisKey key)
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
            return ResponseParser.ParseOKtoBoolean(.Execute(TDIGEST.MERGE, destinationKey, sourceKey));
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
            return ResponseParser.ParseOKtoBoolean(result);
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

            return ResponseParser.ParseOKtoBoolean(_db.Execute(TDIGEST.MERGE, args));
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

            var result =  await _db.ExecuteAsync(TDIGEST.MERGE, args);
            return ResponseParser.ParseOKtoBoolean(result);
        }

        /// <summary>
        /// Merges all of the values from 'from' keys to 'destination-key' sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="numkeys">Number of sketch(es) to copy observation values from.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public bool MergeStore(RedisKey destinationKey, long numkeys, long compression = 100, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentException(nameof(sourceKeys));

            var args = new List<object>{destinationKey, numkeys};
            foreach (var key in sourceKeys) args.Add(key);
            args.Add(TdigestArgs.COMPRESSION);
            args.Add(compression);

            return ResponseParser.ParseOKtoBoolean(_db.Execute(TDIGEST.MERGE, args));
        }

        /// <summary>
        /// Merges all of the values from 'from' keys to 'destination-key' sketch.
        /// </summary>
        /// <param name="destinationKey">TSketch to copy observation values to (a t-digest data structure).</param>
        /// <param name="numkeys">Number of sketch(es) to copy observation values from.</param>
        /// <param name="compression">The compression parameter.</param>
        /// <param name="sourceKeys">Sketch to copy observation values from (a t-digest data structure).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/tdigest.merge"/></remarks>
        public async Task<bool> MergeStoreAsync(RedisKey destinationKey, long numkeys, long compression = 100, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentException(nameof(sourceKeys));

            var args = new List<object>{destinationKey, numkeys};
            foreach (var key in sourceKeys) args.Add(key);
            args.Add(TdigestArgs.COMPRESSION);
            args.Add(compression);

            var result = await _db.ExecuteAsync(TDIGEST.MERGE, args);
            return ResponseParser.ParseOKtoBoolean(result);
        }








    }
}
