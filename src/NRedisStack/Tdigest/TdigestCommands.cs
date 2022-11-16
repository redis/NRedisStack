using NRedisStack.Tdigest.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack
{

    public class TdigestCommands : ITdigestCommands
    {
        IDatabase _db;
        public TdigestCommands(IDatabase db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public bool Add(RedisKey key, params double[] values)
        {
            if (values.Length < 0) throw new ArgumentOutOfRangeException(nameof(values));
            var args = new string[values.Length + 1];
            args[0] = key.ToString();
            for (int i = 0; i < values.Length; i++)
            {
                args[i + 1] = values[i].ToString();
            }

            return _db.Execute(TDIGEST.ADD, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> AddAsync(RedisKey key, params double[] values)
        {
            if (values.Length < 0) throw new ArgumentOutOfRangeException(nameof(values));
            var args = new string[values.Length + 1];
            args[0] = key;
            for (int i = 0; i < values.Length; i++)
            {
                args[i + 1] = values[i].ToString();
            }

            return (await _db.ExecuteAsync(TDIGEST.ADD, args)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public double[] CDF(RedisKey key, params double[] values)
        {
            var args = new List<object>(values.Length + 1) { key };
            foreach (var value in values) args.Add(value);
            return _db.Execute(TDIGEST.CDF, args).ToDoubleArray();
        }

        /// <inheritdoc/>
        public async Task<double[]> CDFAsync(RedisKey key, params double[] values)
        {
            var args = new List<object>(values.Length + 1) { key };
            foreach (var value in values) args.Add(value);
            return (await _db.ExecuteAsync(TDIGEST.CDF, args)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public bool Create(RedisKey key, long compression = 100)
        {
            return _db.Execute(TDIGEST.CREATE, key, TdigestArgs.COMPRESSION, compression).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> CreateAsync(RedisKey key, long compression = 100)
        {
            return (await _db.ExecuteAsync(TDIGEST.CREATE, key, TdigestArgs.COMPRESSION, compression)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public TdigestInformation Info(RedisKey key)
        {
            return _db.Execute(TDIGEST.INFO, key).ToTdigestInfo();
        }

        /// <inheritdoc/>
        public async Task<TdigestInformation> InfoAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TDIGEST.INFO, key)).ToTdigestInfo();
        }


        /// <inheritdoc/>
        public double Max(RedisKey key)
        {
            var result = _db.Execute(TDIGEST.MAX, key);
            return result.ToDouble();
        }

        /// <inheritdoc/>
        public async Task<double> MaxAsync(RedisKey key)
        {
            var result = await _db.ExecuteAsync(TDIGEST.MAX, key);
            return result.ToDouble();
        }

        /// <inheritdoc/>
        public double Min(RedisKey key)
        {
            return _db.Execute(TDIGEST.MIN, key).ToDouble();
        }

        /// <inheritdoc/>
        public async Task<double> MinAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TDIGEST.MIN, key)).ToDouble();
        }

        /// <inheritdoc/>
        public bool Merge(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentOutOfRangeException(nameof(sourceKeys));

            int numkeys = sourceKeys.Length;
            var args = new List<object>() { destinationKey, numkeys };
            foreach (var key in sourceKeys)
            {
                args.Add(key);
            }

            if (compression != default(long))
            {
                args.Add("COMPRESSION");
                args.Add(compression);
            }

            if (overide)
            {
                args.Add("OVERRIDE");
            }

            return _db.Execute(TDIGEST.MERGE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> MergeAsync(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys)
        {
            if (sourceKeys.Length < 1) throw new ArgumentOutOfRangeException(nameof(sourceKeys));

            int numkeys = sourceKeys.Length;
            var args = new List<object>() { destinationKey, numkeys };
            foreach (var key in sourceKeys)
            {
                args.Add(key);
            }

            if (compression != default(long))
            {
                args.Add("COMPRESSION");
                args.Add(compression);
            }

            if (overide)
            {
                args.Add("OVERRIDE");
            }

            return (await _db.ExecuteAsync(TDIGEST.MERGE, args)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public double[] Quantile(RedisKey key, params double[] quantile)
        {
            if (quantile.Length < 1) throw new ArgumentOutOfRangeException(nameof(quantile));

            var args = new List<object> { key };
            foreach (var q in quantile) args.Add(q);

            return _db.Execute(TDIGEST.QUANTILE, args).ToDoubleArray();
        }

        ///added to this t-digest would be less than or equal to each of the specified cutoffs.
        /// <inheritdoc/>
        public async Task<double[]> QuantileAsync(RedisKey key, params double[] quantile)
        {
            if (quantile.Length < 1) throw new ArgumentOutOfRangeException(nameof(quantile));

            var args = new List<object> { key };
            foreach (var q in quantile) args.Add(q);

            return (await _db.ExecuteAsync(TDIGEST.QUANTILE, args)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public long[] Rank(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return _db.Execute(TDIGEST.RANK, args).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> RankAsync(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return (await _db.ExecuteAsync(TDIGEST.RANK, args)).ToLongArray();
        }

        /// <inheritdoc/>
        public long[] RevRank(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return _db.Execute(TDIGEST.REVRANK, args).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> RevRankAsync(RedisKey key, params long[] values)
        {
            if (values.Length < 1) throw new ArgumentOutOfRangeException(nameof(values));

            var args = new List<object>(values.Length + 1) { key };
            foreach (var v in values) args.Add(v);
            return (await _db.ExecuteAsync(TDIGEST.REVRANK, args)).ToLongArray();
        }

        /// <inheritdoc/>
        public double[] ByRank(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return _db.Execute(TDIGEST.BYRANK, args).ToDoubleArray();
        }

        /// <inheritdoc/>
        public async Task<double[]> ByRankAsync(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return (await _db.ExecuteAsync(TDIGEST.BYRANK, args)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public double[] ByRevRank(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return _db.Execute(TDIGEST.BYREVRANK, args).ToDoubleArray();
        }

        /// <inheritdoc/>
        public async Task<double[]> ByRevRankAsync(RedisKey key, params long[] ranks)
        {
            if (ranks.Length < 1) throw new ArgumentOutOfRangeException(nameof(ranks));

            var args = new List<object>(ranks.Length + 1) { key };
            foreach (var v in ranks) args.Add(v);
            return (await _db.ExecuteAsync(TDIGEST.BYREVRANK, args)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public bool Reset(RedisKey key)
        {
            return _db.Execute(TDIGEST.RESET, key).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ResetAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TDIGEST.RESET, key)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public double TrimmedMean(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return _db.Execute(TDIGEST.TRIMMED_MEAN, key, lowCutQuantile, highCutQuantile).ToDouble();
        }

        /// <inheritdoc/>
        public async Task<double> TrimmedMeanAsync(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return (await _db.ExecuteAsync(TDIGEST.TRIMMED_MEAN, key, lowCutQuantile, highCutQuantile)).ToDouble();
        }
    }
}
