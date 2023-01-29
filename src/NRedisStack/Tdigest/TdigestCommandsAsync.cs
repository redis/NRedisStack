using NRedisStack.Tdigest.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{

    public class TdigestCommandsAsync : ITdigestCommandsAsync
    {
        IDatabaseAsync _db;
        public TdigestCommandsAsync(IDatabaseAsync db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public async Task<bool> AddAsync(RedisKey key, params double[] values)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Add(key, values))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<double[]> CDFAsync(RedisKey key, params double[] values)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.CDF(key, values))).ToDoubleArray();
        }

        /// <inheritdoc/>
        public async Task<bool> CreateAsync(RedisKey key, long compression = 100)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Create(key, compression))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<TdigestInformation> InfoAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Info(key))).ToTdigestInfo();
        }

        /// <inheritdoc/>
        public async Task<double> MaxAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Max(key))).ToDouble();
        }

        /// <inheritdoc/>
        public async Task<double> MinAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Min(key))).ToDouble();
        }

        /// <inheritdoc/>
        public async Task<bool> MergeAsync(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Merge(destinationKey, compression, overide, sourceKeys))).OKtoBoolean();
        }

        ///added to this t-digest would be less than or equal to each of the specified cutoffs.
        /// <inheritdoc/>
        public async Task<double[]> QuantileAsync(RedisKey key, params double[] quantile)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Quantile(key, quantile))).ToDoubleArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> RankAsync(RedisKey key, params long[] values)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Rank(key, values))).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<long[]> RevRankAsync(RedisKey key, params long[] values)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.RevRank(key, values))).ToLongArray();
        }

        /// <inheritdoc/>
        public async Task<double[]> ByRankAsync(RedisKey key, params long[] ranks)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.ByRank(key, ranks))).ToDoubleArray();
        }

        /// <inheritdoc/>
        public async Task<double[]> ByRevRankAsync(RedisKey key, params long[] ranks)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.ByRevRank(key, ranks))).ToDoubleArray();
        }

        /// <inheritdoc/>
        public async Task<bool> ResetAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.Reset(key))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<double> TrimmedMeanAsync(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return (await _db.ExecuteAsync(TdigestCommandBuilder.TrimmedMean(key, lowCutQuantile, highCutQuantile))).ToDouble();
        }
    }
}