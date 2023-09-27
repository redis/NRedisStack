using NRedisStack.Tdigest.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{

    public class TdigestCommands : TdigestCommandsAsync, ITdigestCommands
    {
        IDatabase _db;
        public TdigestCommands(IDatabase db) : base(db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public bool Add(RedisKey key, params double[] values)
        {
            return _db.Execute(TdigestCommandBuilder.Add(key, values)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public double[] CDF(RedisKey key, params double[] values)
        {
            return _db.Execute(TdigestCommandBuilder.CDF(key, values)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public bool Create(RedisKey key, long compression = 100)
        {
            return _db.Execute(TdigestCommandBuilder.Create(key, compression)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public TdigestInformation Info(RedisKey key)
        {
            return _db.Execute(TdigestCommandBuilder.Info(key)).ToTdigestInfo();
        }

        /// <inheritdoc/>
        public double Max(RedisKey key)
        {
            return _db.Execute(TdigestCommandBuilder.Max(key)).ToDouble();
        }

        /// <inheritdoc/>
        public double Min(RedisKey key)
        {
            var cmd = TdigestCommandBuilder.Min(key);
            var res = _db.Execute(cmd);
            return res.ToDouble();
        }

        /// <inheritdoc/>
        public bool Merge(RedisKey destinationKey, long compression = default(long), bool overide = false, params RedisKey[] sourceKeys)
        {
            return _db.Execute(TdigestCommandBuilder.Merge(destinationKey, compression, overide, sourceKeys)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public double[] Quantile(RedisKey key, params double[] quantile)
        {
            return _db.Execute(TdigestCommandBuilder.Quantile(key, quantile)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public long[] Rank(RedisKey key, params long[] values)
        {
            return _db.Execute(TdigestCommandBuilder.Rank(key, values)).ToLongArray();
        }

        /// <inheritdoc/>
        public long[] RevRank(RedisKey key, params long[] values)
        {
            return _db.Execute(TdigestCommandBuilder.RevRank(key, values)).ToLongArray();
        }

        /// <inheritdoc/>
        public double[] ByRank(RedisKey key, params long[] ranks)
        {
            return _db.Execute(TdigestCommandBuilder.ByRank(key, ranks)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public double[] ByRevRank(RedisKey key, params long[] ranks)
        {
            return _db.Execute(TdigestCommandBuilder.ByRevRank(key, ranks)).ToDoubleArray();
        }

        /// <inheritdoc/>
        public bool Reset(RedisKey key)
        {
            return _db.Execute(TdigestCommandBuilder.Reset(key)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public double TrimmedMean(RedisKey key, double lowCutQuantile, double highCutQuantile)
        {
            return _db.Execute(TdigestCommandBuilder.TrimmedMean(key, lowCutQuantile, highCutQuantile)).ToDouble();
        }
    }
}