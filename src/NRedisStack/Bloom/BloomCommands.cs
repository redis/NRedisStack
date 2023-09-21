using NRedisStack.Bloom.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{

    public class BloomCommands : BloomCommandsAsync, IBloomCommands
    {
        IDatabase _db;
        public BloomCommands(IDatabase db) : base(db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public bool Add    (RedisKey key, RedisValue item)
        {
            return _db.Execute(BloomCommandBuilder.Add(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public long Card(RedisKey key)
        {
            return _db.Execute(BloomCommandBuilder.Card(key)).ToLong();
        }

        /// <inheritdoc/>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(BloomCommandBuilder.Exists(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public BloomInformation Info(RedisKey key)
        {
            return _db.Execute(BloomCommandBuilder.Info(key)).ToBloomInfo();
        }

        /// <inheritdoc/>
        public bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null,
                                          double? error = null, int? expansion = null,
                                          bool nocreate = false, bool nonscaling = false)
        {
            return _db.Execute(BloomCommandBuilder.Insert(key, items, capacity, error, expansion, nocreate, nonscaling)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
        {
            return _db.Execute(BloomCommandBuilder.LoadChunk(key, iterator, data)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool[] MAdd(RedisKey key, params RedisValue[] items)
        {
            return _db.Execute(BloomCommandBuilder.MAdd(key, items)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool[] MExists(RedisKey key, RedisValue[] items)
        {
            return _db.Execute(BloomCommandBuilder.MExists(key, items)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool Reserve(RedisKey key, double errorRate, long capacity,
                                           int? expansion = null, bool nonscaling = false)
        {
            return _db.Execute(BloomCommandBuilder.Reserve(key, errorRate, capacity, expansion, nonscaling)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return _db.Execute(BloomCommandBuilder.ScanDump(key, iterator)).ToScanDumpTuple();
        }
    }
}
