using NRedisStack.Bloom.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{

    public class BloomCommands : IBloomCommands
    {
        IDatabase _db;
        public BloomCommands(IDatabase db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public bool Add(RedisKey key, RedisValue item)
        {
            return _db.Execute(BloomCommandBuilder.Add(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> AddAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.Add(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(BloomCommandBuilder.Exists(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.Exists(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public BloomInformation Info(RedisKey key)
        {
            return _db.Execute(BloomCommandBuilder.Info(key)).ToBloomInfo();
        }

        /// <inheritdoc/>
        public async Task<BloomInformation> InfoAsync(RedisKey key)
        {
            var info = (await _db.ExecuteAsync(BloomCommandBuilder.Info(key)));
            return info.ToBloomInfo();
        }

        /// <inheritdoc/>
        public bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null,
                                          double? error = null, int? expansion = null,
                                          bool nocreate = false, bool nonscaling = false)
        {
            return _db.Execute(BloomCommandBuilder.Insert(key, items, capacity, error, expansion, nocreate, nonscaling)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertAsync(RedisKey key, RedisValue[] items, int? capacity = null,
                                          double? error = null, int? expansion = null,
                                          bool nocreate = false, bool nonscaling = false)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.Insert(key, items, capacity, error, expansion, nocreate, nonscaling))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
        {
            return _db.Execute(BloomCommandBuilder.LoadChunk(key, iterator, data)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.LoadChunk(key, iterator, data))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool[] MAdd(RedisKey key, params RedisValue[] items)
        {
            return _db.Execute(BloomCommandBuilder.MAdd(key, items)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> MAddAsync(RedisKey key, params RedisValue[] items)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.MAdd(key, items))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool[] MExists(RedisKey key, RedisValue[] items)
        {
            return _db.Execute(BloomCommandBuilder.MExists(key, items)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> MExistsAsync(RedisKey key, RedisValue[] items)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.MExists(key, items))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool Reserve(RedisKey key, double errorRate, long capacity,
                                           int? expansion = null, bool nonscaling = false)
        {
            return _db.Execute(BloomCommandBuilder.Reserve(key, errorRate, capacity, expansion, nonscaling)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReserveAsync(RedisKey key, double errorRate, long capacity,
                                           int? expansion = null, bool nonscaling = false)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.Reserve(key, errorRate, capacity, expansion, nonscaling))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return _db.Execute(BloomCommandBuilder.ScanDump(key, iterator)).ToScanDumpTuple();
        }

        /// <inheritdoc/>
        public async Task<Tuple<long, Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.ScanDump(key, iterator))).ToScanDumpTuple();
        }
    }
}
