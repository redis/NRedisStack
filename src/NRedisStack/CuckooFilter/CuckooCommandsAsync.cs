using NRedisStack.CuckooFilter.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{

    public class CuckooCommandsAsync : ICuckooCommandsAsync
    {
        IDatabaseAsync _db;
        public CuckooCommandsAsync(IDatabaseAsync db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public async Task<bool> AddAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Add(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> AddNXAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.AddNX(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<long> CountAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Count(key, item))).ToLong();
        }

        /// <inheritdoc/>
        public async Task<bool> DelAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Del(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Exists(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<CuckooInformation> InfoAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Info(key))).ToCuckooInfo();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertAsync(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Insert(key, items, capacity, nocreate))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertNXAsync(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.InsertNX(key, items, capacity, nocreate))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.LoadChunk(key, iterator, data))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool[]> MExistsAsync(RedisKey key, params RedisValue[] items)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.MExists(key, items))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool> ReserveAsync(RedisKey key, long capacity,
                                           long? bucketSize = null, int? maxIterations = null, int? expansion = null)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Reserve(key, capacity, bucketSize, maxIterations, expansion))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<Tuple<long, Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.ScanDump(key, iterator))).ToScanDumpTuple();
        }
    }
}