using NRedisStack.CuckooFilter.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack
{

    public class CuckooCommands : ICuckooCommands
    {
        IDatabase _db;
        public CuckooCommands(IDatabase db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public bool Add(RedisKey key, RedisValue item)
        {
            return _db.Execute(CuckooCommandBuilder.Add(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> AddAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Add(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public bool AddNX(RedisKey key, RedisValue item)
        {
            return _db.Execute(CuckooCommandBuilder.AddNX(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> AddNXAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.AddNX(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public long Count(RedisKey key, RedisValue item)
        {
            return _db.Execute(CuckooCommandBuilder.Count(key, item)).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> CountAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Count(key, item))).ToLong();
        }

        /// <inheritdoc/>
        public bool Del(RedisKey key, RedisValue item)
        {
            return _db.Execute(CuckooCommandBuilder.Del(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> DelAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Del(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(CuckooCommandBuilder.Exists(key, item)).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Exists(key, item))).ToString() == "1";
        }

        /// <inheritdoc/>
        public CuckooInformation Info(RedisKey key)
        {
            return _db.Execute(CuckooCommandBuilder.Info(key)).ToCuckooInfo();
        }

        /// <inheritdoc/>
        public async Task<CuckooInformation> InfoAsync(RedisKey key)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Info(key))).ToCuckooInfo();
        }

        /// <inheritdoc/>
        public bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            return _db.Execute(CuckooCommandBuilder.Insert(key, items, capacity, nocreate)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertAsync(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Insert(key, items, capacity, nocreate))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool[] InsertNX(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            return _db.Execute(CuckooCommandBuilder.InsertNX(key, items, capacity, nocreate)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertNXAsync(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.InsertNX(key, items, capacity, nocreate))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
        {
            return _db.Execute(CuckooCommandBuilder.LoadChunk(key, iterator, data)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.LoadChunk(key, iterator, data))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool[] MExists(RedisKey key, params RedisValue[] items)
        {
            return _db.Execute(CuckooCommandBuilder.MExists(key, items)).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> MExistsAsync(RedisKey key, params RedisValue[] items)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.MExists(key, items))).ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool Reserve(RedisKey key, long capacity,
                                           long? bucketSize = null, int? maxIterations = null, int? expansion = null)
        {
            return _db.Execute(CuckooCommandBuilder.Reserve(key, capacity, bucketSize, maxIterations, expansion)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReserveAsync(RedisKey key, long capacity,
                                           long? bucketSize = null, int? maxIterations = null, int? expansion = null)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.Reserve(key, capacity, bucketSize, maxIterations, expansion))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return _db.Execute(CuckooCommandBuilder.ScanDump(key, iterator)).ToScanDumpTuple();
        }

        /// <inheritdoc/>
        public async Task<Tuple<long, Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            return (await _db.ExecuteAsync(CuckooCommandBuilder.ScanDump(key, iterator))).ToScanDumpTuple();
        }
    }
}