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
            return _db.Execute(CF.ADD, key, item).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> AddAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.ADD, key, item);
            return result.ToString() == "1";
        }

        /// <inheritdoc/>
        public bool AddNX(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.ADDNX, key, item).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> AddNXAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.ADDNX, key, item);
            return result.ToString() == "1";
        }

        /// <inheritdoc/>
        public long Count(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.COUNT, key, item).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> CountAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.COUNT, key, item);
            return result.ToLong();
        }

        /// <inheritdoc/>
        public bool Del(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.DEL, key, item).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> DelAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.DEL, key, item);
            return result.ToString() == "1";
        }

        /// <inheritdoc/>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.EXISTS, key, item).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.EXISTS, key, item);
            return result.ToString() == "1";
        }

        /// <inheritdoc/>
        public CuckooInformation Info(RedisKey key)
        {
            var info = _db.Execute(CF.INFO, key);
            return info.ToCuckooInfo();
        }

        /// <inheritdoc/>
        public async Task<CuckooInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(CF.INFO, key);
            return info.ToCuckooInfo();
        }

        /// <inheritdoc/>
        public bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            if (capacity != null)
            {
                args.Add(CuckooArgs.CAPACITY);
                args.Add(capacity);
            }

            if (nocreate)
            {
                args.Add(CuckooArgs.NOCREATE);
            }

            args.Add(CuckooArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(CF.INSERT, args).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertAsync(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            if (capacity != null)
            {
                args.Add(CuckooArgs.CAPACITY);
                args.Add(capacity);
            }

            if (nocreate)
            {
                args.Add(CuckooArgs.NOCREATE);
            }

            args.Add(CuckooArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }

            var result = await _db.ExecuteAsync(CF.INSERT, args);
            return result.ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool[] InsertNX(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            if (capacity != null)
            {
                args.Add(CuckooArgs.CAPACITY);
                args.Add(capacity);
            }

            if (nocreate)
            {
                args.Add(CuckooArgs.NOCREATE);
            }

            args.Add(CuckooArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(CF.INSERTNX, args).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertNXAsync(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            if (capacity != null)
            {
                args.Add(CuckooArgs.CAPACITY);
                args.Add(capacity);
            }

            if (nocreate)
            {
                args.Add(CuckooArgs.NOCREATE);
            }

            args.Add(CuckooArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }

            var result = await _db.ExecuteAsync(CF.INSERTNX, args);
            return result.ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
        {
            return _db.Execute(CF.LOADCHUNK, key, iterator, data).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
        {
            var result = await _db.ExecuteAsync(CF.LOADCHUNK, key, iterator, data);
            return result.OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool[] MExists(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(CF.MEXISTS, args).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> MExistsAsync(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            var result = await _db.ExecuteAsync(CF.MEXISTS, args);
            return result.ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool Reserve(RedisKey key, long capacity,
                                           long? bucketSize = null, int? maxIterations = null, int? expansion = null)
        {
            List<object> args = new List<object> { key, capacity };

            if (bucketSize != null)
            {
                args.Add(CuckooArgs.BUCKETSIZE);
                args.Add(bucketSize);
            }

            if (maxIterations != null)
            {
                args.Add(CuckooArgs.MAXITERATIONS);
                args.Add(maxIterations);
            }

            if (expansion != null)
            {
                args.Add(CuckooArgs.EXPANSION);
                args.Add(expansion);
            }

            return _db.Execute(CF.RESERVE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReserveAsync(RedisKey key, long capacity,
                                           long? bucketSize = null, int? maxIterations = null, int? expansion = null)
        {
            List<object> args = new List<object> { key, capacity };

            if (bucketSize != null)
            {
                args.Add(CuckooArgs.BUCKETSIZE);
                args.Add(bucketSize);
            }

            if (maxIterations != null)
            {
                args.Add(CuckooArgs.MAXITERATIONS);
                args.Add(maxIterations);
            }

            if (expansion != null)
            {
                args.Add(CuckooArgs.EXPANSION);
                args.Add(expansion);
            }

            var result = await _db.ExecuteAsync(CF.RESERVE, args);
            return result.OKtoBoolean();
        }

        /// <inheritdoc/>
        public Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return _db.Execute(CF.SCANDUMP, key, iterator).ToScanDumpTuple();
        }

        /// <inheritdoc/>
        public async Task<Tuple<long, Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            var result = await _db.ExecuteAsync(CF.SCANDUMP, key, iterator);
            return result.ToScanDumpTuple();
        }
    }
}