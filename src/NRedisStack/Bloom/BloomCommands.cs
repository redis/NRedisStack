using NRedisStack.Bloom.DataTypes;
using NRedisStack.Literals;
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
            return _db.Execute(BF.ADD, key, item).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> AddAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(BF.ADD, key, item);
            return result.ToString() == "1";
        }

        /// <inheritdoc/>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(BF.EXISTS, key, item).ToString() == "1";
        }

        /// <inheritdoc/>
        public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(BF.EXISTS, key, item);
            return result.ToString() == "1";
        }

        /// <inheritdoc/>
        public BloomInformation Info(RedisKey key)
        {
            return _db.Execute(BF.INFO, key).ToBloomInfo();
        }

        /// <inheritdoc/>
        public async Task<BloomInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(BF.INFO, key);
            return info.ToBloomInfo();
        }

        /// <inheritdoc/>
        public bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null,
                                          double? error = null, int? expansion = null,
                                          bool nocreate = false, bool nonscaling = false)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = BloomAux.BuildInsertArgs(key, items, capacity, error, expansion, nocreate, nonscaling);

            return _db.Execute(BF.INSERT, args).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> InsertAsync(RedisKey key, RedisValue[] items, int? capacity = null,
                                          double? error = null, int? expansion = null,
                                          bool nocreate = false, bool nonscaling = false)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = BloomAux.BuildInsertArgs(key, items, capacity, error, expansion, nocreate, nonscaling);

            var result = await _db.ExecuteAsync(BF.INSERT, args);
            return result.ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
        {
            return _db.Execute(BF.LOADCHUNK, key, iterator, data).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
        {
            var result = await _db.ExecuteAsync(BF.LOADCHUNK, key, iterator, data);
            return result.OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool[] MAdd(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(BF.MADD, args).ToBooleanArray();
        }

        /// <inheritdoc/>
        public async Task<bool[]> MAddAsync(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            var result = await _db.ExecuteAsync(BF.MADD, args);
            return result.ToBooleanArray();
        }

        /// <inheritdoc/>
        public bool[] MExists(RedisKey key, RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return _db.Execute(BF.MEXISTS, args).ToBooleanArray();

        }

        /// <inheritdoc/>
        public async Task<bool[]> MExistsAsync(RedisKey key, RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            var result = await _db.ExecuteAsync(BF.MEXISTS, args);
            return result.ToBooleanArray();

        }

        /// <inheritdoc/>
        public bool Reserve(RedisKey key, double errorRate, long capacity,
                                           int? expansion = null, bool nonscaling = false)
        {
            List<object> args = new List<object> { key, errorRate, capacity };

            if (expansion != null)
            {
                args.Add(expansion);
            }

            if (nonscaling)
            {
                args.Add(BloomArgs.NONSCALING);
            }

            return _db.Execute(BF.RESERVE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ReserveAsync(RedisKey key, double errorRate, long capacity,
                                           int? expansion = null, bool nonscaling = false)
        {
            List<object> args = new List<object> { key, errorRate, capacity };

            if (expansion != null)
            {
                args.Add(expansion);
            }

            if (nonscaling)
            {
                args.Add(BloomArgs.NONSCALING);
            }

            var result = await _db.ExecuteAsync(BF.RESERVE, args);
            return result.OKtoBoolean();
        }

        /// <inheritdoc/>
        public Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return _db.Execute(BF.SCANDUMP, key, iterator).ToScanDumpTuple();
        }

        /// <inheritdoc/>
        public async Task<Tuple<long, Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            var result = await _db.ExecuteAsync(BF.SCANDUMP, key, iterator);
            return result.ToScanDumpTuple();
        }
    }
}
