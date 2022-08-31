using NRedisStack.Bloom.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
namespace NRedisStack
{

    public class BloomCommands
    {
        IDatabase _db;
        public BloomCommands(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Adds an item to a Bloom Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.add"/></remarks>
        public bool Add(RedisKey key, RedisValue item)
        {
            return _db.Execute(BF.ADD, key, item).ToString() == "1";
        }

        /// <summary>
        /// Adds an item to a Bloom Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.add"/></remarks>
        public async Task<bool> AddAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(BF.ADD, key, item);
            return result.ToString() == "1";
        }

        /// <summary>
        /// Checks whether an item exist in the Bloom Filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="item">The item to check for.</param>
        /// <returns><see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means it does not exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.exists"/></remarks>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(BF.EXISTS, key, item).ToString() == "1";
        }

        /// <summary>
        /// Checks whether an item exist in the Bloom Filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="item">The item to check for.</param>
        /// <returns><see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means it does not exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.exists"/></remarks>
        public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(BF.EXISTS, key, item);
            return result.ToString() == "1";
        }

        /// <summary>
        /// Return information about a bloom filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.info"/></remarks>
        public BloomInformation Info(RedisKey key)
        {
            return _db.Execute(BF.INFO, key).ToBloomInfo();
        }

        /// <summary>
        /// Return information about a bloom filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.info"/></remarks>
        public async Task<BloomInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(BF.INFO, key);
            return info.ToBloomInfo();
        }

        /// <summary>
        /// Adds one or more items to a Bloom Filter. A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="error">(Optional) Specifies the error ratio of the newly created filter if it does not yet exist.</param>
        /// <param name="expansion">(Optional) When capacity is reached, an additional sub-filter is
        /// created in size of the last sub-filter multiplied by expansion.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the
        /// filter should not be created if it does not already exist.</param>
        /// <param name="nonscaling">(Optional) <see langword="true"/> toprevent the filter
        /// from creating additional sub-filters if initial capacity is reached.</param>
        /// <returns>An array of booleans. Each element is either true or false depending on whether the
        /// corresponding input element was newly added to the filter or may have previously existed.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.insert"/></remarks>
        public bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null,
                                  double? error = null, int? expansion = null,
                                  bool nocreate = false, bool nonscaling = false)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            var args = BloomAux.BuildInsertArgs(key, items, capacity, error, expansion, nocreate, nonscaling);
            
            return _db.Execute(BF.INSERT, args).ToBooleanArray();
        }

        /// <summary>
        /// Adds one or more items to a Bloom Filter. A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="error">(Optional) Specifies the error ratio of the newly created filter if it does not yet exist.</param>
        /// <param name="expansion">(Optional) When capacity is reached, an additional sub-filter is
        /// created in size of the last sub-filter multiplied by expansion.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the
        /// filter should not be created if it does not already exist.</param>
        /// <param name="nonscaling">(Optional) <see langword="true"/> toprevent the filter
        /// from creating additional sub-filters if initial capacity is reached.</param>
        /// <returns>An array of booleans. Each element is either true or false depending on whether the
        /// corresponding input element was newly added to the filter or may have previously existed.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.insert"/></remarks>
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

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the key to restore.</param>
        /// <param name="iterator">Iterator value associated with data (returned by SCANDUMP).</param>
        /// <param name="data">Current data chunk (returned by SCANDUMP).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.loadchunk"/></remarks>
        public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
        {
            return _db.Execute(BF.LOADCHUNK, key, iterator, data).OKtoBoolean();
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the key to restore.</param>
        /// <param name="iterator">Iterator value associated with data (returned by SCANDUMP).</param>
        /// <param name="data">Current data chunk (returned by SCANDUMP).</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.loadchunk"/></remarks>
        public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
        {
            var result = await _db.ExecuteAsync(BF.LOADCHUNK, key, iterator, data);
            return result.OKtoBoolean();
        }

        /// <summary>
        /// Adds one or more items to the Bloom Filter. A filter will be created if it does not exist yet.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <returns>An array of booleans. Each element is either true or false depending on whether the
        /// corresponding input element was newly added to the filter or may have previously existed.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.madd"/></remarks>
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

        /// <summary>
        /// Adds one or more items to the Bloom Filter. A filter will be created if it does not exist yet.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <returns>An array of booleans. Each element is either true or false depending on whether the
        /// corresponding input element was newly added to the filter or may have previously existed.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.madd"/></remarks>
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

        /// <summary>
        /// Checks whether one or more items may exist in the filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to check.</param>
        /// <returns>An array of booleans, for each item <see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means the item may exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.mexists"/></remarks>
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

        /// <summary>
        /// Checks whether one or more items may exist in the filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to check.</param>
        /// <returns>An array of booleans, for each item <see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means the item may exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.mexists"/></remarks>
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

        /// <summary>
        /// Creates a new Bloom Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="errorRate">The desired probability for false positives (value between 0 to 1).</param>
        /// <param name="capacity">The number of entries intended to be added to the filter.</param>
        /// <param name="expansion">(Optional) When capacity is reached, an additional sub-filter is
        /// created in size of the last sub-filter multiplied by expansion.</param>
        /// <param name="nonscaling">(Optional) <see langword="true"/> toprevent the filter
        /// from creating additional sub-filters if initial capacity is reached.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.reserve"/></remarks>
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

        /// <summary>
        /// Creates a new Bloom Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="errorRate">The desired probability for false positives (value between 0 to 1).</param>
        /// <param name="capacity">The number of entries intended to be added to the filter.</param>
        /// <param name="expansion">(Optional) When capacity is reached, an additional sub-filter is
        /// created in size of the last sub-filter multiplied by expansion.</param>
        /// <param name="nonscaling">(Optional) <see langword="true"/> toprevent the filter
        /// from creating additional sub-filters if initial capacity is reached.</param>
        /// <returns><see langword="true"/> if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.reserve"/></remarks>
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

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Tuple of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.scandump"/></remarks>
        public Tuple<long,Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return _db.Execute(BF.SCANDUMP, key, iterator).ToScanDumpTuple();
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Tuple of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.scandump"/></remarks>
        public async Task<Tuple<long,Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            var result = await _db.ExecuteAsync(BF.SCANDUMP, key, iterator);
            return result.ToScanDumpTuple();
        }
    }
}
