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
            return _db.Execute(BloomCommandBuilder.Add(key, item)).ToString() == "1";
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
            return (await _db.ExecuteAsync(BloomCommandBuilder.Add(key, item))).ToString() == "1";
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
            return _db.Execute(BloomCommandBuilder.Exists(key, item)).ToString() == "1";
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
            return (await _db.ExecuteAsync(BloomCommandBuilder.Exists(key, item))).ToString() == "1";
        }

        /// <summary>
        /// Return information about a bloom filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.info"/></remarks>
        public BloomInformation Info(RedisKey key)
        {
            return _db.Execute(BloomCommandBuilder.Info(key)).ToBloomInfo();
        }

        /// <summary>
        /// Return information about a bloom filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.info"/></remarks>
        public async Task<BloomInformation> InfoAsync(RedisKey key)
        {
            var info = (await _db.ExecuteAsync(BloomCommandBuilder.Info(key)));
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
            return _db.Execute(BloomCommandBuilder.Insert(key, items, capacity, error, expansion, nocreate, nonscaling)).ToBooleanArray();
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
            return (await _db.ExecuteAsync(BloomCommandBuilder.Insert(key, items, capacity, error, expansion, nocreate, nonscaling))).ToBooleanArray();
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
            return _db.Execute(BloomCommandBuilder.LoadChunk(key, iterator, data)).OKtoBoolean();
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
            return (await _db.ExecuteAsync(BloomCommandBuilder.LoadChunk(key, iterator, data))).OKtoBoolean();
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
            return _db.Execute(BloomCommandBuilder.MAdd(key, items)).ToBooleanArray();
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
            return (await _db.ExecuteAsync(BloomCommandBuilder.MAdd(key, items))).ToBooleanArray();
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
            return _db.Execute(BloomCommandBuilder.MExists(key, items)).ToBooleanArray();
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
            return (await _db.ExecuteAsync(BloomCommandBuilder.MExists(key, items))).ToBooleanArray();
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
            return _db.Execute(BloomCommandBuilder.Reserve(key, errorRate, capacity, expansion, nonscaling)).OKtoBoolean();
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
            return (await _db.ExecuteAsync(BloomCommandBuilder.Reserve(key, errorRate, capacity, expansion, nonscaling))).OKtoBoolean();
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Tuple of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.scandump"/></remarks>
        public Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return _db.Execute(BloomCommandBuilder.ScanDump(key, iterator)).ToScanDumpTuple();
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Tuple of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.scandump"/></remarks>
        public async Task<Tuple<long, Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            return (await _db.ExecuteAsync(BloomCommandBuilder.ScanDump(key, iterator))).ToScanDumpTuple();
        }
    }
}
