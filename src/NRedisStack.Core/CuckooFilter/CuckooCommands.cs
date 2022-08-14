using NRedisStack.Core.CuckooFilter.DataTypes;
using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
{

    public class CuckooCommands
    {
        IDatabase _db;
        public CuckooCommands(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Adds an item to a Cuckoo Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.add"/></remarks>
        public bool Add(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.ADD, key, item).ToString() == "1";
        }

        /// <summary>
        /// Adds an item to a Cuckoo Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.add"/></remarks>
        public async Task<bool> AddAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.ADD, key, item);
            return result.ToString() == "1";
        }

        /// <summary>
        /// Adds an item to a Cuckoo Filter if the item did not exist previously.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.addnx"/></remarks>
        public bool AddNX(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.ADDNX, key, item).ToString() == "1";
        }

        /// <summary>
        /// Adds an item to a Cuckoo Filter if the item did not exist previously.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.addnx"/></remarks>
        public async Task<bool> AddNXAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.ADDNX, key, item);
            return result.ToString() == "1";
        }

        /// <summary>
        /// Returns the number of times an item may be in the filter.
        /// </summary>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to count.</param>
        /// <returns>the count of possible matching copies of the item in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.count"/></remarks>
        public long Count(RedisKey key, RedisValue item)
        {
            return ResponseParser.ToLong(_db.Execute(CF.COUNT, key, item));
        }

        /// <summary>
        /// Returns the number of times an item may be in the filter.
        /// </summary>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to count.</param>
        /// <returns>the count of possible matching copies of the item in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.count"/></remarks>
        public async Task<long> CountAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.COUNT, key, item);
            return ResponseParser.ToLong(result);
        }

        /// <summary>
        /// Deletes an item from the Cuckoo Filter.
        /// </summary>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to delete from the filter.</param>
        /// <returns>see langword="true"/> if the item has been deleted from the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.del"/></remarks>
        public bool Del(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.DEL, key, item).ToString() == "1";
        }

        /// <summary>
        /// Deletes an item from the Cuckoo Filter.
        /// </summary>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to delete from the filter.</param>
        /// <returns>see langword="true"/> if the item has been deleted from the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.del"/></remarks>
        public async Task<bool> DelAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.DEL, key, item);
            return result.ToString() == "1";
        }

        /// <summary>
        /// Checks whether an item exist in the Cuckoo Filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="item">The item to check for.</param>
        /// <returns><see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means it does not exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.exists"/></remarks>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(CF.EXISTS, key, item).ToString() == "1";
        }

        /// <summary>
        /// Checks whether an item exist in the Cuckoo Filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="item">The item to check for.</param>
        /// <returns><see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means it does not exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.exists"/></remarks>
        public async Task<bool> ExistsAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(CF.EXISTS, key, item);
            return result.ToString() == "1";
        }

        /// <summary>
        /// Return information about a Cuckoo filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.info"/></remarks>
        public CuckooInformation Info(RedisKey key)
        {
            var info = _db.Execute(CF.INFO, key);
            return ResponseParser.ToCuckooInfo(info);
        }

        /// <summary>
        /// Return information about a Cuckoo filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.info"/></remarks>
        public async Task<CuckooInformation> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(CF.INFO, key);
            return ResponseParser.ToCuckooInfo(info);
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter. A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the
        /// <returns>An array of booleans.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.insert"/></remarks>
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

            return ResponseParser.ToBooleanArray(_db.Execute(CF.INSERT, args));
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter. A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the
        /// <returns>An array of booleans.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.insert"/></remarks>
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
            return ResponseParser.ToBooleanArray(result);
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter if the items did not exist previously.
        /// A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the
        /// <returns>An array of booleans.where <see langword="true"/> means the item has been added to the filter,
        /// and <see langword="false"/> mean, the item already existed</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.insertnx"/></remarks>
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

            return ResponseParser.ToBooleanArray(_db.Execute(CF.INSERTNX, args));
        }

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter if the items did not exist previously.
        /// A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the
        /// <returns>An array of booleans.where <see langword="true"/> means the item has been added to the filter,
        /// and <see langword="false"/> mean, the item already existed</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.insertnx"/></remarks>
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
            return ResponseParser.ToBooleanArray(result);
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the key to restore.</param>
        /// <param name="iterator">Iterator value associated with data (returned by SCANDUMP).</param>
        /// <param name="data">Current data chunk (returned by SCANDUMP).</param>
        /// <returns>Array with information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.loadchunk"/></remarks>
        public bool LoadChunk(RedisKey key, long iterator, Byte[] data)
        {
            return ResponseParser.OKtoBoolean(_db.Execute(CF.LOADCHUNK, key, iterator, data));
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the key to restore.</param>
        /// <param name="iterator">Iterator value associated with data (returned by SCANDUMP).</param>
        /// <param name="data">Current data chunk (returned by SCANDUMP).</param>
        /// <returns>Array with information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.loadchunk"/></remarks>
        public async Task<bool> LoadChunkAsync(RedisKey key, long iterator, Byte[] data)
        {
            var result = await _db.ExecuteAsync(CF.LOADCHUNK, key, iterator, data);
            return ResponseParser.OKtoBoolean(result);
        }

        /// <summary>
        /// Checks whether one or more items may exist in the a Cuckoo Filter.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to check.</param>
        /// <returns>An array of booleans, for each item <see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means the item may exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.mexists"/></remarks>
        public bool[] MExists(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentOutOfRangeException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return ResponseParser.ToBooleanArray(_db.Execute(CF.MEXISTS, args));
        }

        /// <summary>
        /// Checks whether one or more items may exist in the a Cuckoo Filter.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to check.</param>
        /// <returns>An array of booleans, for each item <see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means the item may exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.mexists"/></remarks>
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
            return ResponseParser.ToBooleanArray(result);
        }

        /// <summary>
        /// Creates a new Cuckoo Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="capacity">The number of entries intended to be added to the filter.</param>
        /// <param name="bucketSize">Number of items in each bucket.</param>
        /// <param name="maxIterations">Number of attempts to swap items between buckets before
        /// declaring filter as full and creating an additional filter.</param>
        /// <param name="expansion">(Optional) When capacity is reached, an additional sub-filter is
        /// created in size of the last sub-filter multiplied by expansion.</param>
        /// <returns><see langword="true"/> if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.reserve"/></remarks>
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

            return ResponseParser.OKtoBoolean(_db.Execute(CF.RESERVE, args));
        }

        /// <summary>
        /// Creates a new Cuckoo Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="capacity">The number of entries intended to be added to the filter.</param>
        /// <param name="bucketSize">Number of items in each bucket.</param>
        /// <param name="maxIterations">Number of attempts to swap items between buckets before
        /// declaring filter as full and creating an additional filter.</param>
        /// <param name="expansion">(Optional) When capacity is reached, an additional sub-filter is
        /// created in size of the last sub-filter multiplied by expansion.</param>
        /// <returns><see langword="true"/> if executed correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.reserve"/></remarks>
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
            return ResponseParser.OKtoBoolean(result);
        }

        /// <summary>
        /// Begins an incremental save of the Cuckoo Filter.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Tuple of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.scandump"/></remarks>
        public Tuple<long,Byte[]> ScanDump(RedisKey key, long iterator)
        {
            return ResponseParser.ToScanDumpTuple(_db.Execute(CF.SCANDUMP, key, iterator));
        }

        /// <summary>
        /// Begins an incremental save of the Cuckoo Filter.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Tuple of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.scandump"/></remarks>
        public async Task<Tuple<long,Byte[]>> ScanDumpAsync(RedisKey key, long iterator)
        {
            var result = await _db.ExecuteAsync(CF.SCANDUMP, key, iterator);
            return ResponseParser.ToScanDumpTuple(result);
        }
    }
}