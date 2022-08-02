using NRedisStack.Core.Bloom.DataTypes;
using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
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
        /// Checks whether an item exist in the Bloom Filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="item">The item to check for.</param>
        /// <returns><see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means the item may exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.exists"/></remarks>
        public bool Exists(RedisKey key, RedisValue item)
        {
            return _db.Execute(BF.EXISTS, key, item).ToString() == "1";
        }

        /// <summary>
        /// Return information about a bloom filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Array with information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.info"/></remarks>
        public BloomInformation? Info(RedisKey key)
        {
            var info = _db.Execute(BF.INFO, key);
            return ResponseParser.ToBloomInfo(info);
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
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            List<object> args = new List<object> { key };

            if (capacity != null)
            {
                args.Add(BloomArgs.CAPACITY);
                args.Add(capacity);
            }

            if (error != null)
            {
                args.Add(BloomArgs.ERROR);
                args.Add(error);
            }

            if (expansion != null)
            {
                args.Add(BloomArgs.EXPANSION);
                args.Add(expansion);
            }

            if (nocreate)
            {
                args.Add(BloomArgs.NOCREATE);

            }

            if (nonscaling)
            {
                args.Add(BloomArgs.NONSCALING);
            }

            args.Add(BloomArgs.ITEMS);
            foreach (var item in items)
            {
                args.Add(item);
            }

            return ResponseParser.ToBooleanArray(_db.Execute(BF.INSERT, args));
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the key to restore.</param>
        /// <param name="iterator">Iterator value associated with data (returned by SCANDUMP).</param>
        /// <param name="data">Current data chunk (returned by SCANDUMP).</param>
        /// <returns>Array with information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.loadchunk"/></remarks>
        public bool LoadChunk(RedisKey key, int iterator, RedisValue data)
        {
            return ResponseParser.ParseOKtoBoolean(_db.Execute(BF.LOADCHUNK, key, iterator, data));
        }

        /// <summary>
        /// Adds one or more items to the Bloom Filter. A filter will be created if it does not exist yet.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <returns>An array of booleans. Each element is either true or false depending on whether the
        /// corresponding input element was newly added to the filter or may have previously existed.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.madd"/></remarks>
        public bool[] MAdd(RedisKey key, RedisValue[] items)
        {
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return ResponseParser.ToBooleanArray(_db.Execute(BF.MADD, args));
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
            if (items == null)
                throw new ArgumentNullException(nameof(items));

            List<object> args = new List<object> { key };

            foreach (var item in items)
            {
                args.Add(item);
            }

            return ResponseParser.ToBooleanArray(_db.Execute(BF.MEXISTS, args));

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
        /// <returns><see langword="true"/> if executed correctly, <see langword="false"/> otherwise.</returns>
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

            return ResponseParser.ParseOKtoBoolean(_db.Execute(BF.RESERVE, args));
        }

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Array of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bf.scandump"/></remarks>
        public RedisResult[] ScanDump(RedisKey key, int iterator)
        {
            return ResponseParser.ToArray(_db.Execute(BF.SCANDUMP, key, iterator));
        }
    }
}
