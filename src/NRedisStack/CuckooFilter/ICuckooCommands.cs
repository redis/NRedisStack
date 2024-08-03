using NRedisStack.CuckooFilter.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{
    public interface ICuckooCommands
    {
        /// <summary>
        /// Adds an item to a Cuckoo Filter.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.add"/></remarks>
        bool Add(RedisKey key, RedisValue item);

        /// <summary>
        /// Adds an item to a Cuckoo Filter if the item did not exist previously.
        /// </summary>
        /// <param name="key">The key under which the filter is found.</param>
        /// <param name="item">The item to add.</param>
        /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.addnx"/></remarks>
        bool AddNX(RedisKey key, RedisValue item);

        /// <summary>
        /// Returns the number of times an item may be in the filter.
        /// </summary>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to count.</param>
        /// <returns>the count of possible matching copies of the item in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.count"/></remarks>
        long Count(RedisKey key, RedisValue item);

        /// <summary>
        /// Deletes an item from the Cuckoo Filter.
        /// </summary>
        /// <param name="key">The name of the filter</param>
        /// <param name="item">The item to delete from the filter.</param>
        /// <returns>see langword="true"/> if the item has been deleted from the filter, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.del"/></remarks>
        bool Del(RedisKey key, RedisValue item);

        /// <summary>
        /// Checks whether an item exist in the Cuckoo Filter or not.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="item">The item to check for.</param>
        /// <returns><see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means it does not exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.exists"/></remarks>
        bool Exists(RedisKey key, RedisValue item);

        /// <summary>
        /// Return information about a Cuckoo filter.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>Information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.info"/></remarks>
        CuckooInformation Info(RedisKey key);

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter. A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the filter should not be created if it does not already exist.</param>
        /// <returns>An array of booleans.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.insert"/></remarks>
        bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false);

        /// <summary>
        /// Adds one or more items to a Cuckoo Filter if the items did not exist previously.
        /// A filter will be created if it does not exist.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to add.</param>
        /// <param name="capacity">(Optional) Specifies the desired capacity for the filter to be created.</param>
        /// <param name="nocreate">(Optional) <see langword="true"/> to indicates that the filter should not be created if it does not already exist.</param>
        /// <returns>An array of booleans.where <see langword="true"/> means the item has been added to the filter,
        /// and <see langword="false"/> mean, the item already existed</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.insertnx"/></remarks>
        bool[] InsertNX(RedisKey key, RedisValue[] items, int? capacity = null, bool nocreate = false);

        /// <summary>
        /// Restores a filter previosly saved using SCANDUMP.
        /// </summary>
        /// <param name="key">Name of the key to restore.</param>
        /// <param name="iterator">Iterator value associated with data (returned by SCANDUMP).</param>
        /// <param name="data">Current data chunk (returned by SCANDUMP).</param>
        /// <returns>Array with information of the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.loadchunk"/></remarks>
        bool LoadChunk(RedisKey key, long iterator, Byte[] data);

        /// <summary>
        /// Checks whether one or more items may exist in the a Cuckoo Filter.
        /// </summary>
        /// <param name="key">The name of the filter.</param>
        /// <param name="items">One or more items to check.</param>
        /// <returns>An array of booleans, for each item <see langword="true"/> means the item may exist in the filter,
        /// and <see langword="false"/> means the item may exist in the filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.mexists"/></remarks>
        bool[] MExists(RedisKey key, params RedisValue[] items);

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
        bool Reserve(RedisKey key, long capacity,
                                   long? bucketSize = null, int? maxIterations = null, int? expansion = null);

        /// <summary>
        /// Begins an incremental save of the Cuckoo Filter.
        /// </summary>
        /// <param name="key">Name of the filter.</param>
        /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
        /// <returns>Tuple of iterator and data.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.scandump"/></remarks>
        Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator);
    }
}