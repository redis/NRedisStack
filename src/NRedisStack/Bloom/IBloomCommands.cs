using NRedisStack.Bloom.DataTypes;
using StackExchange.Redis;

namespace NRedisStack;

public interface IBloomCommands
{
    /// <summary>
    /// Adds an item to a Bloom Filter.
    /// </summary>
    /// <param name="key">The key under which the filter is found.</param>
    /// <param name="item">The item to add.</param>
    /// <returns><see langword="true"/> if the item did not exist in the filter, <see langword="false"/> otherwise.</returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.add"/></remarks>
    bool Add(RedisKey key, RedisValue item);

    /// <summary>
    /// Returns the cardinality of a Bloom filter.
    /// </summary>
    /// <param name="key">The name of the filter.</param>
    /// <returns>number of items that were added to a Bloom filter and detected as unique.</returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.card"/></remarks>
    long Card(RedisKey key);

    /// <summary>
    /// Checks whether an item exist in the Bloom Filter or not.
    /// </summary>
    /// <param name="key">The name of the filter.</param>
    /// <param name="item">The item to check for.</param>
    /// <returns><see langword="true"/> means the item may exist in the filter,
    /// and <see langword="false"/> means it does not exist in the filter.</returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.exists"/></remarks>
    bool Exists(RedisKey key, RedisValue item);

    /// <summary>
    /// Return information about a bloom filter.
    /// </summary>
    /// <param name="key">Name of the key to return information about.</param>
    /// <returns>Information of the filter.</returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.info"/></remarks>
    BloomInformation Info(RedisKey key);

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
    bool[] Insert(RedisKey key, RedisValue[] items, int? capacity = null,
        double? error = null, int? expansion = null,
        bool nocreate = false, bool nonscaling = false);

    /// <summary>
    /// Restores a filter previously saved using SCANDUMP.
    /// </summary>
    /// <param name="key">Name of the key to restore.</param>
    /// <param name="iterator">Iterator value associated with data (returned by SCANDUMP).</param>
    /// <param name="data">Current data chunk (returned by SCANDUMP).</param>
    /// <returns><see langword="true"/> if executed correctly, error otherwise/></returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.loadchunk"/></remarks>
    bool LoadChunk(RedisKey key, long iterator, byte[] data);

    /// <summary>
    /// Adds one or more items to the Bloom Filter. A filter will be created if it does not exist yet.
    /// </summary>
    /// <param name="key">The name of the filter.</param>
    /// <param name="items">One or more items to add.</param>
    /// <returns>An array of booleans. Each element is either true or false depending on whether the
    /// corresponding input element was newly added to the filter or may have previously existed.</returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.madd"/></remarks>
    bool[] MAdd(RedisKey key, params RedisValue[] items);

    /// <summary>
    /// Checks whether one or more items may exist in the filter or not.
    /// </summary>
    /// <param name="key">The name of the filter.</param>
    /// <param name="items">One or more items to check.</param>
    /// <returns>An array of booleans, for each item <see langword="true"/> means the item may exist in the filter,
    /// and <see langword="false"/> means the item may exist in the filter.</returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.mexists"/></remarks>
    bool[] MExists(RedisKey key, RedisValue[] items);

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
    bool Reserve(RedisKey key, double errorRate, long capacity,
        int? expansion = null, bool nonscaling = false);

    /// <summary>
    /// Restores a filter previously saved using SCANDUMP.
    /// </summary>
    /// <param name="key">Name of the filter.</param>
    /// <param name="iterator">Iterator value; either 0 or the iterator from a previous invocation of this command.</param>
    /// <returns>Tuple of iterator and data.</returns>
    /// <remarks><seealso href="https://redis.io/commands/bf.scandump"/></remarks>
    Tuple<long, Byte[]> ScanDump(RedisKey key, long iterator);
}