using NRedisStack.TopK.DataTypes;
using StackExchange.Redis;

namespace NRedisStack
{
    public interface ITopKCommands
    {
        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="items">Items to be added</param>
        /// <returns>Array of simple-string-reply - if an element was dropped from the TopK list, null otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.add"/></remarks>
        RedisResult[]? Add(RedisKey key, params RedisValue[] items);

        /// <summary>
        /// Returns count for an items.
        /// </summary>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <param name="items">Items to be counted.</param>
        /// <returns>count for responding item.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.count"/></remarks>
        [Obsolete("TOPK.COUNT is deprecated as of Bloom 2.4.0")]
        long[] Count(RedisKey key, params RedisValue[] items);

        /// <summary>
        /// Increase the score of an item in the data structure by increment.
        /// </summary>
        /// <param name="key">Name of sketch where item is added.</param>
        /// <param name="itemIncrements">Tuple of The items which counter is to be increased
        /// and the Amount by which the item score is to be increased.</param>
        /// <returns>Score of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.incrby"/></remarks>
        RedisResult[] IncrBy(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements);

        /// <summary>
        /// Return TopK information.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>TopK Information.</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.info"/></remarks>
        TopKInformation Info(RedisKey key);

        /// <summary>
        /// Return full list of items in Top K list.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="withcount">return Count of each element is returned.</param>
        /// <returns>Full list of items in Top K list</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.list"/></remarks>
        RedisResult[] List(RedisKey key, bool withcount = false);

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="item">Item to be queried.</param>
        /// <returns><see langword="true"/> if item is in Top-K, <see langword="false"/> otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.query"/></remarks>
        bool Query(RedisKey key, RedisValue item);

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="items">Items to be queried.</param>
        /// <returns>Bolean Array where <see langword="true"/> if item is in Top-K, <see langword="false"/> otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.query"/></remarks>
        bool[] Query(RedisKey key, params RedisValue[] items);

        /// <summary>
        /// Initializes a TopK with specified parameters.
        /// </summary>
        /// <param name="key">Key under which the sketch is to be found.</param>
        /// <param name="topk">Number of top occurring items to keep.</param>
        /// <param name="width">Number of counters kept in each array. (Default 8)</param>
        /// <param name="depth">Number of arrays. (Default 7)</param>
        /// <param name="decay">The probability of reducing a counter in an occupied bucket. (Default 0.9)</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.reserve"/></remarks>
        bool Reserve(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9);
    }
}