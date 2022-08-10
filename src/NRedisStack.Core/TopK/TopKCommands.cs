using NRedisStack.Core.TopK.DataTypes;
using NRedisStack.Core.Literals;
using StackExchange.Redis;
namespace NRedisStack.Core
{

    public class TopKCommands //TODO: Finish this
    {
        IDatabase _db;
        public TopKCommands(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="item">Item to be added.</param>
        /// <returns>Array of simple-string-reply - if an element was dropped from the TopK list, null otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.add"/></remarks>
        public RedisResult[]? Add(RedisKey key, RedisValue item)
        {
            return ResponseParser.ToArray(_db.Execute(TOPK.ADD, key, item));
        }

        /// <inheritdoc cref="Add(RedisKey, RedisValue)"/>
        public async Task<RedisResult[]?> AddAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(TOPK.ADD, key, item);
            return ResponseParser.ToArray(result);
        }

        /// <summary>
        /// Increases the count of item by increment.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="items">Items to be added</param>
        /// <returns>Array of simple-string-reply - if an element was dropped from the TopK list, null otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.add"/></remarks>
        public RedisResult[]? Add(RedisKey key, params RedisValue[] items)
        {
            var args = Auxiliary.MergeArgs(key, items);

            return (RedisResult[]?)_db.Execute(TOPK.ADD, args);
        }

        /// <inheritdoc cref="Add(RedisKey, RedisValue[])"/>
        public async Task<RedisResult[]?> AddAsync(RedisKey key, params RedisValue[] items)
        {
            var args = Auxiliary.MergeArgs(key, items);

            var result = await _db.ExecuteAsync(TOPK.ADD, args);
            return (RedisResult[]?)result;
        }

        /// <summary>
        /// Returns count for an item.
        /// </summary>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <param name="item">Item to be counted.</param>
        /// <returns>count for responding item.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.count"/></remarks>
        public long Count(RedisKey key, RedisValue item)
        {
            return ResponseParser.ToLong(_db.Execute(TOPK.COUNT, key, item));
        }

        /// <inheritdoc cref="Count(RedisKey, RedisValue)"/>
        public async Task<long> CountAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(TOPK.COUNT, key, item);
            return ResponseParser.ToLong(result);
        }

        /// <summary>
        /// Returns count for an items.
        /// </summary>
        /// <param name="key">Name of sketch where item is counted</param>
        /// <param name="item">Items to be counted.</param>
        /// <returns>count for responding item.</returns>
        /// <remarks><seealso href="https://redis.io/commands/cf.count"/></remarks>
        public long[]? Count(RedisKey key, params RedisValue[] items)
        {
            var args = Auxiliary.MergeArgs(key, items);
            return ResponseParser.ToLongArray(_db.Execute(TOPK.COUNT, args));
        }

        /// <inheritdoc cref="Count(RedisKey, RedisValue[])"/>
        public async Task<long[]?> CountAsync(RedisKey key, params RedisValue[] items)
        {
            var args = Auxiliary.MergeArgs(key, items);
            var result = await _db.ExecuteAsync(TOPK.COUNT, args);
            return ResponseParser.ToLongArray(result);
        }


        /// <summary>
        /// Increase the score of an item in the data structure by increment.
        /// </summary>
        /// <param name="key">Name of sketch where item is added.</param>
        /// <param name="itemIncrements">Tuple of The items which counter is to be increased
        /// and the Amount by which the item score is to be increased.</param>
        /// <returns>Score of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.incrby"/></remarks>
        public RedisResult[]? IncrBy(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements)
        {
            if (itemIncrements.Length < 1)
                throw new ArgumentException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
            foreach (var pair in itemIncrements)
            {
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }
            return ResponseParser.ToArray(_db.Execute(TOPK.INCRBY, args));
        }

        /// <summary>
        /// Increase the score of an item in the data structure by increment.
        /// </summary>
        /// <param name="key">Name of sketch where item is added.</param>
        /// <param name="itemIncrements">Tuple of The items which counter is to be increased
        /// and the Amount by which the item score is to be increased.</param>
        /// <returns>Score of each item after increment.</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.incrby"/></remarks>
        public async Task<RedisResult[]?> IncrByAsync(RedisKey key, params Tuple<RedisValue, long>[] itemIncrements)
        {
            if (itemIncrements.Length < 1)
                throw new ArgumentException(nameof(itemIncrements));

            List<object> args = new List<object> { key };
            foreach (var pair in itemIncrements)
            {
                args.Add(pair.Item1);
                args.Add(pair.Item2);
            }

            var result = await _db.ExecuteAsync(TOPK.INCRBY, args);
            return ResponseParser.ToArray(result);
        }

        // //TODO: information about what?
        /// <summary>
        /// Return TopK information.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>TopK Information.</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.info"/></remarks>
        public TopKInformation? Info(RedisKey key)
        {
            var info = _db.Execute(TOPK.INFO, key);
            return ResponseParser.ToTopKInfo(info);
        }

        // //TODO: information about what?
        /// <summary>
        /// Return TopK information.
        /// </summary>
        /// <param name="key">Name of the key to return information about.</param>
        /// <returns>TopK Information.</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.info"/></remarks>
        public async Task<TopKInformation?> InfoAsync(RedisKey key)
        {
            var info = await _db.ExecuteAsync(TOPK.INFO, key);
            return ResponseParser.ToTopKInfo(info);
        }

        /// <summary>
        /// Return full list of items in Top K list.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="withcount">return Count of each element is returned.</param>
        /// <returns>Full list of items in Top K list</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.list"/></remarks>
        public RedisResult[]? List(RedisKey key, bool withcount = false)
        {
            var result = (withcount) ? _db.Execute(TOPK.LIST, key, "WITHCOUNT")
                                     : _db.Execute(TOPK.LIST, key);
            return ResponseParser.ToArray(result);
        }

        /// <summary>
        /// Return full list of items in Top K list.
        /// </summary>
        /// <param name="key">The name of the sketch.</param>
        /// <param name="withcount">return Count of each element is returned.</param>
        /// <returns>Full list of items in Top K list</returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.list"/></remarks>
        public async Task<RedisResult[]?> ListAsync(RedisKey key, bool withcount = false)
        {
            var result = await ((withcount) ? _db.ExecuteAsync(TOPK.LIST, key, "WITHCOUNT")
                                            : _db.ExecuteAsync(TOPK.LIST, key));
            return ResponseParser.ToArray(result);
        }

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="item">Item to be queried.</param>
        /// <returns><see langword="true"/> if item is in Top-K, <see langword="false"/> otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.query"/></remarks>
        public bool? Query(RedisKey key, RedisValue item)
        {
            return _db.Execute(TOPK.QUERY, key, item).ToString() == "1";
        }

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="item">Item to be queried.</param>
        /// <returns><see langword="true"/> if item is in Top-K, <see langword="false"/> otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.query"/></remarks>
        public async Task<bool?> QueryAsync(RedisKey key, RedisValue item)
        {
            var result = await _db.ExecuteAsync(TOPK.QUERY, key, item);
            return result.ToString() == "1";
        }

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="items">Items to be queried.</param>
        /// <returns>Bolean Array where <see langword="true"/> if item is in Top-K, <see langword="false"/> otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.query"/></remarks>
        public bool[]? Query(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentNullException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);

            return ResponseParser.ToBooleanArray(_db.Execute(TOPK.QUERY, args));
        }

        /// <summary>
        /// Returns the count for one or more items in a sketch.
        /// </summary>
        /// <param name="key">The name of the sketch</param>
        /// <param name="items">Items to be queried.</param>
        /// <returns>Bolean Array where <see langword="true"/> if item is in Top-K, <see langword="false"/> otherwise/></returns>
        /// <remarks><seealso href="https://redis.io/commands/topk.query"/></remarks>
        public async Task<bool[]?> QueryAsync(RedisKey key, params RedisValue[] items)
        {
            if (items.Length < 1)
                throw new ArgumentNullException(nameof(items));

            var args = Auxiliary.MergeArgs(key, items);

            var result = await _db.ExecuteAsync(TOPK.QUERY, args);
            return ResponseParser.ToBooleanArray(result);
        }

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
        public bool? Reserve(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9)
        {
            return ResponseParser.ParseOKtoBoolean(_db.Execute(TOPK.RESERVE, key, topk, width, depth, decay));
        }

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
        public async Task<bool?> ReserveAsync(RedisKey key, long topk, long width = 7, long depth = 8, double decay = 0.9)
        {
            var result = await _db.ExecuteAsync(TOPK.RESERVE, key, topk, width, depth, decay);
            return ResponseParser.ParseOKtoBoolean(result);
        }
    }
}
