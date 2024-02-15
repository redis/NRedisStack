using NRedisStack.Core;
using NRedisStack.Core.DataTypes;
using StackExchange.Redis;

namespace NRedisStack
{

    public static class CoreCommands
    {
        /// <summary>
        /// Sets information specific to the client or connection.
        /// </summary>
        /// <param name="attr">which attribute to set</param>
        /// <param name="value">the attribute value</param>
        /// <returns><see langword="true"/> if the attribute name was successfully set, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/client-setinfo/"/></remarks>
        public static bool ClientSetInfo(this IDatabase db, SetInfoAttr attr, string value)
        {
            var compareVersions = db.Multiplexer.GetServer(db.Multiplexer.GetEndPoints()[0]).Version.CompareTo(new Version(7, 1, 242));
            if (compareVersions < 0) // the server does not support the CLIENT SETNAME command
                return false;
            return db.Execute(CoreCommandBuilder.ClientSetInfo(attr, value)).OKtoBoolean();
        }

        /// <summary>
        /// The BZMPOP command.
        /// <p/>
        /// Removes and returns up to <paramref name="count"/> entries from the first non-empty sorted set in
        /// <paramref name="keys"/>. If none of the sets contain elements, the call blocks on the server until elements
        /// become available, or the given <paramref name="timeout"/> expires. A <paramref name="timeout"/> of <c>0</c>
        /// means to wait indefinitely server-side. Returns <c>null</c> if the server timeout expires. 
        /// <p/>
        /// When using this, pay attention to the timeout configured in the client, on the
        /// <see cref="ConnectionMultiplexer"/>, which by default can be too small:
        /// <code>
        /// ConfigurationOptions configurationOptions = new ConfigurationOptions();
        /// configurationOptions.SyncTimeout = 120000; // set a meaningful value here
        /// configurationOptions.EndPoints.Add("localhost");
        /// ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(configurationOptions);
        /// </code>
        /// If the connection multiplexer timeout expires in the client, a <c>StackExchange.Redis.RedisTimeoutException</c>
        /// is thrown.
        /// <p/>
        /// This is an extension method added to the <see cref="IDatabase"/> class, for convenience.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <param name="keys">The keys to check.</param>
        /// <param name="minMaxModifier">Specify from which end of the sorted set to pop values. If set to <c>MinMaxModifier.Min</c>
        /// then the minimum elements will be popped, otherwise the maximum values.</param>
        /// <param name="count">The maximum number of records to pop out. If set to <c>null</c> then the server default
        /// will be used.</param>
        /// <returns>A collection of sorted set entries paired with their scores, together with the key they were popped
        /// from, or <c>null</c> if the server timeout expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bzmpop"/></remarks>
        public static Tuple<RedisKey, List<RedisValueWithScore>>? BZMPop(this IDatabase db, double timeout, RedisKey[] keys, MinMaxModifier minMaxModifier, long? count = null)
        {
            var command = CoreCommandBuilder.BZMPop(timeout, keys, minMaxModifier, count);
            return db.Execute(command).ToSortedSetPopResults();
        }

        /// <summary>
        /// Syntactic sugar for
        /// <see cref="BZMPop(StackExchange.Redis.IDatabase,double,StackExchange.Redis.RedisKey[],NRedisStack.Core.DataTypes.MinMaxModifier,System.Nullable{long})"/>,
        /// where only one key is used.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <param name="key">The key to check.</param>
        /// <param name="minMaxModifier">Specify from which end of the sorted set to pop values. If set to <c>MinMaxModifier.Min</c>
        /// then the minimum elements will be popped, otherwise the maximum values.</param>
        /// <param name="count">The maximum number of records to pop out. If set to <c>null</c> then the server default
        /// will be used.</param>
        /// <returns>A collection of sorted set entries paired with their scores, together with the key they were popped
        /// from, or <c>null</c> if the server timeout expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bzmpop"/></remarks>
        public static Tuple<RedisKey, List<RedisValueWithScore>>? BZMPop(this IDatabase db, double timeout, RedisKey key, MinMaxModifier minMaxModifier, long? count = null)
        {
            return BZMPop(db, timeout, new[] { key }, minMaxModifier, count);
        }

        /// <summary>
        /// The BZPOPMIN command.
        /// <p/>
        /// Removes and returns the entry with the smallest score from the first non-empty sorted set in
        /// <paramref name="keys"/>. If none of the sets contain elements, the call blocks on the server until elements
        /// become available, or the given <paramref name="timeout"/> expires. A <paramref name="timeout"/> of <c>0</c>
        /// means to wait indefinitely server-side. Returns <c>null</c> if the server timeout expires.
        /// <p/>
        /// When using this, pay attention to the timeout configured in the client, on the
        /// <see cref="ConnectionMultiplexer"/>, which by default can be too small:
        /// <code>
        /// ConfigurationOptions configurationOptions = new ConfigurationOptions();
        /// configurationOptions.SyncTimeout = 120000; // set a meaningful value here
        /// configurationOptions.EndPoints.Add("localhost");
        /// ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(configurationOptions);
        /// </code>
        /// If the connection multiplexer timeout expires in the client, a <c>StackExchange.Redis.RedisTimeoutException</c>
        /// is thrown.
        /// <p/>
        /// This is an extension method added to the <see cref="IDatabase"/> class, for convenience.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="keys">The keys to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A sorted set entry paired with its score, together with the key it was popped from, or <c>null</c>
        /// if the server timeout expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bzpopmin"/></remarks>
        public static Tuple<RedisKey, RedisValueWithScore>? BZPopMin(this IDatabase db, RedisKey[] keys, double timeout)
        {
            var command = CoreCommandBuilder.BZPopMin(keys, timeout);
            return db.Execute(command).ToSortedSetPopResult();
        }

        /// <summary>
        /// Syntactic sugar for <see cref="BZPopMin(StackExchange.Redis.IDatabase,StackExchange.Redis.RedisKey[],double)"/>,
        /// where only one key is used.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="key">The key to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A sorted set entry paired with its score, together with the key it was popped from, or <c>null</c>
        /// if the server timeout expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bzpopmin"/></remarks>
        public static Tuple<RedisKey, RedisValueWithScore>? BZPopMin(this IDatabase db, RedisKey key, double timeout)
        {
            return BZPopMin(db, new[] { key }, timeout);
        }


        /// <summary>
        /// The BZPOPMAX command.
        /// <p/>
        /// Removes and returns the entry with the highest score from the first non-empty sorted set in
        /// <paramref name="keys"/>. If none of the sets contain elements, the call blocks on the server until elements
        /// become available, or the given <paramref name="timeout"/> expires. A <paramref name="timeout"/> of <c>0</c>
        /// means to wait indefinitely server-side. Returns <c>null</c> if the server timeout expires.
        /// <p/>
        /// When using this, pay attention to the timeout configured in the client, on the
        /// <see cref="ConnectionMultiplexer"/>, which by default can be too small:
        /// <code>
        /// ConfigurationOptions configurationOptions = new ConfigurationOptions();
        /// configurationOptions.SyncTimeout = 120000; // set a meaningful value here
        /// configurationOptions.EndPoints.Add("localhost");
        /// ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(configurationOptions);
        /// </code>
        /// If the connection multiplexer timeout expires in the client, a <c>StackExchange.Redis.RedisTimeoutException</c>
        /// is thrown.
        /// <p/>
        /// This is an extension method added to the <see cref="IDatabase"/> class, for convenience.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="keys">The keys to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A sorted set entry paired with its score, together with the key it was popped from, or <c>null</c>
        /// if the server timeout expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bzpopmax"/></remarks>
        public static Tuple<RedisKey, RedisValueWithScore>? BZPopMax(this IDatabase db, RedisKey[] keys, double timeout)
        {
            var command = CoreCommandBuilder.BZPopMax(keys, timeout);
            return db.Execute(command).ToSortedSetPopResult();
        }

        /// <summary>
        /// Syntactic sugar for <see cref="BZPopMax(StackExchange.Redis.IDatabase,StackExchange.Redis.RedisKey[],double)"/>,
        /// where only one key is used.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="key">The key to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A sorted set entry paired with its score, together with the key it was popped from, or <c>null</c>
        /// if the server timeout expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bzpopmax"/></remarks>
        public static Tuple<RedisKey, RedisValueWithScore>? BZPopMax(this IDatabase db, RedisKey key, double timeout)
        {
            return BZPopMax(db, new[] { key }, timeout);
        }

        /// <summary>
        /// The BLPOP command.
        /// <p/>
        /// Removes and returns an entry from the head (left side) of the first non-empty list in <paramref name="keys"/>.
        /// If none of the lists contain elements, the call blocks on the server until elements
        /// become available, or the given <paramref name="timeout"/> expires. A <paramref name="timeout"/> of <c>0</c>
        /// means to wait indefinitely server-side. Returns <c>null</c> if the server timeout expires.
        /// <p/>
        /// When using this, pay attention to the timeout configured in the client, on the
        /// <see cref="ConnectionMultiplexer"/>, which by default can be too small:
        /// <code>
        /// ConfigurationOptions configurationOptions = new ConfigurationOptions();
        /// configurationOptions.SyncTimeout = 120000; // set a meaningful value here
        /// configurationOptions.EndPoints.Add("localhost");
        /// ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(configurationOptions);
        /// </code>
        /// If the connection multiplexer timeout expires in the client, a <c>StackExchange.Redis.RedisTimeoutException</c>
        /// is thrown.
        /// <p/>
        /// This is an extension method added to the <see cref="IDatabase"/> class, for convenience.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="keys">The keys to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A value, together with the key it was popped from, or <c>null</c> if the server timeout
        /// expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/blpop"/></remarks>
        public static Tuple<RedisKey, RedisValue>? BLPop(this IDatabase db, RedisKey[] keys, double timeout)
        {
            var command = CoreCommandBuilder.BLPop(keys, timeout);
            return db.Execute(command).ToListPopResult();
        }

        /// <summary>
        /// Syntactic sugar for <see cref="BLPop(StackExchange.Redis.IDatabase,StackExchange.Redis.RedisKey[],double)"/>,
        /// where only one key is used.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="key">The key to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A value, together with the key it was popped from, or <c>null</c> if the server timeout
        /// expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/blpop"/></remarks>
        public static Tuple<RedisKey, RedisValue>? BLPop(this IDatabase db, RedisKey key, double timeout)
        {
            return BLPop(db, new[] { key }, timeout);
        }

        /// <summary>
        /// The BRPOP command.
        /// <p/>
        /// Removes and returns an entry from the tail (right side) of the first non-empty list in <paramref name="keys"/>.
        /// If none of the lists contain elements, the call blocks on the server until elements
        /// become available, or the given <paramref name="timeout"/> expires. A <paramref name="timeout"/> of <c>0</c>
        /// means to wait indefinitely server-side. Returns <c>null</c> if the server timeout expires.
        /// <p/>
        /// When using this, pay attention to the timeout configured in the client, on the
        /// <see cref="ConnectionMultiplexer"/>, which by default can be too small:
        /// <code>
        /// ConfigurationOptions configurationOptions = new ConfigurationOptions();
        /// configurationOptions.SyncTimeout = 120000; // set a meaningful value here
        /// configurationOptions.EndPoints.Add("localhost");
        /// ConnectionMultiplexer redis = ConnectionMultiplexer.Connect(configurationOptions);
        /// </code>
        /// If the connection multiplexer timeout expires in the client, a <c>StackExchange.Redis.RedisTimeoutException</c>
        /// is thrown.
        /// <p/>
        /// This is an extension method added to the <see cref="IDatabase"/> class, for convenience.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="keys">The keys to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A value, together with the key it was popped from, or <c>null</c> if the server timeout
        /// expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/brpop"/></remarks>
        public static Tuple<RedisKey, RedisValue>? BRPop(this IDatabase db, RedisKey[] keys, double timeout)
        {
            var command = CoreCommandBuilder.BRPop(keys, timeout);
            return db.Execute(command).ToListPopResult();
        }

        /// <summary>
        /// Syntactic sugar for <see cref="BLPop(StackExchange.Redis.IDatabase,StackExchange.Redis.RedisKey[],double)"/>,
        /// where only one key is used.
        /// </summary>
        /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
        /// <param name="key">The key to check.</param>
        /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
        /// <returns>A value, together with the key it was popped from, or <c>null</c> if the server timeout
        /// expires.</returns>
        /// <remarks><seealso href="https://redis.io/commands/brpop"/></remarks>
        public static Tuple<RedisKey, RedisValue>? BRPop(this IDatabase db, RedisKey key, double timeout)
        {
            return BRPop(db, new[] { key }, timeout);
        }
    }
}
