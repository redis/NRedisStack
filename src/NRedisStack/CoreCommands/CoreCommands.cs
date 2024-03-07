using NRedisStack.Core;
using NRedisStack.Core.DataTypes;
using StackExchange.Redis;

namespace NRedisStack;

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
    /// The BLMPOP command.
    /// <p/>
    /// Removes and returns up to <paramref name="count"/> entries from the first non-empty list in
    /// <paramref name="keys"/>. If none of the lists contain elements, the call blocks on the server until elements
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
    /// <param name="listSide">Specify from which end of the list to pop values: left or right.</param>
    /// <param name="count">The maximum number of records to pop. If set to <c>null</c> then the server default
    /// will be used.</param>
    /// <returns>A collection of values, together with the key they were popped from, or <c>null</c> if the
    /// server timeout expires.</returns>
    /// <remarks><seealso href="https://redis.io/commands/blmpop"/></remarks>
    public static Tuple<RedisKey, List<RedisValue>>? BLMPop(this IDatabase db, double timeout, RedisKey[] keys, ListSide listSide, long? count = null)
    {
        var command = CoreCommandBuilder.BLMPop(timeout, keys, listSide, count);
        return db.Execute(command).ToListPopResults();
    }

    /// <summary>
    /// Syntactic sugar for
    /// <see cref="BLMPop(StackExchange.Redis.IDatabase,double,StackExchange.Redis.RedisKey[],StackExchange.Redis.ListSide,System.Nullable{long})"/>,
    /// where only one key is used.
    /// </summary>
    /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
    /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
    /// <param name="key">The key to check.</param>
    /// <param name="listSide">Specify from which end of the list to pop values: left or right.</param>
    /// <param name="count">The maximum number of records to pop. If set to <c>null</c> then the server default
    /// will be used.</param>
    /// <returns>A collection of values, together with the key they were popped from, or <c>null</c> if the
    /// server timeout expires.</returns>
    /// <remarks><seealso href="https://redis.io/commands/blmpop"/></remarks>
    public static Tuple<RedisKey, List<RedisValue>>? BLMPop(this IDatabase db, double timeout, RedisKey key, ListSide listSide, long? count = null)
    {
        return BLMPop(db, timeout, new[] { key }, listSide, count);
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
    /// Syntactic sugar for <see cref="BRPop(StackExchange.Redis.IDatabase,StackExchange.Redis.RedisKey[],double)"/>,
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

    /// <summary>
    /// The BLMOVE command.
    /// <p/>
    /// Atomically returns and removes the first or last element of the list stored at <paramref name="source"/>
    /// (depending on the value of <paramref name="sourceSide"/>), and pushes the element as the first or last
    /// element of the list stored at <paramref name="destination"/> (depending on the value of
    /// <paramref name="destinationSide"/>).
    /// <p/>
    /// This is an extension method added to the <see cref="IDatabase"/> class, for convenience.
    /// </summary>
    /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
    /// <param name="source">The key of the source list.</param>
    /// <param name="destination">The key of the destination list.</param>
    /// <param name="sourceSide">What side of the <paramref name="source"/> list to remove from.</param>
    /// <param name="destinationSide">What side of the <paramref name="destination"/> list to move to.</param>
    /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
    /// <returns>The element being popped and pushed, or <c>null</c> if the server timeout expires.</returns>
    /// <remarks><seealso href="https://redis.io/commands/blmove"/></remarks>
    public static RedisValue? BLMove(this IDatabase db, RedisKey source, RedisKey destination, ListSide sourceSide, ListSide destinationSide, double timeout)
    {
        var command = CoreCommandBuilder.BLMove(source, destination, sourceSide, destinationSide, timeout);
        return db.Execute(command).ToRedisValue();
    }

    /// <summary>
    /// The BRPOPLPUSH command.
    /// <p/>
    /// Atomically returns and removes the last element (tail) of the list stored at source, and pushes the element
    /// at the first element (head) of the list stored at destination.
    /// <p/>
    /// This is an extension method added to the <see cref="IDatabase"/> class, for convenience.
    /// </summary>
    /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
    /// <param name="source">The key of the source list.</param>
    /// <param name="destination">The key of the destination list.</param>
    /// <param name="timeout">Server-side timeout for the wait. A value of <c>0</c> means to wait indefinitely.</param>
    /// <returns>The element being popped and pushed, or <c>null</c> if the server timeout expires.</returns>
    /// <remarks><seealso href="https://redis.io/commands/rpoplpush"/></remarks>
    public static RedisValue? BRPopLPush(this IDatabase db, RedisKey source, RedisKey destination, double timeout)
    {
        var command = CoreCommandBuilder.BRPopLPush(source, destination, timeout);
        return db.Execute(command).ToRedisValue();
    }

    /// <summary>
    /// The XREAD command.
    /// <para/>
    /// Read data from one or multiple streams, only returning entries with an ID greater than an ID provided by the caller.
    /// </summary>
    /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
    /// <param name="keys">Keys of the streams where to read from.</param>
    /// <param name="positions">The positions from which to begin reading for each stream. See
    /// <see cref="NRedisStack.Core.DataTypes.StreamSpecialIds"/> for special Ids that can be used.</param>
    /// <param name="count">The maximum number of messages to return from each stream.</param>
    /// <param name="timeoutMilliseconds">Amount of time in milliseconds to block in case all the streams are empty.
    /// If not provided, or set to <c>null</c> then the read does not block. If set to <c>0</c> then it blocks indefinitely.</param>
    /// <returns>A value of <see cref="RedisStreamEntries"/> for each stream, or <c>null</c> if the command times out
    /// on the server.</returns>
    /// <remarks>
    /// <para>This is the blocking alternative for <seealso cref="IDatabase.StreamRead(StackExchange.Redis.StreamPosition[],System.Nullable{int},StackExchange.Redis.CommandFlags)"/>.</para>
    /// <para><seealso href="https://redis.io/commands/xread"/></para>
    /// </remarks>
    public static RedisStreamEntries[]? XRead(this IDatabase db, RedisKey[] keys, RedisValue[] positions, int? count = null, int? timeoutMilliseconds = null)
    {
        var command = CoreCommandBuilder.XRead(keys, positions, count, timeoutMilliseconds);
        return db.Execute(command).ToRedisStreamEntries();
    }

    /// <summary>
    /// Syntactic sugar for <see cref="XRead(StackExchange.Redis.IDatabase,StackExchange.Redis.RedisKey[],StackExchange.Redis.RedisValue[],System.Nullable{int},System.Nullable{int})"/>,
    /// where only one stream is being read from.
    /// </summary>
    /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
    /// <param name="key">Key of the stream where to read from.</param>
    /// <param name="position">The position from which to begin reading. See
    /// <see cref="NRedisStack.Core.DataTypes.StreamSpecialIds"/> for special Ids that can be used.</param>
    /// <param name="count">The maximum number of messages to return from each stream.</param>
    /// <param name="timeoutMilliseconds">Amount of time in milliseconds to block in case all the streams are empty.
    /// If not provided, or set to <c>null</c> then the read does not block. If set to <c>0</c> then it blocks indefinitely.</param>
    /// <returns>A <see cref="StreamEntry"/> list with the data read from the stream, of <c>null</c> if the command
    /// times out on the server.</returns>
    /// <remarks>
    /// <para>This is the blocking alternative for <seealso cref="IDatabase.StreamRead(StackExchange.Redis.RedisKey,StackExchange.Redis.RedisValue,System.Nullable{int},StackExchange.Redis.CommandFlags)"/>.</para>
    /// <para><seealso href="https://redis.io/commands/xread"/></para>
    /// </remarks>
    public static StreamEntry[]? XRead(this IDatabase db, RedisKey key, RedisValue position, int? count = null, int? timeoutMilliseconds = null)
    {
        var result = XRead(db, new[] { key }, new[] { position }, count, timeoutMilliseconds);
        if (result == null || result.Length == 0)
        {
            return null;
        }
        return result[0].Entries;
    }

    /// <summary>
    /// The XREADGROUP command.
    /// <para/>
    /// Read new or historical messages in one or several streams, for a consumer in a consumer group.
    /// </summary>
    /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
    /// <param name="groupName">The consumer group name.</param>
    /// <param name="consumerName">The name of the consumer in the consumer group.</param>
    /// <param name="keys">Keys of the streams where to read from.</param>
    /// <param name="positions">The positions from which to begin reading for each stream. See
    /// <see cref="NRedisStack.Core.DataTypes.StreamSpecialIds"/> for special Ids that can be used.</param>
    /// <param name="count">The maximum number of messages to return from each stream.</param>
    /// <param name="timeoutMilliseconds">Amount of time in milliseconds to block in case all the streams are empty.
    /// If not provided, or set to <c>null</c> then the read does not block. If set to <c>0</c> then it blocks indefinitely.</param>
    /// <param name="noAck">If set to <c>true</c> then inform the server that it should not wait for ACK for the
    /// messages it sends to this read call.</param>
    /// <returns>A value of <see cref="RedisStreamEntries"/> for each stream, or <c>null</c> if the command times out
    /// on the server.</returns>
    /// <remarks>
    /// <para>This is the blocking alternative for <seealso cref="IDatabase.StreamReadGroup(StackExchange.Redis.StreamPosition[],StackExchange.Redis.RedisValue,StackExchange.Redis.RedisValue,System.Nullable{int},StackExchange.Redis.CommandFlags)"/>.</para>
    /// <para><seealso href="https://redis.io/commands/xreadgroup"/></para>
    /// </remarks>
    public static RedisStreamEntries[]? XReadGroup(this IDatabase db, RedisValue groupName, RedisValue consumerName, RedisKey[] keys, RedisValue[] positions, int? count = null, int? timeoutMilliseconds = null, bool? noAck = null)
    {
        var command = CoreCommandBuilder.XReadGroup(groupName, consumerName, keys, positions, count, timeoutMilliseconds, noAck);
        return db.Execute(command).ToRedisStreamEntries();
    }

    /// <summary>
    /// Syntactic sugar for <see cref="XReadGroup(StackExchange.Redis.IDatabase,StackExchange.Redis.RedisValue,StackExchange.Redis.RedisValue,StackExchange.Redis.RedisKey[],StackExchange.Redis.RedisValue[],System.Nullable{int},System.Nullable{int},System.Nullable{bool})"/>,
    /// where only one stream is being read from.
    /// </summary>
    /// <param name="db">The <see cref="IDatabase"/> class where this extension method is applied.</param>
    /// <param name="groupName">The consumer group name.</param>
    /// <param name="consumerName">The name of the consumer in the consumer group.</param>
    /// <param name="key">Key of the stream where to read from.</param>
    /// <param name="position">The position from which to begin reading. See
    /// <see cref="NRedisStack.Core.DataTypes.StreamSpecialIds"/> for special Ids that can be used.</param>
    /// <param name="count">The maximum number of messages to return from each stream.</param>
    /// <param name="timeoutMilliseconds">Amount of time in milliseconds to block in case all the streams are empty.
    /// If not provided, or set to <c>null</c> then the read does not block. If set to <c>0</c> then it blocks indefinitely.</param>
    /// <param name="noAck">If set to <c>true</c> then inform the server that it should not wait for ACK for the
    /// messages it sends to this read call.</param>
    /// <returns>A <see cref="StreamEntry"/> list with the data read from the stream, of <c>null</c> if the command
    /// times out on the server.</returns>
    /// <remarks>
    /// <para>This is the blocking alternative for <seealso cref="IDatabase.StreamReadGroup(StackExchange.Redis.RedisKey,StackExchange.Redis.RedisValue,StackExchange.Redis.RedisValue,System.Nullable{StackExchange.Redis.RedisValue},System.Nullable{int},StackExchange.Redis.CommandFlags)"/>.</para>
    /// <para><seealso href="https://redis.io/commands/xreadgroup"/></para>
    /// </remarks>
    public static StreamEntry[]? XReadGroup(this IDatabase db, RedisValue groupName, RedisValue consumerName, RedisKey key, RedisValue position, int? count = null, int? timeoutMilliseconds = null, bool? noAck = null)
    {
        var result = XReadGroup(db, groupName, consumerName, new[] { key }, new[] { position }, count, timeoutMilliseconds, noAck);
        if (result == null || result.Length == 0)
        {
            return null;
        }
        return result[0].Entries;
    }
}
}

