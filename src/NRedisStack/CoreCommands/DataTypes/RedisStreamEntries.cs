using StackExchange.Redis;

namespace NRedisStack.Core.DataTypes;

/// <summary>
/// Holds the key and the entries for a Redis Stream, as returned by, for example, the XREAD or the XREADGROUP commands.
/// </summary>
public readonly struct RedisStreamEntries
{
    internal RedisStreamEntries(RedisKey key, StreamEntry[] entries)
    {
        Key = key;
        Entries = entries;
    }

    /// <summary>
    /// The key for the stream.
    /// </summary>
    public RedisKey Key { get; }

    /// <summary>
    /// An array of entries contained within the stream.
    /// </summary>
    public StreamEntry[] Entries { get; }
}
