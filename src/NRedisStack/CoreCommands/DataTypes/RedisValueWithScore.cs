using StackExchange.Redis;

namespace NRedisStack.Core.DataTypes;

/// <summary>
/// Holds a <see cref="RedisValue"/> with an associated score.
/// Used when working with sorted sets.
/// </summary>
public struct RedisValueWithScore
{
    /// <summary>
    /// Pair a <see cref="RedisValue"/> with a numeric score.
    /// </summary>
    public RedisValueWithScore(RedisValue value, double score)
    {
        Value = value;
        Score = score;
    }

    /// <summary>
    /// The value of an item stored in a sorted set. For example, in the Redis command
    /// <c>ZADD my-set 5.1 my-value</c>, the value is <c>my-value</c>.
    /// </summary>
    public RedisValue Value { get; }

    /// <summary>
    /// The score of an item stored in a sorted set. For example, in the Redis command
    /// <c>ZADD my-set 5.1 my-value</c>, the score is <c>5.1</c>.
    /// </summary>
    public double Score { get; }
}
