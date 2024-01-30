using StackExchange.Redis;

namespace NRedisStack.Core.DataTypes;

/// <summary>
/// Modifier that can be used for sorted set commands, where a MIN/MAX argument is expected by the Redis server.
/// </summary>
public enum MinMaxModifier
{
    /// <summary>
    /// Maps to the <c>MIN</c> argument on the Redis server.
    /// </summary>
    Min,

    /// <summary>
    /// Maps to the <c>MAX</c> argument on the Redis server.
    /// </summary>
    Max
}

/// <summary>
/// Conversion methods from/to other common data types.
/// </summary>
public static class MinMaxModifierExtensions
{
    /// <summary>
    /// Convert from <see cref="Order"/> to <see cref="MinMaxModifier"/>.
    /// </summary>
    public static MinMaxModifier ToMinMax(this Order order) => order switch
    {
        Order.Ascending => MinMaxModifier.Min,
        Order.Descending => MinMaxModifier.Max,
        _ => throw new ArgumentOutOfRangeException(nameof(order))
    };
}