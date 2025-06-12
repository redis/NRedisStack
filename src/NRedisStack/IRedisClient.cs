using StackExchange.Redis;

namespace NRedisStack;

/// <summary>
/// This class is EXPERIMENTAL!! and may change or removed in future releases.
/// Represents a Redis client that can connect to a Redis server 
/// providing access to <see cref="IRedisDatabase"/> instances as well as the underlying multiplexer.
/// </summary>
public interface IRedisClient
{
    /// <summary>
    /// Gets a database instance.
    /// </summary>
    /// <param name="db">The ID to get a database for.</param>
    /// <param name="asyncState">The async state to pass into the resulting <see cref="RedisDatabase"/>.</param>
    /// <returns></returns>
    public IRedisDatabase GetDatabase(int db = -1, object? asyncState = null);

    /// <summary>
    /// Gets the underlying <see cref="ConnectionMultiplexer"/> instance.
    /// </summary>
    /// <returns></returns>
    public IConnectionMultiplexer GetMultiplexer();
}



