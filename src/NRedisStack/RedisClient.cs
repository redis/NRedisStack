using StackExchange.Redis;

namespace NRedisStack;

/// <summary>
/// This class is EXPERIMENTAL!! and may change or removed in future releases.
/// Represents a Redis client that can connect to a Redis server 
/// providing access to <see cref="IRedisDatabase"/> instances as well as the underlying multiplexer.
/// </summary>
public class RedisClient: IRedisClient
{
    private IConnectionMultiplexer _multiplexer;

    private RedisClient(IConnectionMultiplexer multiplexer)
    {
        _multiplexer = multiplexer;
    }

    /// <summary>
    /// Creates a new <see cref="RedisClient"/> instance.
    /// </summary>
    /// <param name="configuration">The string configuration to use for this client.</param>
    /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
    public static async Task<IRedisClient> ConnectAsync(string configuration, TextWriter? log = null) =>
        await ConnectAsync(ConfigurationOptions.Parse(configuration), log);

    /// <summary>
    /// Creates a new <see cref="RedisClient"/> instance.
    /// </summary>
    /// <param name="configuration">The string configuration to use for this client.</param>
    /// <param name="configure">Action to further modify the parsed configuration options.</param>
    /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
    public static async Task<IRedisClient> ConnectAsync(string configuration, Action<ConfigurationOptions> configure, TextWriter? log = null)
    {
        Action<ConfigurationOptions> config = (ConfigurationOptions config) =>
        {
            configure?.Invoke(config);
            SetNames(config);
        };
        return new RedisClient(await ConnectionMultiplexer.ConnectAsync(configuration, configure, log));
    }

    /// <summary>
    /// Creates a new <see cref="ConnectionMultiplexer"/> instance.
    /// </summary>
    /// <param name="configuration">The configuration options to use for this client.</param>
    /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
    /// <remarks>Note: For Sentinel, do <b>not</b> specify a <see cref="ConfigurationOptions.CommandMap"/> - this is handled automatically.</remarks>
    public static async Task<IRedisClient> ConnectAsync(ConfigurationOptions configuration, TextWriter? log = null)
    {
        SetNames(configuration);
        return new RedisClient(await ConnectionMultiplexer.ConnectAsync(configuration, log));
    }

    /// <summary>
    /// Creates a new <see cref="RedisClient"/> instance.
    /// </summary>
    /// <param name="configuration">The string configuration to use for this client. See the StackExchange.Redis configuration documentation(https://stackexchange.github.io/StackExchange.Redis/Configuration) for detailed information.</param>
    /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
    public static IRedisClient Connect(string configuration, TextWriter? log = null) =>
         Connect(ConfigurationOptions.Parse(configuration), log);

    /// <summary>
    /// Creates a new <see cref="RedisClient"/> instance.
    /// </summary>
    /// <param name="configuration">The string configuration to use for this client.</param>
    /// <param name="configure">Action to further modify the parsed configuration options.</param>
    /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
    public static IRedisClient Connect(string configuration, Action<ConfigurationOptions> configure, TextWriter? log = null)
    {
        Action<ConfigurationOptions> config = (ConfigurationOptions config) =>
        {
            configure?.Invoke(config);
            SetNames(config);
        };
        return new RedisClient(ConnectionMultiplexer.Connect(configuration, configure, log));
    }

    /// <summary>
    /// Creates a new <see cref="RedisClient"/> instance.
    /// </summary>
    /// <param name="configuration">The configuration options to use for this client.</param>
    /// <param name="log">The <see cref="TextWriter"/> to log to.</param>
    /// <remarks>Note: For Sentinel, do <b>not</b> specify a <see cref="ConfigurationOptions.CommandMap"/> - this is handled automatically.</remarks>
    public static IRedisClient Connect(ConfigurationOptions configuration, TextWriter? log = null)
    {
        SetNames(configuration);
        return new RedisClient(ConnectionMultiplexer.Connect(configuration, log));
    }

    /// <summary>
    /// Gets a database instance.
    /// </summary>
    /// <param name="db">The ID to get a database for.</param>
    /// <param name="asyncState">The async state to pass into the resulting <see cref="RedisDatabase"/>.</param>
    /// <returns></returns>
    public IRedisDatabase GetDatabase(int db = -1, object? asyncState = null)
    {
        var idatabase = _multiplexer.GetDatabase(db, asyncState);
        return new RedisDatabase(idatabase);
    }

    /// <summary>
    /// Gets the underlying <see cref="ConnectionMultiplexer"/> instance.
    /// </summary>
    /// <returns></returns>
    public IConnectionMultiplexer GetMultiplexer()
    {
        return _multiplexer;
    }

    private static void SetNames(ConfigurationOptions config)
    {
        if (config.LibraryName == null)
        {
            config.LibraryName = Auxiliary.GetNRedisStackLibName();
        }

        if (config.ClientName == null)
        {
            config.ClientName = (Environment.MachineName ?? Environment.GetEnvironmentVariable("ComputerName") ?? "Unknown") + $"-NRedisStack(.NET_v{Environment.Version})";
        }
    }
}



