using StackExchange.Redis;

namespace NRedisStack;

/// <summary>
/// This class is EXPERIMENTAL!! and may change or removed in future releases.
/// Interface that provides access to Redis commands
/// including Search, JSON, TimeSeries, Bloom and more..
/// </summary>
public interface IRedisDatabase : IDatabase
{
    /// <summary>
    /// Gets a command set for interacting with the RedisBloom Bloom Filter module.
    /// </summary>
    /// <returns>Command set for Bloom Filter operations.</returns>
    public BloomCommands BF();

    /// <summary>
    /// Gets a command set for interacting with the RedisBloom Count-Min Sketch module.
    /// </summary>
    /// <returns>Command set for Count-Min Sketch operations.</returns>
    public CmsCommands CMS();

    /// <summary>
    /// Gets a command set for interacting with the RedisBloom Cuckoo Filter module.
    /// </summary>
    /// <returns>Command set for Cuckoo Filter operations.</returns>
    public CuckooCommands CF();

    /// <summary>
    /// Gets a command set for interacting with the RedisJSON module.
    /// </summary>
    /// <returns>Command set for JSON operations.</returns>
    public JsonCommands JSON();

    /// <summary>
    /// Gets a command set for interacting with the RediSearch module.
    /// </summary>
    /// <param name="searchDialect">The search dialect version to use. Defaults to 2.</param>
    /// <returns>Command set for search operations.</returns>
    public SearchCommands FT(int? searchDialect = 2);

    /// <summary>
    /// Gets a command set for interacting with the Redis t-digest module.
    /// </summary>
    /// <returns>Command set for t-digest operations.</returns>
    public TdigestCommands TDIGEST();

    /// <summary>
    /// Gets a command set for interacting with the RedisTimeSeries module.
    /// </summary>
    /// <returns>Command set for time series operations.</returns>
    public TimeSeriesCommands TS();

    /// <summary>
    /// Gets a command set for interacting with the RedisBloom TopK module.
    /// </summary>
    /// <returns>Command set for TopK operations.</returns>
    public TopKCommands TOPK();
}