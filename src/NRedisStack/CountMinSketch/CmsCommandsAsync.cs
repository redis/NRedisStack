using NRedisStack.CountMinSketch.DataTypes;
using StackExchange.Redis;

namespace NRedisStack;

/// <summary>
/// The original Count-Min Sketch surface, served from the <see cref="RespCountMinSketch"/> group.
/// </summary>
/// <remarks>
/// Every method here is a thin proxy: <c>db.CountMinSketch.XxxAsync(...)</c> plus whatever adaptation the
/// old signature needs. <see cref="IDatabaseAsync"/> carries the context directly, so this works for a
/// database, a batch, a transaction, and the async-only retry wrapper alike. The <see cref="Task"/> shape is
/// kept because this is legacy API; new code should use the group's <see cref="ValueTask"/> methods, which
/// can complete synchronously (client-side caching). The conversion is <c>db.AsTask(...)</c>, which returns
/// the task <see cref="IDatabaseAsync"/>'s own methods would: it carries the database's async state, and a
/// fault is marked observed as it happens, so a discarded task (<c>_ = tran.Cms.InitByDimAsync(...)</c>, then a
/// transaction that aborts) never raises <see cref="TaskScheduler.UnobservedTaskException"/>.
/// </remarks>
public class CmsCommandsAsync : ICmsCommandsAsync
{
    private readonly IDatabaseAsync _db;

    public CmsCommandsAsync(IDatabaseAsync db)
    {
        _db = db;
    }

    private RespCountMinSketch Group
    {
        get
        {
            _db.SetLibraryInfoOnce();
            return _db.CountMinSketch;
        }
    }

    /// <inheritdoc/>
    public Task<long> IncrByAsync(RedisKey key, RedisValue item, long increment)
        => _db.AsTask(Group.IncrByAsync(key, item, increment));

    /// <inheritdoc/>
    public Task<long[]> IncrByAsync(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
        => _db.AsTask(Group.IncrByAsync(key, ToPairs(itemIncrements)));

    /// <inheritdoc/>
    public Task<CmsInformation> InfoAsync(RedisKey key)
        => _db.AsTask(Group.InfoAsync(key));

    /// <inheritdoc/>
    public Task<bool> InitByDimAsync(RedisKey key, long width, long depth)
        => _db.AsTask(True(Group.InitByDimAsync(key, width, depth)));

    /// <inheritdoc/>
    public Task<bool> InitByProbAsync(RedisKey key, double error, double probability)
        => _db.AsTask(True(Group.InitByProbAsync(key, error, probability)));

    /// <inheritdoc/>
    public Task<bool> MergeAsync(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
        => _db.AsTask(True(Group.MergeAsync(ToKey(destination), ToKeys(source, numKeys), weight ?? default(ReadOnlySpan<long>))));

    /// <inheritdoc/>
    public Task<long[]> QueryAsync(RedisKey key, params RedisValue[] items)
        => _db.AsTask(Group.QueryAsync(key, items));

    // the old surface reported "OK" as true, and threw otherwise; the new one just completes or throws
    private static async ValueTask<bool> True(ValueTask pending)
    {
        await pending.ConfigureAwait(false);
        return true;
    }

    internal static (RedisValue Item, long Increment)[] ToPairs(Tuple<RedisValue, long>[] itemIncrements)
    {
        if (itemIncrements is null) throw new ArgumentNullException(nameof(itemIncrements));
        var pairs = new (RedisValue, long)[itemIncrements.Length];
        for (int i = 0; i < pairs.Length; i++)
        {
            pairs[i] = (itemIncrements[i].Item1, itemIncrements[i].Item2);
        }
        return pairs;
    }

    // CMS.MERGE's old signature typed its keys as values, so they never took part in cluster routing or
    // key prefixing; the group types them correctly, and these adapt the old spelling
    internal static RedisKey ToKey(RedisValue value) => (byte[]?)value;

    internal static RedisKey[] ToKeys(RedisValue[] source, long numKeys)
    {
        if (source is null) throw new ArgumentNullException(nameof(source));
        if (numKeys != source.Length) throw new ArgumentOutOfRangeException(nameof(numKeys), "numKeys must match the number of source sketches.");
        var keys = new RedisKey[source.Length];
        for (int i = 0; i < keys.Length; i++)
        {
            keys[i] = ToKey(source[i]);
        }
        return keys;
    }
}
