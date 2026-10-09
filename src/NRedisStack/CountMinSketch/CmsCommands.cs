using NRedisStack.CountMinSketch.DataTypes;
using StackExchange.Redis;

namespace NRedisStack;

/// <summary>
/// The original synchronous Count-Min Sketch surface, served from the <see cref="RespCountMinSketch"/> group.
/// </summary>
/// <remarks>
/// The group is async-only, as StackExchange.Redis's own groups are. The synchronous methods here send through a
/// <b>blocking</b> context (<c>db.Context.Blocking()</c>): every send then completes on the calling thread and the
/// <see cref="ValueTask"/> comes back already completed, so <c>GetAwaiter().GetResult()</c> is a read, not a wait.
/// This is how the synchronous <see cref="IDatabase"/> methods are implemented too.
/// </remarks>
public class CmsCommands : CmsCommandsAsync, ICmsCommands
{
    private readonly IDatabase _db;
    private readonly RespCountMinSketch _blocking;

    public CmsCommands(IDatabase db) : base(db)
    {
        _db = db;
        // once, not per call: a context is immutable and the database's does not change, and Blocking()
        // allocates a new one each time it is asked - the same reason RedisDatabase keeps its own
        _blocking = db.Context.Blocking().CountMinSketch;
    }

    private RespCountMinSketch Group
    {
        get
        {
            _db.SetLibraryInfoOnce();
            return _blocking;
        }
    }

    /// <inheritdoc/>
    public long IncrBy(RedisKey key, RedisValue item, long increment)
        => Group.IncrByAsync(key, item, increment).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public long[] IncrBy(RedisKey key, Tuple<RedisValue, long>[] itemIncrements)
        => Group.IncrByAsync(key, ToPairs(itemIncrements)).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public CmsInformation Info(RedisKey key)
        => Group.InfoAsync(key).GetAwaiter().GetResult();

    /// <inheritdoc/>
    public bool InitByDim(RedisKey key, long width, long depth)
        => True(Group.InitByDimAsync(key, width, depth));

    /// <inheritdoc/>
    public bool InitByProb(RedisKey key, double error, double probability)
        => True(Group.InitByProbAsync(key, error, probability));

    /// <inheritdoc/>
    public bool Merge(RedisValue destination, long numKeys, RedisValue[] source, long[]? weight = null)
        => True(Group.MergeAsync(ToKey(destination), ToKeys(source, numKeys), weight ?? default(ReadOnlySpan<long>)));

    /// <inheritdoc/>
    public long[] Query(RedisKey key, params RedisValue[] items)
        => Group.QueryAsync(key, items).GetAwaiter().GetResult();

    // the old surface reported "OK" as true, and threw otherwise; the new one just completes or throws
    private static bool True(ValueTask completed)
    {
        completed.GetAwaiter().GetResult();
        return true;
    }
}
