using NRedisStack.CountMinSketch.DataTypes;
using NRedisStack.RedisStackCommands;
using RESPite.Messages;
using StackExchange.Redis;
using StackExchange.Redis.Protocol;

namespace NRedisStack;

/// <summary>
/// The Count-Min Sketch commands, as extension methods on <see cref="RespCountMinSketch"/>.
/// </summary>
/// <remarks>
/// <para>
/// Each command is written as an interpolated string over the group's context. The hole <b>type</b> decides
/// whether an argument is a key: <see cref="RedisKey"/> holes are routed, prefixed and tracked; everything
/// else is a value. Commands with a variable tail (<c>CMS.MERGE ... WEIGHTS</c>) start from
/// <c>Compose</c> and append.
/// </para>
/// <para>
/// Every command declares its retry category via <c>WithRetryCategory</c>, which is caller-wins: an explicit
/// category in the <c>flags</c> argument is respected, otherwise the command's own applies. The read-only
/// category is also what makes <c>CMS.QUERY</c> and <c>CMS.INFO</c> eligible for client-side caching; a
/// <c>CMS.INCRBY</c> invalidates them, because the module marks its write as a key modification.
/// </para>
/// <para>
/// Multi-value replies come back as a <see cref="ReadOnlyLease{T}"/> over a pooled buffer, as the
/// StackExchange.Redis groups return theirs: <b>dispose it</b>. An empty input yields an empty lease without a
/// round trip.
/// </para>
/// </remarks>
public static partial class CountMinSketchCommands
{
    // rendered once, not per call: the server does not know these, so without `preform` each call would
    // re-encode the name
    private static readonly RespCommand
        InitByDim = "CMS.INITBYDIM".Command(preform: true),
        InitByProb = "CMS.INITBYPROB".Command(preform: true),
        IncrBy = "CMS.INCRBY".Command(preform: true),
        Query = "CMS.QUERY".Command(preform: true),
        Merge = "CMS.MERGE".Command(preform: true),
        Info = "CMS.INFO".Command(preform: true);

    /// <summary>The <c>WEIGHTS</c> operand of <c>CMS.MERGE</c>.</summary>
    [Resp]
    private static partial RespFragment Weights { get; }

    /// <summary>
    /// Initializes a Count-Min Sketch to dimensions specified by user.
    /// </summary>
    /// <param name="cms">The command group.</param>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="width">Number of counters in each array. Reduces the error size.</param>
    /// <param name="depth">Number of counter-arrays. Reduces the probability for an error
    /// of a certain size (percentage of total count).</param>
    /// <param name="flags">Command flags.</param>
    /// <param name="cancellationToken">Cancels the request: one not yet written is never sent; one already written still runs on the server, and its reply is discarded.</param>
    /// <remarks><seealso href="https://redis.io/commands/cms.initbydim"/></remarks>
    public static ValueTask InitByDimAsync(this RespCountMinSketch cms, RedisKey key, long width, long depth,
        CommandFlags flags = CommandFlags.None, CancellationToken cancellationToken = default)
        => cms.Context.SendAsync($"{InitByDim}{key}{width}{depth}",
            flags.WithRetryCategory(CommandCategories.WriteAccumulating), cancellationToken);

    /// <summary>
    /// Initializes a Count-Min Sketch to accommodate requested tolerances.
    /// </summary>
    /// <param name="cms">The command group.</param>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="error">Estimate size of error.</param>
    /// <param name="probability">The desired probability for inflated count.</param>
    /// <param name="flags">Command flags.</param>
    /// <param name="cancellationToken">Cancels the request: one not yet written is never sent; one already written still runs on the server, and its reply is discarded.</param>
    /// <remarks><seealso href="https://redis.io/commands/cms.initbyprob"/></remarks>
    public static ValueTask InitByProbAsync(this RespCountMinSketch cms, RedisKey key, double error, double probability,
        CommandFlags flags = CommandFlags.None, CancellationToken cancellationToken = default)
        => cms.Context.SendAsync($"{InitByProb}{key}{error}{probability}",
            flags.WithRetryCategory(CommandCategories.WriteAccumulating), cancellationToken);

    /// <summary>
    /// Increases the count of an item by an increment.
    /// </summary>
    /// <param name="cms">The command group.</param>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="item">The item whose counter is to be increased.</param>
    /// <param name="increment">Amount by which the item counter is to be increased.</param>
    /// <param name="flags">Command flags.</param>
    /// <param name="cancellationToken">Cancels the request: one not yet written is never sent; one already written still runs on the server, and its reply is discarded.</param>
    /// <returns>The count of the item after the increment.</returns>
    /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
    public static ValueTask<long> IncrByAsync(this RespCountMinSketch cms, RedisKey key, RedisValue item, long increment,
        CommandFlags flags = CommandFlags.None, CancellationToken cancellationToken = default)
        => cms.Context.SendAsync($"{IncrBy}{key}{item}{increment}",
            flags.WithRetryCategory(CommandCategories.WriteAccumulating), SingletonInt64Handler.Instance, cancellationToken);

    /// <summary>
    /// Increases the counts of several items, each by its own increment.
    /// </summary>
    /// <param name="cms">The command group.</param>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="increments">The items and the amounts by which to increase them.</param>
    /// <param name="flags">Command flags.</param>
    /// <param name="cancellationToken">Cancels the request: one not yet written is never sent; one already written still runs on the server, and its reply is discarded.</param>
    /// <returns>The count of each item after its increment, in the order given; dispose it. Empty when no items were given, without a round trip.</returns>
    /// <remarks><seealso href="https://redis.io/commands/cms.incrby"/></remarks>
    public static ValueTask<ReadOnlyLease<long>> IncrByAsync(this RespCountMinSketch cms, RedisKey key,
        ReadOnlySpan<(RedisValue Item, long Increment)> increments,
        CommandFlags flags = CommandFlags.None, CancellationToken cancellationToken = default)
    {
        if (increments.IsEmpty) return new(ReadOnlyLease<long>.Empty);

        // a pair hole does not exist for (value, number), so this is the cumulative form: start the
        // command, then append the pairs
        var cmd = cms.Context.Compose($"{IncrBy}{key}");
        try
        {
            foreach (var (item, increment) in increments)
            {
                cmd.AppendFormatted(item);
                cmd.AppendFormatted(increment);
            }
        }
        catch
        {
            cmd.Dispose();
            throw;
        }

        return cms.Context.SendAsync<ReadOnlyLease<long>>(ref cmd,
            flags.WithRetryCategory(CommandCategories.WriteAccumulating), cancellationToken: cancellationToken);
    }

    /// <summary>
    /// Returns the count for one or more items in a sketch.
    /// </summary>
    /// <param name="cms">The command group.</param>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="items">The items for which to return the count.</param>
    /// <param name="flags">Command flags.</param>
    /// <param name="cancellationToken">Cancels the request: one not yet written is never sent; one already written still runs on the server, and its reply is discarded.</param>
    /// <returns>The min-count of each of the items in the sketch, in the order given; dispose it. Empty when no items were given, without a round trip.</returns>
    /// <remarks><seealso href="https://redis.io/commands/cms.query"/></remarks>
    public static ValueTask<ReadOnlyLease<long>> QueryAsync(this RespCountMinSketch cms, RedisKey key, ReadOnlySpan<RedisValue> items,
        CommandFlags flags = CommandFlags.None, CancellationToken cancellationToken = default)
        => items.IsEmpty
            ? new(ReadOnlyLease<long>.Empty)
            : cms.Context.SendAsync<ReadOnlyLease<long>>($"{Query}{key}{items}",
                flags.WithRetryCategory(CommandCategories.ReadOnly), cancellationToken: cancellationToken);

    /// <summary>
    /// Merges several sketches into one sketch.
    /// </summary>
    /// <param name="cms">The command group.</param>
    /// <param name="destination">The name of the destination sketch; must already be initialized.</param>
    /// <param name="sources">The names of the source sketches to be merged.</param>
    /// <param name="weights">A multiplier per source sketch, or empty for a weight of 1 each.</param>
    /// <param name="flags">Command flags.</param>
    /// <param name="cancellationToken">Cancels the request: one not yet written is never sent; one already written still runs on the server, and its reply is discarded.</param>
    /// <exception cref="ArgumentException">No sources were given, or the weights do not match the sources one for one.</exception>
    /// <remarks>
    /// <seealso href="https://redis.io/commands/cms.merge"/>. Categorized as an accumulating write, which is
    /// never replayed: a replay would give the same result only if no source changed in between, and nothing
    /// here can know that.
    /// </remarks>
    public static ValueTask MergeAsync(this RespCountMinSketch cms, RedisKey destination, ReadOnlySpan<RedisKey> sources,
        ReadOnlySpan<long> weights = default, CommandFlags flags = CommandFlags.None, CancellationToken cancellationToken = default)
    {
        if (sources.IsEmpty) throw new ArgumentException("At least one source sketch is required.", nameof(sources));
        if (!weights.IsEmpty && weights.Length != sources.Length)
            throw new ArgumentException("When specified, there must be one weight per source.", nameof(weights));

        // the fixed part as one interpolation - the keys hole handles the whole span - then the optional
        // WEIGHTS tail appended; the fragment writes nothing when there are no weights
        var cmd = cms.Context.Compose($"{Merge}{destination}{sources.Length}{sources}{Weights.When(!weights.IsEmpty)}");
        try
        {
            foreach (var weight in weights)
            {
                cmd.AppendFormatted(weight);
            }
        }
        catch
        {
            cmd.Dispose();
            throw;
        }

        return cms.Context.SendAsync(ref cmd,
            flags.WithRetryCategory(CommandCategories.WriteAccumulating), cancellationToken);
    }

    /// <summary>
    /// Returns information about a sketch.
    /// </summary>
    /// <param name="cms">The command group.</param>
    /// <param name="key">The name of the sketch.</param>
    /// <param name="flags">Command flags.</param>
    /// <param name="cancellationToken">Cancels the request: one not yet written is never sent; one already written still runs on the server, and its reply is discarded.</param>
    /// <remarks><seealso href="https://redis.io/commands/cms.info"/></remarks>
    public static ValueTask<CmsInformation> InfoAsync(this RespCountMinSketch cms, RedisKey key,
        CommandFlags flags = CommandFlags.None, CancellationToken cancellationToken = default)
        => cms.Context.SendAsync($"{Info}{key}",
            flags.WithRetryCategory(CommandCategories.ReadOnly), CmsInformationHandler.Instance, cancellationToken);

    /// <summary>Reads a one-element integer array as the integer: <c>CMS.INCRBY</c> with a single item.</summary>
    private sealed class SingletonInt64Handler : IRespHandler<long>
    {
        public static readonly SingletonInt64Handler Instance = new();

        public long Parse(ref RespReader reader)
        {
            reader.MoveNext();
            return reader.ReadInt64();
        }
    }

    /// <summary>Reads the <c>CMS.INFO</c> reply: label/value pairs, as an array (RESP2) or a map (RESP3).</summary>
    private sealed class CmsInformationHandler : IRespHandler<CmsInformation>
    {
        public static readonly CmsInformationHandler Instance = new();

        public CmsInformation Parse(ref RespReader reader)
        {
            long width = -1, depth = -1, count = -1;
            int cellSize = -1;

            // both shapes enumerate as an interleaved label, value, label, value sequence; the labels
            // are matched by content, so an unknown one is skipped rather than breaking the parse
            var iter = reader.AggregateChildren();
            while (iter.MoveNext())
            {
                var label = iter.Value;
                if (!iter.MoveNext()) break;
                var value = iter.Value;

                if (label.Is("width"u8)) width = value.ReadInt64();
                else if (label.Is("depth"u8)) depth = value.ReadInt64();
                else if (label.Is("count"u8)) count = value.ReadInt64();
                else if (label.Is("cell size"u8)) cellSize = value.ReadInt32();
            }

            return new(width, depth, count, cellSize);
        }
    }
}
