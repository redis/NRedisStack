using System.Diagnostics.CodeAnalysis;

namespace NRedisStack.Literals.Enums;

/// <summary>
/// Flags controlling the behaviour of the multi-series range commands
/// (<c>TS.MRANGE</c> / <c>TS.MREVRANGE</c>). Consolidates the individual
/// <c>LATEST</c>, <c>WITHLABELS</c>, <c>EMPTY</c> and <c>EXCLUDEEMPTY</c> modifiers.
/// </summary>
[Flags]
public enum TimeSeriesRangeFlags
{
    /// <summary>
    /// No modifiers.
    /// </summary>
    None = 0,

    /// <summary>
    /// <c>LATEST</c>: for a compaction, also report the compacted value of the latest, possibly partial, bucket.
    /// Ignored for non-compaction series.
    /// </summary>
    Latest = 1 << 0,

    /// <summary>
    /// <c>EMPTY</c>: when aggregating, also report aggregations for empty buckets <em>within</em> each reported
    /// series. Requires an aggregation to be specified. This is the opposite concern to <see cref="ExcludeEmpty"/>,
    /// which acts at the whole-series level.
    /// </summary>
    Empty = 1 << 1,

    /// <summary>
    /// <c>WITHLABELS</c>: include the label-value pairs of each series in the reply. Cannot be combined with
    /// an explicit <c>selectLabels</c> collection.
    /// </summary>
    WithLabels = 1 << 2,

    /// <summary>
    /// <c>EXCLUDEEMPTY</c>: omit an entire matching series from the reply when the queried range and options
    /// produce no reported samples for that series. This is the opposite concern to <see cref="Empty"/>, which
    /// acts on empty buckets <em>within</em> a reported series.
    /// <para>
    /// Mutually exclusive with grouping (<c>GROUPBY … REDUCE …</c>): the client performs no local validation,
    /// so combining this flag with a group-by results in the server error being propagated unchanged.
    /// </para>
    /// </summary>
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    ExcludeEmpty = 1 << 3,
}
