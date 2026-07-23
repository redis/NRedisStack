using System.Buffers;
using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using NRedisStack.Extensions;
using StackExchange.Redis;
using System.ComponentModel;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Text;

namespace NRedisStack;

public static class TimeSeriesAux
{
    public static void AddLatest(this IList<object> args, bool latest)
    {
        if (latest) args.Add(TimeSeriesArgs.LATEST);
    }

    public static void AddCount(this IList<object> args, long? count)
    {
        if (count.HasValue)
        {
            args.Add(TimeSeriesArgs.COUNT);
            args.Add(count.Value);
        }
    }

    public static void AddAlign(this IList<object> args, TimeStamp? alignMaybe)
    {
        if (alignMaybe is { } align)
        {
            args.Add(TimeSeriesArgs.ALIGN);
            args.Add(align.Value);
        }
    }

    public static void AddBucketTimestamp(this IList<object> args, TsBucketTimestamps? bt)
    {
        if (bt != null)
        {
            args.Add(TimeSeriesArgs.BUCKETTIMESTAMP);
            args.Add(bt.Value.AsArg());
        }
    }

    [OverloadResolutionPriority(1)]
    public static void AddAggregation(this IList<object> args, TimeStamp? align,
        TsAggregations aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        bool empty)
    {
        if (aggregation.IsEmpty)
        {
            if (align != null || timeBucket != null || bt != null || empty)
            {
                throw new ArgumentException("align, timeBucket, BucketTimestamps or empty cannot be defined without Aggregation");
            }
        }
        else
        {
            args.AddAlign(align);
            args.AddAggregation(aggregation, timeBucket);
            args.AddBucketTimestamp(bt);
            if (empty) args.Add(TimeSeriesArgs.EMPTY);
        }
    }

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static void AddAggregation(this IList<object> args, TimeStamp? align,
        TsAggregation? aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        bool empty) => args.AddAggregation(align, (TsAggregations)aggregation, timeBucket, bt, empty);

    [OverloadResolutionPriority(1)]
    public static void AddAggregation(this IList<object> args, TsAggregations aggregation, long? timeBucket)
    {
        if (!aggregation.IsEmpty)
        {
            args.Add(TimeSeriesArgs.AGGREGATION);
            args.Add(GetAggregationArgs(aggregation));
            if (!timeBucket.HasValue)
            {
                throw new ArgumentException("RANGE Aggregation should have timeBucket value");
            }
            args.Add(timeBucket.Value);
        }
    }

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static void AddAggregation(this IList<object> args, TsAggregation? aggregation, long? timeBucket) =>
        args.AddAggregation((TsAggregations)aggregation, timeBucket);

    public static void AddFilters(this List<object> args, IReadOnlyCollection<string> filter)
    {
        if (filter == null || filter.Count == 0)
        {
            throw new ArgumentException("There should be at least one filter on MRANGE/MREVRANGE");
        }
        args.Add(TimeSeriesArgs.FILTER);
        foreach (string f in filter)
        {
            args.Add(f);
        }
    }

    public static void AddFilterByTs(this List<object> args, IReadOnlyCollection<TimeStamp>? filter)
    {
        if (filter != null)
        {
            args.Add(TimeSeriesArgs.FILTER_BY_TS);
            foreach (var ts in filter)
            {
                args.Add(ts.Value);
            }
        }
    }

    public static void AddFilterByValue(this List<object> args, (long, long)? filter)
    {
        if (filter != null)
        {
            args.Add(TimeSeriesArgs.FILTER_BY_VALUE);
            args.Add(filter.Value.Item1);
            args.Add(filter.Value.Item2);
        }
    }

    public static void AddWithLabels(this IList<object> args, bool? withLabels, IReadOnlyCollection<string>? selectLabels = null)
    {
        // WITHLABELS is only emitted when withLabels == true; when it is false it is a no-op, so only the
        // actively-requested case conflicts with an explicit selectLabels set.
        if (withLabels == true && selectLabels != null)
        {
            throw new ArgumentException("withLabels and selectLabels cannot be specified together.");
        }

        if (withLabels == true)
        {
            args.Add(TimeSeriesArgs.WITHLABELS);
        }

        if (selectLabels != null)
        {
            args.Add(TimeSeriesArgs.SELECTEDLABELS);
            foreach (string label in selectLabels)
            {
                args.Add(label);
            }
        }
    }

    public static void AddGroupby(this IList<object> args, (string groupby, TsReduce reduce)? groupbyTuple)
    {
        if (groupbyTuple.HasValue)
        {
            args.Add(TimeSeriesArgs.GROPUBY);
            args.Add(groupbyTuple.Value.groupby);
            args.Add(TimeSeriesArgs.REDUCE);
            args.Add(groupbyTuple.Value.reduce.AsArg());
        }
    }

    public static void AddTimeStamp(this IList<object> args, TimeStamp timeStamp)
    {
        args.Add(TimeSeriesArgs.TIMESTAMP);
        args.Add(timeStamp.Value);
    }

    public static void AddRule(this IList<object> args, TimeSeriesRule rule)
    {
        args.Add(rule.DestKey);
        args.Add(TimeSeriesArgs.AGGREGATION);
        args.Add(rule.Aggregation.AsArg());
        args.Add(rule.TimeBucket);
    }

    public static List<object> BuildTsDelArgs(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
    {
        var args = new List<object>
            {key, fromTimeStamp.Value, toTimeStamp.Value};
        return args;
    }

    public static List<object> BuildTsMaddArgs(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
    {
        var args = new List<object>();
        foreach (var tuple in sequence)
        {
            args.Add(tuple.key);
            args.Add(tuple.timestamp.Value);
            args.Add(tuple.value);
        }
        return args;
    }

    public static List<object> BuildTsMgetArgs(bool latest, IReadOnlyCollection<string> filter, bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
    {
        var args = new List<object>();
        args.AddLatest(latest);
        args.AddWithLabels(withLabels, selectedLabels);
        args.AddFilters(filter);
        return args;
    }

    [OverloadResolutionPriority(1)]
    public static List<object> BuildRangeArgs(string key,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        bool latest,
        IReadOnlyCollection<TimeStamp>? filterByTs,
        (long, long)? filterByValue,
        long? count,
        TimeStamp? align,
        TsAggregations aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        bool empty)
    {
        var args = new List<object>() { (RedisKey)key, fromTimeStamp.Value, toTimeStamp.Value };
        args.AddLatest(latest);
        args.AddFilterByTs(filterByTs);
        args.AddFilterByValue(filterByValue);
        args.AddCount(count);
        args.AddAggregation(align, aggregation, timeBucket, bt, empty);
        return args;
    }

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static List<object> BuildRangeArgs(string key,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        bool latest,
        IReadOnlyCollection<TimeStamp>? filterByTs,
        (long, long)? filterByValue,
        long? count,
        TimeStamp? align,
        TsAggregation? aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        bool empty) => BuildRangeArgs(key, fromTimeStamp, toTimeStamp, latest, filterByTs, filterByValue, count, align, (TsAggregations)aggregation, timeBucket, bt, empty);

    [OverloadResolutionPriority(2)]
    public static List<object> BuildMultiRangeArgs(TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        TimeSeriesRangeFlags flags,
        IReadOnlyCollection<TimeStamp>? filterByTs,
        (long, long)? filterByValue,
        IReadOnlyCollection<string>? selectLabels,
        long? count,
        TimeStamp? align,
        TsAggregations aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        (string, TsReduce)? groupbyTuple)
    {
        var args = new List<object>() { fromTimeStamp.Value, toTimeStamp.Value };
        args.AddLatest((flags & TimeSeriesRangeFlags.Latest) != 0);
        args.AddFilterByTs(filterByTs);
        args.AddFilterByValue(filterByValue);
        // withLabels is passed as true only when the flag is set, otherwise null (never false) so that
        // supplying selectLabels does not trip the WITHLABELS/SELECTED_LABELS mutual-exclusion check.
        args.AddWithLabels((flags & TimeSeriesRangeFlags.WithLabels) != 0 ? true : null, selectLabels);
        args.AddCount(count);
        args.AddAggregation(align, aggregation, timeBucket, bt, (flags & TimeSeriesRangeFlags.Empty) != 0);
        // EXCLUDEEMPTY must precede FILTER: the FILTER argument list is variadic and would otherwise
        // consume the EXCLUDEEMPTY token as a filter expression.
        if ((flags & TimeSeriesRangeFlags.ExcludeEmpty) != 0) args.Add(TimeSeriesArgs.EXCLUDEEMPTY);
        args.AddFilters(filter);
        args.AddGroupby(groupbyTuple);
        return args;
    }

    internal static TimeSeriesRangeFlags ToRangeFlags(bool latest, bool? withLabels, bool empty)
    {
        var flags = TimeSeriesRangeFlags.None;
        if (latest) flags |= TimeSeriesRangeFlags.Latest;
        if (empty) flags |= TimeSeriesRangeFlags.Empty;
        if (withLabels == true) flags |= TimeSeriesRangeFlags.WithLabels;
        return flags;
    }

    [OverloadResolutionPriority(1)]
    public static List<object> BuildMultiRangeArgs(TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        bool latest,
        IReadOnlyCollection<TimeStamp>? filterByTs,
        (long, long)? filterByValue,
        bool? withLabels,
        IReadOnlyCollection<string>? selectLabels,
        long? count,
        TimeStamp? align,
        TsAggregations aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        bool empty,
        (string, TsReduce)? groupbyTuple) =>
        BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, ToRangeFlags(latest, withLabels, empty),
            filterByTs, filterByValue, selectLabels, count, align, aggregation, timeBucket, bt, groupbyTuple);

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static List<object> BuildMultiRangeArgs(TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        bool latest,
        IReadOnlyCollection<TimeStamp>? filterByTs,
        (long, long)? filterByValue,
        bool? withLabels,
        IReadOnlyCollection<string>? selectLabels,
        long? count,
        TimeStamp? align,
        TsAggregation? aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        bool empty,
        (string, TsReduce)? groupbyTuple) =>
        BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, latest, filterByTs, filterByValue, withLabels, selectLabels, count, align, (TsAggregations)aggregation, timeBucket, bt, empty, groupbyTuple);

    /// <summary>
    /// Builds the argument list shared by <c>TS.NRANGE</c> / <c>TS.NREVRANGE</c>: <c>numkeys key [key ...]
    /// fromTimestamp toTimestamp</c> followed by the optional range modifiers.
    /// </summary>
    /// <remarks>
    /// No client-side validation is performed: the flag/option compatibility rules for these commands are
    /// inconsistent server-side, so arguments are emitted verbatim and the server enforces its own rules and
    /// reports its own errors. The one exception is structural wire order - <c>EMPTY</c> is emitted at the tail
    /// of the <c>AGGREGATION</c> clause because the server rejects it anywhere else - and the per-key aggregator
    /// count is left for the server to validate against numkeys.
    /// </remarks>
    public static List<object> BuildNRangeArgs(
        IReadOnlyList<string> keys,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        TimeSeriesRangeFlags flags,
        IReadOnlyCollection<TimeStamp>? filterByTs,
        (long, long)? filterByValue,
        long? count,
        TimeStamp? align,
        IReadOnlyList<TsAggregations>? aggregations,
        long? timeBucket,
        TsBucketTimestamps? bt)
    {
        var args = new List<object>(keys.Count + 4) { keys.Count };
        foreach (var key in keys) args.Add(key);
        args.Add(fromTimeStamp.Value);
        args.Add(toTimeStamp.Value);

        if ((flags & TimeSeriesRangeFlags.Latest) != 0) args.Add(TimeSeriesArgs.LATEST);
        if ((flags & TimeSeriesRangeFlags.WithLabels) != 0) args.Add(TimeSeriesArgs.WITHLABELS);
        if ((flags & TimeSeriesRangeFlags.ExcludeEmpty) != 0) args.Add(TimeSeriesArgs.EXCLUDEEMPTY);
        args.AddFilterByTs(filterByTs);
        args.AddFilterByValue(filterByValue);
        args.AddCount(count);
        if (aggregations is { Count: > 0 })
        {
            args.AddAlign(align);
            args.Add(TimeSeriesArgs.AGGREGATION);
            // one comma-joined aggregator group per key (group count must equal numkeys, enforced server-side);
            // each group may itself hold multiple aggregators, producing multiple value columns for that key.
            foreach (var group in aggregations) args.Add(GetAggregationArgs(group));
            if (timeBucket.HasValue) args.Add(timeBucket.Value);
            args.AddBucketTimestamp(bt);
            // EMPTY must follow the AGGREGATION clause; the server rejects it in an earlier position.
            if ((flags & TimeSeriesRangeFlags.Empty) != 0) args.Add(TimeSeriesArgs.EMPTY);
        }
        return args;
    }

    private static string GetAggregationArgs(TsAggregations aggregations)
    {
        switch (aggregations.Length)
        {
            case 0: return "";
            case 1: return aggregations[0].AsArg();
            case 2: return $"{aggregations[0].AsArg()},{aggregations[1].AsArg()}";
            case 3: return $"{aggregations[0].AsArg()},{aggregations[1].AsArg()},{aggregations[2].AsArg()}";
            case 4: return $"{aggregations[0].AsArg()},{aggregations[1].AsArg()},{aggregations[2].AsArg()},{aggregations[3].AsArg()}";
            case 5: return $"{aggregations[0].AsArg()},{aggregations[1].AsArg()},{aggregations[2].AsArg()},{aggregations[3].AsArg()},{aggregations[4].AsArg()}";
            default:
                var sb = new StringBuilder(aggregations.Length * (AggregationExtensions.MaxArgLen + 1) - 1); // over-estimate capacity including commas 
                for (int i = 0; i < aggregations.Length; i++)
                {
                    if (i != 0) sb.Append(',');
                    sb.Append(aggregations[i].AsArg());
                }
                return sb.ToString();
        }
    }
}
