using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using NRedisStack.Extensions;

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

    public static void AddAggregation(this IList<object> args, TimeStamp? align,
        TsAggregation? aggregation,
        long? timeBucket,
        TsBucketTimestamps? bt,
        bool empty)
    {
        if (aggregation == null && (align != null || timeBucket != null || bt != null || empty))
        {
            throw new ArgumentException("align, timeBucket, BucketTimestamps or empty cannot be defined without Aggregation");
        }
        else
        {
            args.AddAlign(align);
            args.AddAggregation(aggregation, timeBucket);
            args.AddBucketTimestamp(bt);
            if (empty) args.Add(TimeSeriesArgs.EMPTY);
        }
    }

    public static void AddAggregation(this IList<object> args, TsAggregation? aggregation, long? timeBucket)
    {
        if (aggregation != null)
        {
            args.Add(TimeSeriesArgs.AGGREGATION);
            args.Add(aggregation.Value.AsArg());
            if (!timeBucket.HasValue)
            {
                throw new ArgumentException("RANGE Aggregation should have timeBucket value");
            }
            args.Add(timeBucket.Value);
        }
    }

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
        if (withLabels.HasValue && selectLabels != null)
        {
            throw new ArgumentException("withLabels and selectLabels cannot be specified together.");
        }

        if (withLabels.HasValue && withLabels.Value)
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
        bool empty)
    {
        var args = new List<object>() { key, fromTimeStamp.Value, toTimeStamp.Value };
        args.AddLatest(latest);
        args.AddFilterByTs(filterByTs);
        args.AddFilterByValue(filterByValue);
        args.AddCount(count);
        args.AddAggregation(align, aggregation, timeBucket, bt, empty);
        return args;
    }


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
        (string, TsReduce)? groupbyTuple)
    {
        var args = new List<object>() { fromTimeStamp.Value, toTimeStamp.Value };
        args.AddLatest(latest);
        args.AddFilterByTs(filterByTs);
        args.AddFilterByValue(filterByValue);
        args.AddWithLabels(withLabels, selectLabels);
        args.AddCount(count);
        args.AddAggregation(align, aggregation, timeBucket, bt, empty);
        args.AddFilters(filter);
        args.AddGroupby(groupbyTuple);
        return args;
    }
}