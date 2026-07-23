using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;

namespace NRedisStack;

public static class TimeSeriesCommandsBuilder
{
    #region Create

    [Obsolete()]
    public static SerializedCommand Create(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
    {
        var parameters = new TsCreateParams(retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
        return new(TS.CREATE, parameters.ToArray(key));
    }

    public static SerializedCommand Create(string key, TsCreateParams parameters)
    {
        return new(TS.CREATE, parameters.ToArray(key));
    }

    #endregion

    #region Update
    [Obsolete()]
    public static SerializedCommand Alter(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
    {
        var parameters = new TsAlterParams(retentionTime, chunkSizeBytes, duplicatePolicy, labels);
        return new(TS.ALTER, parameters.ToArray(key));
    }

    public static SerializedCommand Alter(string key, TsAlterParams parameters)
    {
        return new(TS.ALTER, parameters.ToArray(key));
    }

    [Obsolete()]
    public static SerializedCommand Add(string key, TimeStamp timestamp, double value, long? retentionTime = null,
        IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null,
        long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
    {
        var parameters = new TsAddParams(timestamp, value, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
        return new(TS.ADD, parameters.ToArray(key));
    }

    public static SerializedCommand Add(string key, TsAddParams parameters)
    {
        return new(TS.ADD, parameters.ToArray(key));
    }

    public static SerializedCommand MAdd(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
    {
        var args = TimeSeriesAux.BuildTsMaddArgs(sequence);
        return new(TS.MADD, args);
    }

    [Obsolete()]
    public static SerializedCommand IncrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
    {
        var parameters = new TsIncrByParams(value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
        return new(TS.INCRBY, parameters.ToArray(key));
    }

    public static SerializedCommand IncrBy(string key, TsIncrByParams parameters)
    {
        return new(TS.INCRBY, parameters.ToArray(key));
    }

    [Obsolete()]
    public static SerializedCommand DecrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
    {
        var parameters = new TsDecrByParams(value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
        return new(TS.DECRBY, parameters.ToArray(key));
    }

    public static SerializedCommand DecrBy(string key, TsDecrByParams parameters)
    {
        return new(TS.DECRBY, parameters.ToArray(key));
    }

    public static SerializedCommand Del(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
    {
        var args = TimeSeriesAux.BuildTsDelArgs(key, fromTimeStamp, toTimeStamp);
        return new(TS.DEL, args);
    }

    #endregion

    #region Aggregation, Compaction, Downsampling

    public static SerializedCommand CreateRule(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0)
    {
        var args = new List<object> { sourceKey };
        args.AddRule(rule);
        args.Add(alignTimestamp);
        return new(TS.CREATERULE, args);
    }

    public static SerializedCommand DeleteRule(string sourceKey, string destKey)
    {
        var args = new List<object> { sourceKey, destKey };
        return new(TS.DELETERULE, args);
    }

    #endregion

    #region Query

    public static SerializedCommand Get(string key, bool latest = false)
    {
        return (latest) ? new(TS.GET, key, TimeSeriesArgs.LATEST)
            : new SerializedCommand(TS.GET, key);
    }

    public static SerializedCommand MGet(IReadOnlyCollection<string> filter, bool latest = false,
        bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
    {
        var args = TimeSeriesAux.BuildTsMgetArgs(latest, filter, withLabels, selectedLabels);
        return new(TS.MGET, args);
    }

    [OverloadResolutionPriority(1)]
    public static SerializedCommand Range(string key,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregations aggregation = default,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false)
    {
        var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
            latest, filterByTs, filterByValue, count, align,
            aggregation, timeBucket, bt, empty);

        return new(TS.RANGE, args);
    }

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static SerializedCommand Range(string key,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregation? aggregation = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false) =>
        Range(key, fromTimeStamp, toTimeStamp, latest, filterByTs, filterByValue, count, align, (TsAggregations)aggregation, timeBucket, bt, empty);

    [OverloadResolutionPriority(1)]
    public static SerializedCommand RevRange(string key,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregations aggregation = default,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false)
    {
        var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
            latest, filterByTs, filterByValue, count, align,
            aggregation, timeBucket, bt, empty);

        return new(TS.REVRANGE, args);
    }

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static SerializedCommand RevRange(string key,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregation? aggregation = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false) =>
        RevRange(key, fromTimeStamp, toTimeStamp, latest, filterByTs, filterByValue, count, align, (TsAggregations)aggregation, timeBucket, bt, empty);

    [OverloadResolutionPriority(2)]
    public static SerializedCommand MRange(
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        TimeSeriesRangeFlags flags = TimeSeriesRangeFlags.None,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        IReadOnlyCollection<string>? selectLabels = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregations aggregation = default,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        (string, TsReduce)? groupbyTuple = null)
    {
        var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, flags, filterByTs,
            filterByValue, selectLabels, count, align, aggregation, timeBucket, bt, groupbyTuple);
        return new(TS.MRANGE, args);
    }

    [OverloadResolutionPriority(1)]
    public static SerializedCommand MRange(
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        bool? withLabels = null,
        IReadOnlyCollection<string>? selectLabels = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregations aggregation = default,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false,
        (string, TsReduce)? groupbyTuple = null)
    {
        return MRange(fromTimeStamp, toTimeStamp, filter, TimeSeriesAux.ToRangeFlags(latest, withLabels, empty),
            filterByTs, filterByValue, selectLabels, count, align, aggregation, timeBucket, bt, groupbyTuple);
    }

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static SerializedCommand MRange(
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        bool? withLabels = null,
        IReadOnlyCollection<string>? selectLabels = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregation? aggregation = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false,
        (string, TsReduce)? groupbyTuple = null) =>
        MRange(fromTimeStamp, toTimeStamp, filter, latest, filterByTs, filterByValue, withLabels, selectLabels, count, align, (TsAggregations)aggregation, timeBucket, bt, empty, groupbyTuple);

    [OverloadResolutionPriority(2)]
    public static SerializedCommand MRevRange(
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        TimeSeriesRangeFlags flags = TimeSeriesRangeFlags.None,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        IReadOnlyCollection<string>? selectLabels = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregations aggregation = default,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        (string, TsReduce)? groupbyTuple = null)
    {
        var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, flags, filterByTs,
            filterByValue, selectLabels, count, align, aggregation, timeBucket, bt, groupbyTuple);
        return new(TS.MREVRANGE, args);
    }

    [OverloadResolutionPriority(1)]
    public static SerializedCommand MRevRange(
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        bool? withLabels = null,
        IReadOnlyCollection<string>? selectLabels = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregations aggregation = default,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false,
        (string, TsReduce)? groupbyTuple = null)
    {
        return MRevRange(fromTimeStamp, toTimeStamp, filter, TimeSeriesAux.ToRangeFlags(latest, withLabels, empty),
            filterByTs, filterByValue, selectLabels, count, align, aggregation, timeBucket, bt, groupbyTuple);
    }

    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public static SerializedCommand MRevRange(
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        IReadOnlyCollection<string> filter,
        bool latest = false,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        bool? withLabels = null,
        IReadOnlyCollection<string>? selectLabels = null,
        long? count = null,
        TimeStamp? align = null,
        TsAggregation? aggregation = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null,
        bool empty = false,
        (string, TsReduce)? groupbyTuple = null) =>
        MRevRange(fromTimeStamp, toTimeStamp, filter, latest, filterByTs, filterByValue, withLabels, selectLabels, count, align, (TsAggregations)aggregation, timeBucket, bt, empty, groupbyTuple);

    #endregion

    #region General

    public static SerializedCommand Info(string key, bool debug = false)
    {
        return (debug) ? new(TS.INFO, key, TimeSeriesArgs.DEBUG)
            : new SerializedCommand(TS.INFO, key);
    }

    public static SerializedCommand QueryIndex(IReadOnlyCollection<string> filter)
    {
        var args = new List<object>(filter);
        return new(TS.QUERYINDEX, args);
    }

    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public static SerializedCommand QueryLabelNames(IReadOnlyCollection<string>? filter = null)
    {
        var args = new List<object> { TimeSeriesArgs.LABELS };
        AddQueryLabelsFilter(args, filter);
        return new(TS.QUERYLABELS, args);
    }

    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public static SerializedCommand QueryLabelValues(string label, IReadOnlyCollection<string>? filter = null)
    {
        var args = new List<object> { TimeSeriesArgs.VALUES, label };
        AddQueryLabelsFilter(args, filter);
        return new(TS.QUERYLABELS, args);
    }

    // FILTER is optional for TS.QUERYLABELS (omitting it queries all indexed series); an empty collection is
    // treated the same as "no filter" so we never emit a bare FILTER keyword (which the server rejects).
    private static void AddQueryLabelsFilter(List<object> args, IReadOnlyCollection<string>? filter)
    {
        if (filter is { Count: > 0 })
        {
            args.Add(TimeSeriesArgs.FILTER);
            foreach (var f in filter) args.Add(f);
        }
    }

    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public static SerializedCommand NRange(
        IReadOnlyList<string> keys,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        TimeSeriesRangeFlags flags = TimeSeriesRangeFlags.None,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        IReadOnlyList<TsAggregation>? aggregations = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null)
    {
        var args = TimeSeriesAux.BuildNRangeArgs(keys, fromTimeStamp, toTimeStamp, flags, filterByTs,
            filterByValue, count, align, aggregations, timeBucket, bt);
        return new(TS.NRANGE, args);
    }

    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public static SerializedCommand NRevRange(
        IReadOnlyList<string> keys,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        TimeSeriesRangeFlags flags = TimeSeriesRangeFlags.None,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        IReadOnlyList<TsAggregation>? aggregations = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null)
    {
        var args = TimeSeriesAux.BuildNRangeArgs(keys, fromTimeStamp, toTimeStamp, flags, filterByTs,
            filterByValue, count, align, aggregations, timeBucket, bt);
        return new(TS.NREVRANGE, args);
    }

    // Note: the server's BLOCK group is intentionally not exposed - blocking does not compose with the
    // SE.Redis multiplexer - so this only ever builds the immediate-return form.
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public static SerializedCommand Read(string key, TimeStamp timestamp, long? maxCount = null)
    {
        var args = new List<object> { key, timestamp.Value };
        if (maxCount.HasValue)
        {
            args.Add(TimeSeriesArgs.MAX_COUNT);
            args.Add(maxCount.Value);
        }
        return new(TS.READ, args);
    }

    #endregion
}
