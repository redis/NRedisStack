using StackExchange.Redis;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
namespace NRedisStack;

public class TimeSeriesCommands : TimeSeriesCommandsAsync, ITimeSeriesCommands
{
    readonly IDatabase _db;
    public TimeSeriesCommands(IDatabase db) : base(db)
    {
        _db = db;
    }

    #region Create

    /// <inheritdoc/>
    [Obsolete("Please use the other method with TsCreateParams and check related builder TsCreateParamsBuilder to build parameters.")]
    public bool Create(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.Create(key, retentionTime, labels,
            uncompressed, chunkSizeBytes,
            duplicatePolicy)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Create(string key, TsCreateParams parameters) => _db.Execute(TimeSeriesCommandsBuilder.Create(key, parameters)).OKtoBoolean();

    #endregion

    #region Update

    /// <inheritdoc/>
    [Obsolete("Please use the other method with TsAlterParams and check related builder TsAlterParamsBuilder to build parameters.")]
    public bool Alter(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.Alter(key, retentionTime, chunkSizeBytes, duplicatePolicy, labels)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Alter(string key, TsAlterParams parameters) => _db.Execute(TimeSeriesCommandsBuilder.Alter(key, parameters)).OKtoBoolean();

    /// <inheritdoc/>
    [Obsolete("Please use the other method with TsAddParams and check related builder TsAddParamsBuilder to build parameters.")]
    public TimeStamp Add(string key, TimeStamp timestamp, double value, long? retentionTime = null,
        IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null,
        long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.Add(key, timestamp, value, retentionTime, labels,
            uncompressed, chunkSizeBytes, duplicatePolicy)).ToTimeStamp();
    }

    /// <inheritdoc/>
    public TimeStamp Add(string key, TsAddParams parameters) => _db.Execute(TimeSeriesCommandsBuilder.Add(key, parameters)).ToTimeStamp();

    /// <inheritdoc/>
    public IReadOnlyList<TimeStamp> MAdd(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.MAdd(sequence)).ToTimeStampArray();
    }

    /// <inheritdoc/>
    [Obsolete("Please use the other method with TsIncrByParams and check related builder TsIncryByParamsBuilder to build parameters.")]
    public TimeStamp IncrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.IncrBy(key, value, timestamp, retentionTime,
            labels, uncompressed, chunkSizeBytes)).ToTimeStamp();
    }

    /// <inheritdoc/>
    public TimeStamp IncrBy(string key, TsIncrByParams parameters) => _db.Execute(TimeSeriesCommandsBuilder.IncrBy(key, parameters)).ToTimeStamp();

    /// <inheritdoc/>
    [Obsolete("Please use the other method with TsDecrByParams and check related builder TsDecryByParamsBuilder to build parameters.")]
    public TimeStamp DecrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.DecrBy(key, value, timestamp, retentionTime,
            labels, uncompressed, chunkSizeBytes)).ToTimeStamp();
    }

    /// <inheritdoc/>
    public TimeStamp DecrBy(string key, TsDecrByParams parameters) => _db.Execute(TimeSeriesCommandsBuilder.DecrBy(key, parameters)).ToTimeStamp();

    /// <inheritdoc/>
    public long Del(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.Del(key, fromTimeStamp, toTimeStamp)).ToLong();
    }

    #endregion

    #region Aggregation, Compaction, Downsampling

    /// <inheritdoc/>
    public bool CreateRule(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.CreateRule(sourceKey, rule, alignTimestamp)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool DeleteRule(string sourceKey, string destKey)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.DeleteRule(sourceKey, destKey)).OKtoBoolean();
    }

    #endregion

    #region Query

    /// <inheritdoc/>
    public TimeSeriesTuple? Get(string key, bool latest = false)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.Get(key, latest)).ToTimeSeriesTuple();
    }

    /// <inheritdoc/>
    public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> MGet(IReadOnlyCollection<string> filter, bool latest = false,
        bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.MGet(filter, latest, withLabels, selectedLabels)).ParseMGetResponse();
    }

    /// <inheritdoc/>
    [OverloadResolutionPriority(1)]
    public IReadOnlyList<TimeSeriesTuple> Range(string key,
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
        return _db.Execute(TimeSeriesCommandsBuilder.Range(key, fromTimeStamp, toTimeStamp,
            latest, filterByTs, filterByValue,
            count, align, aggregation, timeBucket,
            bt, empty)).ToTimeSeriesTupleArray();
    }

    /// <inheritdoc/>
    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public IReadOnlyList<TimeSeriesTuple> Range(string key,
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
        bool empty = false)
    {
        return Range(key, fromTimeStamp, toTimeStamp, latest, filterByTs, filterByValue, count, align, (TsAggregations)aggregation, timeBucket, bt, empty);
    }

    /// <inheritdoc/>
    [OverloadResolutionPriority(1)]
    public IReadOnlyList<TimeSeriesTuple> RevRange(string key,
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
        return _db.Execute(TimeSeriesCommandsBuilder.RevRange(key, fromTimeStamp, toTimeStamp,
            latest, filterByTs, filterByValue,
            count, align, aggregation, timeBucket,
            bt, empty)).ToTimeSeriesTupleArray();
    }

    /// <inheritdoc/>
    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public IReadOnlyList<TimeSeriesTuple> RevRange(string key,
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
        bool empty = false)
    {
        return RevRange(key, fromTimeStamp, toTimeStamp, latest, filterByTs, filterByValue, count, align, (TsAggregations)aggregation, timeBucket, bt, empty);
    }

    /// <inheritdoc/>
    [OverloadResolutionPriority(2)]
    public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRange(
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
        return _db.Execute(TimeSeriesCommandsBuilder.MRange(fromTimeStamp, toTimeStamp, filter,
            flags, filterByTs, filterByValue,
            selectLabels, count, align,
            aggregation, timeBucket, bt,
            groupbyTuple)).ParseMRangeResponse();
    }

    /// <inheritdoc/>
    [OverloadResolutionPriority(1)]
    public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRange(
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
        return _db.Execute(TimeSeriesCommandsBuilder.MRange(fromTimeStamp, toTimeStamp, filter,
            latest, filterByTs, filterByValue,
            withLabels, selectLabels, count, align,
            aggregation, timeBucket, bt, empty,
            groupbyTuple)).ParseMRangeResponse();
    }

    /// <inheritdoc/>
    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRange(
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
        (string, TsReduce)? groupbyTuple = null)
    {
        return MRange(fromTimeStamp, toTimeStamp, filter, latest, filterByTs, filterByValue, withLabels, selectLabels, count, align, (TsAggregations)aggregation, timeBucket, bt, empty, groupbyTuple);
    }

    /// <inheritdoc/>
    [OverloadResolutionPriority(2)]
    public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRevRange(
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
        return _db.Execute(TimeSeriesCommandsBuilder.MRevRange(fromTimeStamp, toTimeStamp, filter,
            flags, filterByTs, filterByValue,
            selectLabels, count, align,
            aggregation, timeBucket, bt,
            groupbyTuple)).ParseMRangeResponse();
    }

    /// <inheritdoc/>
    [OverloadResolutionPriority(1)]
    public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRevRange(
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
        return _db.Execute(TimeSeriesCommandsBuilder.MRevRange(fromTimeStamp, toTimeStamp, filter,
            latest, filterByTs, filterByValue,
            withLabels, selectLabels, count, align,
            aggregation, timeBucket, bt, empty,
            groupbyTuple)).ParseMRangeResponse();
    }

    /// <inheritdoc/>
    [Obsolete]
    [Browsable(false)]
    [EditorBrowsable(EditorBrowsableState.Never)]
    [OverloadResolutionPriority(-1)]
    public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRevRange(
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
        (string, TsReduce)? groupbyTuple = null)
    {
        return MRevRange(fromTimeStamp, toTimeStamp, filter, latest, filterByTs, filterByValue, withLabels, selectLabels, count, align, (TsAggregations)aggregation, timeBucket, bt, empty, groupbyTuple);
    }

    #endregion

    #region General

    /// <inheritdoc/>
    [Obsolete]
    public TimeSeriesInformation Info(string key, bool debug = false)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.Info(key, debug)).ToTimeSeriesInfo();
    }

    /// <inheritdoc/>
    public IReadOnlyList<string> QueryIndex(IReadOnlyCollection<string> filter)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.QueryIndex(filter)).ToStringList();
    }

    /// <inheritdoc/>
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public IReadOnlyList<string> QueryLabelNames(IReadOnlyCollection<string>? filter = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.QueryLabelNames(filter)).ToStringList();
    }

    /// <inheritdoc/>
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public IReadOnlyList<string> QueryLabelValues(string label, IReadOnlyCollection<string>? filter = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.QueryLabelValues(label, filter)).ToStringList();
    }

    /// <inheritdoc/>
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public IReadOnlyList<TimeSeriesPivotRow> NRange(
        IReadOnlyList<string> keys,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        TimeSeriesRangeFlags flags = TimeSeriesRangeFlags.None,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        IReadOnlyList<TsAggregations>? aggregations = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.NRange(keys, fromTimeStamp, toTimeStamp, flags, filterByTs,
            filterByValue, count, align, aggregations, timeBucket, bt)).ToTimeSeriesPivotRowArray();
    }

    /// <inheritdoc/>
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public IReadOnlyList<TimeSeriesPivotRow> NRevRange(
        IReadOnlyList<string> keys,
        TimeStamp fromTimeStamp,
        TimeStamp toTimeStamp,
        TimeSeriesRangeFlags flags = TimeSeriesRangeFlags.None,
        IReadOnlyCollection<TimeStamp>? filterByTs = null,
        (long, long)? filterByValue = null,
        long? count = null,
        TimeStamp? align = null,
        IReadOnlyList<TsAggregations>? aggregations = null,
        long? timeBucket = null,
        TsBucketTimestamps? bt = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.NRevRange(keys, fromTimeStamp, toTimeStamp, flags, filterByTs,
            filterByValue, count, align, aggregations, timeBucket, bt)).ToTimeSeriesPivotRowArray();
    }

    /// <inheritdoc/>
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public IReadOnlyList<TimeSeriesTuple> Read(string key, TimeStamp timestamp, long? maxCount = null)
    {
        return _db.Execute(TimeSeriesCommandsBuilder.Read(key, timestamp, maxCount)).ToTimeSeriesTupleArray();
    }

    /// <inheritdoc/>
    [Experimental(Experiments.Server_8_10, UrlFormat = Experiments.UrlFormat)]
    public IEnumerable<TimeSeriesTuple> ReadEnumerable(string key, TimeStamp fromTimeStamp, long? batchSize = null)
    {
        TimeStamp cursor = fromTimeStamp;
        while (true)
        {
            var batch = Read(key, cursor, batchSize);
            if (batch.Count == 0) yield break;
            foreach (var tuple in batch) yield return tuple;
            // stop when the page is not full (drained); otherwise advance the cursor past the last sample.
            if (batchSize is not { } size || batch.Count < size) yield break;
            long last = batch[batch.Count - 1].Time;
            cursor = last + 1;
        }
    }

    #endregion
}
