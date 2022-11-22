using StackExchange.Redis;
using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
namespace NRedisStack
{
    public class TimeSeriesCommands : ITimeSeriesCommands
    {
        IDatabase _db;
        public TimeSeriesCommands(IDatabase db)
        {
            _db = db;
        }

        #region Create

        /// <inheritdoc/>
        public bool Create(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.Create(key, retentionTime, labels,
                                                                uncompressed, chunkSizeBytes,
                                                                duplicatePolicy)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> CreateAsync(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.Create(key, retentionTime, labels,
                                                                           uncompressed, chunkSizeBytes,
                                                                           duplicatePolicy))).OKtoBoolean();
        }

        #endregion

        #region Update

        /// <inheritdoc/>
        public bool Alter(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.Alter(key, retentionTime, chunkSizeBytes, duplicatePolicy, labels)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> AlterAsync(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.Alter(key, retentionTime, chunkSizeBytes, duplicatePolicy, labels))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public TimeStamp Add(string key, TimeStamp timestamp, double value, long? retentionTime = null,
        IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null,
        long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.Add(key, timestamp, value, retentionTime, labels,
                                                            uncompressed, chunkSizeBytes, duplicatePolicy)).ToTimeStamp();
        }

        /// <inheritdoc/>
        public async Task<TimeStamp> AddAsync(string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.Add(key, timestamp, value, retentionTime, labels,
                                                                        uncompressed, chunkSizeBytes, duplicatePolicy))).ToTimeStamp();
        }

        /// <inheritdoc/>
        public IReadOnlyList<TimeStamp> MAdd(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.MAdd(sequence)).ToTimeStampArray();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<TimeStamp>> MAddAsync(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.MAdd(sequence))).ToTimeStampArray();
        }

        /// <inheritdoc/>
        public TimeStamp IncrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.IncrBy(key, value, timestamp, retentionTime,
                                                                 labels, uncompressed, chunkSizeBytes)).ToTimeStamp();
        }

        /// <inheritdoc/>
        public async Task<TimeStamp> IncrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.IncrBy(key, value, timestamp, retentionTime,
                                                                            labels, uncompressed, chunkSizeBytes))).ToTimeStamp();
        }

        /// <inheritdoc/>
        public TimeStamp DecrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.DecrBy(key, value, timestamp, retentionTime,
                                                                 labels, uncompressed, chunkSizeBytes)).ToTimeStamp();
        }

        /// <inheritdoc/>
        public async Task<TimeStamp> DecrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.DecrBy(key, value, timestamp, retentionTime,
                                                                            labels, uncompressed, chunkSizeBytes))).ToTimeStamp();
        }

        /// <inheritdoc/>
        public long Del(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.Del(key, fromTimeStamp, toTimeStamp)).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> DelAsync(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.Del(key, fromTimeStamp, toTimeStamp))).ToLong();
        }

        #endregion

        #region Aggregation, Compaction, Downsampling

        /// <inheritdoc/>
        public bool CreateRule(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.CreateRule(sourceKey, rule, alignTimestamp)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> CreateRuleAsync(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.CreateRule(sourceKey, rule, alignTimestamp))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool DeleteRule(string sourceKey, string destKey)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.DeleteRule(sourceKey, destKey)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteRuleAsync(string sourceKey, string destKey)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.DeleteRule(sourceKey, destKey))).OKtoBoolean();
        }

        #endregion

        #region Query

        /// <inheritdoc/>
        public TimeSeriesTuple? Get(string key, bool latest = false)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.Get(key, latest)).ToTimeSeriesTuple();
        }

        /// <inheritdoc/>
        public async Task<TimeSeriesTuple?> GetAsync(string key, bool latest = false)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.Get(key, latest))).ToTimeSeriesTuple();
        }

        /// <inheritdoc/>
        public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> MGet(IReadOnlyCollection<string> filter, bool latest = false,
                      bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.MGet(filter, latest, withLabels, selectedLabels)).ParseMGetResponse();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)>> MGetAsync(IReadOnlyCollection<string> filter, bool latest = false,
                                       bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.MGet(filter, latest, withLabels, selectedLabels))).ParseMGetResponse();
        }

        /// <inheritdoc/>
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
            return _db.Execute(TimeSeriesCommandsBuilder.Range(key, fromTimeStamp, toTimeStamp,
                                                               latest, filterByTs, filterByValue,
                                                               count, align, aggregation, timeBucket,
                                                               bt, empty)).ToTimeSeriesTupleArray();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<TimeSeriesTuple>> RangeAsync(string key,
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
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.Range(key, fromTimeStamp, toTimeStamp,
                                                                          latest, filterByTs, filterByValue,
                                                                          count, align, aggregation, timeBucket,
                                                                          bt, empty))).ToTimeSeriesTupleArray();
        }

        /// <inheritdoc/>
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
            return _db.Execute(TimeSeriesCommandsBuilder.RevRange(key, fromTimeStamp, toTimeStamp,
                                                                  latest, filterByTs, filterByValue,
                                                                  count, align, aggregation, timeBucket,
                                                                  bt, empty)).ToTimeSeriesTupleArray();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<TimeSeriesTuple>> RevRangeAsync(string key,
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
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.RevRange(key, fromTimeStamp, toTimeStamp,
                                                                             latest, filterByTs, filterByValue,
                                                                             count, align, aggregation, timeBucket,
                                                                             bt, empty))).ToTimeSeriesTupleArray();
        }

        /// <inheritdoc/>
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
            return _db.Execute(TimeSeriesCommandsBuilder.MRange(fromTimeStamp, toTimeStamp, filter,
                                                                latest, filterByTs, filterByValue,
                                                                withLabels, selectLabels, count, align,
                                                                aggregation, timeBucket, bt, empty,
                                                                groupbyTuple)).ParseMRangeResponse();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>> MRangeAsync(
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
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.MRange(fromTimeStamp, toTimeStamp, filter,
                                                                           latest, filterByTs, filterByValue,
                                                                           withLabels, selectLabels, count, align,
                                                                           aggregation, timeBucket, bt, empty,
                                                                           groupbyTuple))).ParseMRangeResponse();
        }

        /// <inheritdoc/>
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
            return _db.Execute(TimeSeriesCommandsBuilder.MRevRange(fromTimeStamp, toTimeStamp, filter,
                                                                   latest, filterByTs, filterByValue,
                                                                   withLabels, selectLabels, count, align,
                                                                   aggregation, timeBucket, bt, empty,
                                                                   groupbyTuple)).ParseMRangeResponse();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>> MRevRangeAsync(
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
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.MRevRange(fromTimeStamp, toTimeStamp, filter,
                                                                              latest, filterByTs, filterByValue,
                                                                              withLabels, selectLabels, count, align,
                                                                              aggregation, timeBucket, bt, empty,
                                                                              groupbyTuple))).ParseMRangeResponse();
        }

        #endregion

        #region General

        /// <inheritdoc/>
        public TimeSeriesInformation Info(string key, bool debug = false)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.Info(key, debug)).ToTimeSeriesInfo();
        }

        /// <inheritdoc/>
        public async Task<TimeSeriesInformation> InfoAsync(string key, bool debug = false)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.Info(key, debug))).ToTimeSeriesInfo();
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> QueryIndex(IReadOnlyCollection<string> filter)
        {
            return _db.Execute(TimeSeriesCommandsBuilder.QueryIndex(filter)).ToStringArray();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> QueryIndexAsync(IReadOnlyCollection<string> filter)
        {
            return (await _db.ExecuteAsync(TimeSeriesCommandsBuilder.QueryIndex(filter))).ToStringArray();
        }

        #endregion
    }
}