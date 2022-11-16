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
            var args = TimeSeriesAux.BuildTsCreateArgs(key, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return _db.Execute(TS.CREATE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> CreateAsync(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsCreateArgs(key, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return (await _db.ExecuteAsync(TS.CREATE, args)).OKtoBoolean();
        }

        #endregion

        #region Update

        /// <inheritdoc/>
        public bool Alter(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
        {
            var args = TimeSeriesAux.BuildTsAlterArgs(key, retentionTime, chunkSizeBytes, duplicatePolicy, labels);
            return _db.Execute(TS.ALTER, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> AlterAsync(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
        {
            var args = TimeSeriesAux.BuildTsAlterArgs(key, retentionTime, chunkSizeBytes, duplicatePolicy, labels);
            return (await _db.ExecuteAsync(TS.ALTER, args)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public TimeStamp Add(string key, TimeStamp timestamp, double value, long? retentionTime = null,
        IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null,
        long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsAddArgs(key, timestamp, value, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return _db.Execute(TS.ADD, args).ToTimeStamp();
        }

        /// <inheritdoc/>
        public async Task<TimeStamp> AddAsync(string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsAddArgs(key, timestamp, value, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return (await _db.ExecuteAsync(TS.ADD, args)).ToTimeStamp();
        }

        /// <inheritdoc/>
        public IReadOnlyList<TimeStamp> MAdd(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = TimeSeriesAux.BuildTsMaddArgs(sequence);
            return _db.Execute(TS.MADD, args).ToTimeStampArray();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<TimeStamp>> MAddAsync(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = TimeSeriesAux.BuildTsMaddArgs(sequence);
            return (await _db.ExecuteAsync(TS.MADD, args)).ToTimeStampArray();
        }

        /// <inheritdoc/>
        public TimeStamp IncrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return _db.Execute(TS.INCRBY, args).ToTimeStamp();
        }

        /// <inheritdoc/>
        public async Task<TimeStamp> IncrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return (await _db.ExecuteAsync(TS.INCRBY, args)).ToTimeStamp();
        }

        /// <inheritdoc/>
        public TimeStamp DecrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return (_db.Execute(TS.DECRBY, args)).ToTimeStamp();
        }

        /// <inheritdoc/>
        public async Task<TimeStamp> DecrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return (await _db.ExecuteAsync(TS.DECRBY, args)).ToTimeStamp();
        }

        /// <inheritdoc/>
        public long Del(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            var args = TimeSeriesAux.BuildTsDelArgs(key, fromTimeStamp, toTimeStamp);
            return _db.Execute(TS.DEL, args).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> DelAsync(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            var args = TimeSeriesAux.BuildTsDelArgs(key, fromTimeStamp, toTimeStamp);
            return (await _db.ExecuteAsync(TS.DEL, args)).ToLong();
        }

        #endregion

        #region Aggregation, Compaction, Downsampling

        /// <inheritdoc/>
        public bool CreateRule(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            args.Add(alignTimestamp);
            return _db.Execute(TS.CREATERULE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> CreateRuleAsync(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            args.Add(alignTimestamp);
            return (await _db.ExecuteAsync(TS.CREATERULE, args)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool DeleteRule(string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return _db.Execute(TS.DELETERULE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> DeleteRuleAsync(string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return (await _db.ExecuteAsync(TS.DELETERULE, args)).OKtoBoolean();
        }

        #endregion

        #region Query

        /// <inheritdoc/>
        public TimeSeriesTuple? Get(string key, bool latest = false)
        {
            var result = (latest) ? _db.Execute(TS.GET, key, TimeSeriesArgs.LATEST)
                                  : _db.Execute(TS.GET, key);
            return result.ToTimeSeriesTuple();
        }

        /// <inheritdoc/>
        public async Task<TimeSeriesTuple?> GetAsync(string key, bool latest = false)
        {
            var result = (await ((latest) ? _db.ExecuteAsync(TS.GET, key, TimeSeriesArgs.LATEST)
                                                                    : _db.ExecuteAsync(TS.GET, key)));
            return result.ToTimeSeriesTuple();
        }

        /// <inheritdoc/>
        public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> MGet(IReadOnlyCollection<string> filter, bool latest = false,
                      bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
        {
            var args = TimeSeriesAux.BuildTsMgetArgs(latest, filter, withLabels, selectedLabels);
            return _db.Execute(TS.MGET, args).ParseMGetResponse();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)>> MGetAsync(IReadOnlyCollection<string> filter, bool latest = false,
                                       bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
        {
            var args = TimeSeriesAux.BuildTsMgetArgs(latest, filter, withLabels, selectedLabels);
            return (await _db.ExecuteAsync(TS.MGET, args)).ParseMGetResponse();
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
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
                                                    latest, filterByTs, filterByValue, count, align,
                                                    aggregation, timeBucket, bt, empty);

            return _db.Execute(TS.RANGE, args).ToTimeSeriesTupleArray();
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
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
                                                    latest, filterByTs, filterByValue, count, align,
                                                    aggregation, timeBucket, bt, empty);

            return (await _db.ExecuteAsync(TS.RANGE, args)).ToTimeSeriesTupleArray();
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
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
                                                    latest, filterByTs, filterByValue, count, align,
                                                    aggregation, timeBucket, bt, empty);

            return _db.Execute(TS.REVRANGE, args).ToTimeSeriesTupleArray();
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
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
                                                    latest, filterByTs, filterByValue, count, align,
                                                    aggregation, timeBucket, bt, empty);

            return (await _db.ExecuteAsync(TS.REVRANGE, args)).ToTimeSeriesTupleArray();
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
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, latest, filterByTs,
                                                         filterByValue, withLabels, selectLabels, count,
                                                         align, aggregation, timeBucket, bt, empty, groupbyTuple);
            return _db.Execute(TS.MRANGE, args).ParseMRangeResponse();
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
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, latest, filterByTs,
                                                         filterByValue, withLabels, selectLabels, count,
                                                         align, aggregation, timeBucket, bt, empty, groupbyTuple);
            return (await _db.ExecuteAsync(TS.MRANGE, args)).ParseMRangeResponse();
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
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, latest, filterByTs,
                                                         filterByValue, withLabels, selectLabels, count,
                                                         align, aggregation, timeBucket, bt, empty, groupbyTuple);
            return _db.Execute(TS.MREVRANGE, args).ParseMRangeResponse();
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
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, latest, filterByTs,
                                                         filterByValue, withLabels, selectLabels, count,
                                                         align, aggregation, timeBucket, bt, empty, groupbyTuple);
            return (await _db.ExecuteAsync(TS.MREVRANGE, args)).ParseMRangeResponse();
        }

        #endregion

        #region General

        /// <inheritdoc/>
        public TimeSeriesInformation Info(string key, bool debug = false)
        {
            var result = (debug) ? _db.Execute(TS.INFO, key, TimeSeriesArgs.DEBUG)
                                                           : _db.Execute(TS.INFO, key);
            return result.ToTimeSeriesInfo();
        }

        /// <inheritdoc/>
        public async Task<TimeSeriesInformation> InfoAsync(string key, bool debug = false)
        {
            var result = (await ((debug) ? _db.ExecuteAsync(TS.INFO, key, TimeSeriesArgs.DEBUG)
                                                                  : _db.ExecuteAsync(TS.INFO, key)));
            return result.ToTimeSeriesInfo();
        }

        /// <inheritdoc/>
        public IReadOnlyList<string> QueryIndex(IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return _db.Execute(TS.QUERYINDEX, args).ToStringArray();
        }

        /// <inheritdoc/>
        public async Task<IReadOnlyList<string>> QueryIndexAsync(IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return (await _db.ExecuteAsync(TS.QUERYINDEX, args)).ToStringArray();
        }

        #endregion
    }
}