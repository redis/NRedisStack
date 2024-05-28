using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;

namespace NRedisStack
{
    internal static class TimeSeriesCommandsBuilder
    {
        #region Create

        [Obsolete()]
        public static SerializedCommand Create(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var parameters = new TsCreateParams(retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return new SerializedCommand(TS.CREATE, parameters.ToArray(key));
        }

        public static SerializedCommand Create(string key, TsCreateParams parameters)
        {
            return new SerializedCommand(TS.CREATE, parameters.ToArray(key));
        }

        #endregion

        #region Update
        [Obsolete()]
        public static SerializedCommand Alter(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
        {
            var parameters = new TsAlterParams(retentionTime, chunkSizeBytes, duplicatePolicy, labels);
            return new SerializedCommand(TS.ALTER, parameters.ToArray(key));
        }

        public static SerializedCommand Alter(string key, TsAlterParams parameters)
        {
            return new SerializedCommand(TS.ALTER, parameters.ToArray(key));
        }

        [Obsolete()]
        public static SerializedCommand Add(string key, TimeStamp timestamp, double value, long? retentionTime = null,
        IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null,
        long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var parameters = new TsAddParams(timestamp, value, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return new SerializedCommand(TS.ADD, parameters.ToArray(key));
        }

        public static SerializedCommand Add(string key, TsAddParams parameters)
        {
            return new SerializedCommand(TS.ADD, parameters.ToArray(key));
        }

        public static SerializedCommand MAdd(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = TimeSeriesAux.BuildTsMaddArgs(sequence);
            return new SerializedCommand(TS.MADD, args);
        }

        [Obsolete()]
        public static SerializedCommand IncrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var parameters = new TsIncrByParams(value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return new SerializedCommand(TS.INCRBY, parameters.ToArray(key));
        }

        public static SerializedCommand IncrBy(string key, TsIncrByParams parameters)
        {
            return new SerializedCommand(TS.INCRBY, parameters.ToArray(key));
        }

        [Obsolete()]
        public static SerializedCommand DecrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var parameters = new TsDecrByParams(value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return new SerializedCommand(TS.DECRBY, parameters.ToArray(key));
        }

        public static SerializedCommand DecrBy(string key, TsDecrByParams parameters)
        {
            return new SerializedCommand(TS.DECRBY, parameters.ToArray(key));
        }

        public static SerializedCommand Del(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            var args = TimeSeriesAux.BuildTsDelArgs(key, fromTimeStamp, toTimeStamp);
            return new SerializedCommand(TS.DEL, args);
        }

        #endregion

        #region Aggregation, Compaction, Downsampling

        public static SerializedCommand CreateRule(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            args.Add(alignTimestamp);
            return new SerializedCommand(TS.CREATERULE, args);
        }

        public static SerializedCommand DeleteRule(string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return new SerializedCommand(TS.DELETERULE, args);
        }

        #endregion

        #region Query

        public static SerializedCommand Get(string key, bool latest = false)
        {
            return (latest) ? new SerializedCommand(TS.GET, key, TimeSeriesArgs.LATEST)
                                  : new SerializedCommand(TS.GET, key);
        }

        public static SerializedCommand MGet(IReadOnlyCollection<string> filter, bool latest = false,
                      bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null)
        {
            var args = TimeSeriesAux.BuildTsMgetArgs(latest, filter, withLabels, selectedLabels);
            return new SerializedCommand(TS.MGET, args);
        }

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
        bool empty = false)
        {
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
                                                    latest, filterByTs, filterByValue, count, align,
                                                    aggregation, timeBucket, bt, empty);

            return new SerializedCommand(TS.RANGE, args);
        }

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
        bool empty = false)
        {
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp,
                                                    latest, filterByTs, filterByValue, count, align,
                                                    aggregation, timeBucket, bt, empty);

            return new SerializedCommand(TS.REVRANGE, args);
        }

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
        (string, TsReduce)? groupbyTuple = null)
        {
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, latest, filterByTs,
                                                         filterByValue, withLabels, selectLabels, count,
                                                         align, aggregation, timeBucket, bt, empty, groupbyTuple);
            return new SerializedCommand(TS.MRANGE, args);
        }

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
        (string, TsReduce)? groupbyTuple = null)
        {
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, latest, filterByTs,
                                                         filterByValue, withLabels, selectLabels, count,
                                                         align, aggregation, timeBucket, bt, empty, groupbyTuple);
            return new SerializedCommand(TS.MREVRANGE, args);
        }

        #endregion

        #region General

        public static SerializedCommand Info(string key, bool debug = false)
        {
            return (debug) ? new SerializedCommand(TS.INFO, key, TimeSeriesArgs.DEBUG)
                                                           : new SerializedCommand(TS.INFO, key);
        }

        public static SerializedCommand QueryIndex(IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return new SerializedCommand(TS.QUERYINDEX, args);
        }

        #endregion
    }
}