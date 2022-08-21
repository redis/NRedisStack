using StackExchange.Redis;
using System;
using System.Collections.Generic;
using NRedisStack.Literals;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
namespace NRedisStack
{
    public class TimeSeriesCommands
    {
        IDatabase _db;
        public TimeSeriesCommands(IDatabase db)
        {
            _db = db;
        }

         #region Create

        /// <summary>
        /// Create a new time-series.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <param name="duplicatePolicy">Optinal: Define handling of duplicate samples behavior (avalible for RedisTimeseries >= 1.4)</param>
        /// <returns>If the operation executed successfully</returns>
        public bool Create(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsCreateArgs(key, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return ResponseParser.OKtoBoolean(_db.Execute(TS.CREATE, args));
        }

        /// <summary>
        /// Create a new time-series.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <param name="duplicatePolicy">Optinal: Define handling of duplicate samples behavior (avalible for RedisTimeseries >= 1.4)</param>
        /// <returns>If the operation executed successfully</returns>
        public async Task<bool> CreateAsync(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsCreateArgs(key, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return ResponseParser.OKtoBoolean(await _db.ExecuteAsync(TS.CREATE, args));
        }

        #endregion

        #region Update

        /// <summary>
        /// Update the retention, labels of an existing key.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <returns>If the operation executed successfully</returns>
        public bool Alter(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
        {
            var args = TimeSeriesAux.BuildTsAlterArgs(key, retentionTime, labels);
            return ResponseParser.OKtoBoolean(_db.Execute(TS.ALTER, args));
        }

        /// <summary>
        /// Update the retention, labels of an existing key.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <returns>If the operation executed successfully</returns>
        public async Task<bool> AlterAsync(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null)
        {
            var args = TimeSeriesAux.BuildTsAlterArgs(key, retentionTime, labels);
            return ResponseParser.OKtoBoolean(await _db.ExecuteAsync(TS.ALTER, args));
        }

        /// <summary>
        /// Append (or create and append) a new sample to the series.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="timestamp">TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="value">Numeric data value of the sample.</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <param name="duplicatePolicy">Optioal: overwrite key and database configuration for DUPLICATE_POLICY</param>
        /// <returns>The timestamp value of the new sample</returns>
        public TimeStamp Add(string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsAddArgs(key, timestamp, value, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return ResponseParser.ToTimeStamp(_db.Execute(TS.ADD, args));
        }

        /// <summary>
        /// Append (or create and append) a new sample to the series.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="timestamp">TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="value">Numeric data value of the sample.</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <param name="duplicatePolicy">Optioal: overwrite key and database configuration for DUPLICATE_POLICY</param>
        /// <returns>The timestamp value of the new sample</returns>
        public async Task<TimeStamp> AddAsync(string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel> labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null)
        {
            var args = TimeSeriesAux.BuildTsAddArgs(key, timestamp, value, retentionTime, labels, uncompressed, chunkSizeBytes, duplicatePolicy);
            return ResponseParser.ToTimeStamp(await _db.ExecuteAsync(TS.ADD, args));
        }

        /// <summary>
        /// Append new samples to multiple series.
        /// </summary>
        /// <param name="sequence">An Collection of (key, timestamp, value) tuples </param>
        /// <returns>List of timestamps of the new samples</returns>
        public IReadOnlyList<TimeStamp> MAdd(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = TimeSeriesAux.BuildTsMaddArgs(sequence);
            return ResponseParser.ToTimeStampArray(_db.Execute(TS.MADD, args));
        }

        /// <summary>
        /// Append new samples to multiple series.
        /// </summary>
        /// <param name="sequence">An Collection of (key, timestamp, value) tuples </param>
        /// <returns>List of timestamps of the new samples</returns>
        public async Task<IReadOnlyList<TimeStamp>> MAddAsync(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence)
        {
            var args = TimeSeriesAux.BuildTsMaddArgs(sequence);
            return ResponseParser.ToTimeStampArray(await _db.ExecuteAsync(TS.MADD, args));
        }

        /// <summary>
        /// Creates a new sample that increments the latest sample's value.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="value">Delta to add</param>
        /// <param name="timestamp">Optional: TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public TimeStamp IncrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return ResponseParser.ToTimeStamp(_db.Execute(TS.INCRBY, args));
        }

        /// <summary>
        /// Creates a new sample that increments the latest sample's value.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="value">Delta to add</param>
        /// <param name="timestamp">Optional: TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public async Task<TimeStamp> IncrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return ResponseParser.ToTimeStamp(await _db.ExecuteAsync(TS.INCRBY, args));
        }

        /// <summary>
        /// Creates a new sample that decrements the latest sample's value.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="value">Delta to substract</param>
        /// <param name="timestamp">Optional: TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public TimeStamp DecrBy(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return ResponseParser.ToTimeStamp(_db.Execute(TS.DECRBY, args));
        }

        /// <summary>
        /// Creates a new sample that decrements the latest sample's value.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="value">Delta to substract</param>
        /// <param name="timestamp">Optional: TimeStamp to add. UNIX timestamp of the sample. * can be used for automatic timestamp (using the system clock)</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <param name="uncompressed">Optional: Adding this flag will keep data in an uncompressed form</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <returns>The latests sample timestamp (updated sample)</returns>
        public async Task<TimeStamp> DecrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null)
        {
            var args = TimeSeriesAux.BuildTsIncrDecrByArgs(key, value, timestamp, retentionTime, labels, uncompressed, chunkSizeBytes);
            return ResponseParser.ToTimeStamp(await _db.ExecuteAsync(TS.DECRBY, args));
        }

        /// <summary>
        /// Delete data points for a given timeseries and interval range in the form of start and end delete timestamps.
        /// The given timestamp interval is closed (inclusive), meaning start and end data points will also be deleted.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range deletion.</param>
        /// <param name="toTimeStamp">End timestamp for the range deletion.</param>
        /// <returns>The count of deleted items</returns>
        public long Del(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            var args = TimeSeriesAux.BuildTsDelArgs(key, fromTimeStamp, toTimeStamp);
            return ResponseParser.ToLong(_db.Execute(TS.DEL, args));
        }

        /// <summary>
        /// Delete data points for a given timeseries and interval range in the form of start and end delete timestamps.
        /// The given timestamp interval is closed (inclusive), meaning start and end data points will also be deleted.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range deletion.</param>
        /// <param name="toTimeStamp">End timestamp for the range deletion.</param>
        /// <returns>The count of deleted items</returns>
        public async Task<long> DelAsync(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp)
        {
            var args = TimeSeriesAux.BuildTsDelArgs(key, fromTimeStamp, toTimeStamp);
            return ResponseParser.ToLong(await _db.ExecuteAsync(TS.DEL, args));
        }

        #endregion

        #region Aggregation, Compaction, Downsampling

        /// <summary>
        /// Create a compaction rule.
        /// </summary>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="rule">TimeSeries rule:
        /// Key name for destination time series, Aggregation type and Time bucket for aggregation in milliseconds</param>
        /// <returns>If the operation executed successfully</returns>
        public bool CreateRule(string sourceKey, TimeSeriesRule rule)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            return ResponseParser.OKtoBoolean(_db.Execute(TS.CREATERULE, args));
        }

        /// <summary>
        /// Create a compaction rule.
        /// </summary>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="rule">TimeSeries rule:
        /// Key name for destination time series, Aggregation type and Time bucket for aggregation in milliseconds</param>
        /// <returns>If the operation executed successfully</returns>
        public async Task<bool> CreateRuleAsync(string sourceKey, TimeSeriesRule rule)
        {
            var args = new List<object> { sourceKey };
            args.AddRule(rule);
            return ResponseParser.OKtoBoolean(await _db.ExecuteAsync(TS.CREATERULE, args));
        }

        /// <summary>
        /// Deletes a compaction rule.
        /// </summary>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="destKey">Key name for destination time series</param>
        /// <returns>If the operation executed successfully</returns>
        public bool DeleteRule(string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return ResponseParser.OKtoBoolean(_db.Execute(TS.DELETERULE, args));
        }

        /// <summary>
        /// Deletes a compaction rule.
        /// </summary>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="destKey">Key name for destination time series</param>
        /// <returns>If the operation executed successfully</returns>
        public async Task<bool> DeleteRuleAsync(string sourceKey, string destKey)
        {
            var args = new List<object> { sourceKey, destKey };
            return ResponseParser.OKtoBoolean(await _db.ExecuteAsync(TS.DELETERULE, args));
        }

        #endregion

        #region Query

        /// <summary>
        /// Get the last sample.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesTuple that represents the last sample. Null if the series is empty. </returns>
        public TimeSeriesTuple? Get(string key)
        {
            return ResponseParser.ToTimeSeriesTuple(_db.Execute(TS.GET, key));
        }

        /// <summary>
        /// Get the last sample.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesTuple that represents the last sample. Null if the series is empty. </returns>
        public async Task<TimeSeriesTuple?> GetAsync(string key)
        {
            return ResponseParser.ToTimeSeriesTuple(await _db.ExecuteAsync(TS.GET, key));
        }

        /// <summary>
        /// Get the last samples matching the specific filter.
        /// </summary>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <returns>The command returns the last sample for entries with labels matching the specified filter.</returns>
        public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> MGet(IReadOnlyCollection<string> filter, bool? withLabels = null)
        {
            var args = TimeSeriesAux.BuildTsMgetArgs(filter, withLabels);
            return ResponseParser.ParseMGetResponse(_db.Execute(TS.MGET, args));
        }

        /// <summary>
        /// Get the last samples matching the specific filter.
        /// </summary>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <returns>The command returns the last sample for entries with labels matching the specified filter.</returns>
        public async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)>> MGetAsync(IReadOnlyCollection<string> filter, bool? withLabels = null)
        {
            var args = TimeSeriesAux.BuildTsMgetArgs(filter, withLabels);
            return ResponseParser.ParseMGetResponse(await _db.ExecuteAsync(TS.MGET, args));
        }

        /// <summary>
        /// Query a range.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        public IReadOnlyList<TimeSeriesTuple> Range(string key,
                                                    TimeStamp fromTimeStamp,
                                                    TimeStamp toTimeStamp,
                                                    long? count = null,
                                                    TsAggregation? aggregation = null,
                                                    long? timeBucket = null,
                                                    IReadOnlyCollection<TimeStamp>? filterByTs = null,
                                                    (long, long)? filterByValue = null,
                                                    TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation, timeBucket, filterByTs, filterByValue, align);
            return ResponseParser.ToTimeSeriesTupleArray(_db.Execute(TS.RANGE, args));
        }

        /// <summary>
        /// Query a range.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        public async Task<IReadOnlyList<TimeSeriesTuple>> RangeAsync(string key,
                                                    TimeStamp fromTimeStamp,
                                                    TimeStamp toTimeStamp,
                                                    long? count = null,
                                                    TsAggregation? aggregation = null,
                                                    long? timeBucket = null,
                                                    IReadOnlyCollection<TimeStamp>? filterByTs = null,
                                                    (long, long)? filterByValue = null,
                                                    TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation, timeBucket, filterByTs, filterByValue, align);
            return ResponseParser.ToTimeSeriesTupleArray(await _db.ExecuteAsync(TS.RANGE, args));
        }

        /// <summary>
        /// Query a range in reverse order.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        public IReadOnlyList<TimeSeriesTuple> RevRange(
            string key,
            TimeStamp fromTimeStamp,
            TimeStamp toTimeStamp,
            long? count = null,
            TsAggregation? aggregation = null,
            long? timeBucket = null,
            IReadOnlyCollection<TimeStamp>? filterByTs = null,
            (long, long)? filterByValue = null,
            TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation, timeBucket, filterByTs, filterByValue, align);
            return ResponseParser.ToTimeSeriesTupleArray(_db.Execute(TS.REVRANGE, args));
        }

        /// <summary>
        /// Query a range in reverse order.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        public async Task<IReadOnlyList<TimeSeriesTuple>> RevRangeAsync(
            string key,
            TimeStamp fromTimeStamp,
            TimeStamp toTimeStamp,
            long? count = null,
            TsAggregation? aggregation = null,
            long? timeBucket = null,
            IReadOnlyCollection<TimeStamp>? filterByTs = null,
            (long, long)? filterByValue = null,
            TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildRangeArgs(key, fromTimeStamp, toTimeStamp, count, aggregation, timeBucket, filterByTs, filterByValue, align);
            return ResponseParser.ToTimeSeriesTupleArray(await _db.ExecuteAsync(TS.REVRANGE, args));
        }

        /// <summary>
        /// Query a timestamp range across multiple time-series by filters.
        /// </summary>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRange(
            TimeStamp fromTimeStamp,
            TimeStamp toTimeStamp,
            IReadOnlyCollection<string> filter,
            long? count = null,
            TsAggregation? aggregation = null,
            long? timeBucket = null,
            bool? withLabels = null,
            (string, TsReduce)? groupbyTuple = null,
            IReadOnlyCollection<TimeStamp>? filterByTs = null,
            (long, long)? filterByValue = null,
            IReadOnlyCollection<string>? selectLabels = null,
            TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count, aggregation, timeBucket, withLabels, groupbyTuple, filterByTs, filterByValue, selectLabels, align);
            return ResponseParser.ParseMRangeResponse(_db.Execute(TS.MRANGE, args));
        }

        /// <summary>
        /// Query a timestamp range across multiple time-series by filters.
        /// </summary>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        public async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>> MRangeAsync(
            TimeStamp fromTimeStamp,
            TimeStamp toTimeStamp,
            IReadOnlyCollection<string> filter,
            long? count = null,
            TsAggregation? aggregation = null,
            long? timeBucket = null,
            bool? withLabels = null,
            (string, TsReduce)? groupbyTuple = null,
            IReadOnlyCollection<TimeStamp>? filterByTs = null,
            (long, long)? filterByValue = null,
            IReadOnlyCollection<string>? selectLabels = null,
            TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count, aggregation, timeBucket,
                                                         withLabels, groupbyTuple, filterByTs, filterByValue, selectLabels, align);
            return ResponseParser.ParseMRangeResponse(await _db.ExecuteAsync(TS.MRANGE, args));
        }

        /// <summary>
        /// Query a timestamp range in reverse order across multiple time-series by filters.
        /// </summary>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        public IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> MRevRange(
            TimeStamp fromTimeStamp,
            TimeStamp toTimeStamp,
            IReadOnlyCollection<string> filter,
            long? count = null,
            TsAggregation? aggregation = null,
            long? timeBucket = null,
            bool? withLabels = null,
            (string, TsReduce)? groupbyTuple = null,
            IReadOnlyCollection<TimeStamp>? filterByTs = null,
            (long, long)? filterByValue = null,
            IReadOnlyCollection<string>? selectLabels = null,
            TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count, aggregation, timeBucket, withLabels, groupbyTuple, filterByTs, filterByValue, selectLabels, align);
            return ResponseParser.ParseMRangeResponse(_db.Execute(TS.MREVRANGE, args));
        }

        /// <summary>
        /// Query a timestamp range in reverse order across multiple time-series by filters.
        /// </summary>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        public async Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>> MRevRangeAsync(
            TimeStamp fromTimeStamp,
            TimeStamp toTimeStamp,
            IReadOnlyCollection<string> filter,
            long? count = null,
            TsAggregation? aggregation = null,
            long? timeBucket = null,
            bool? withLabels = null,
            (string, TsReduce)? groupbyTuple = null,
            IReadOnlyCollection<TimeStamp>? filterByTs = null,
            (long, long)? filterByValue = null,
            IReadOnlyCollection<string>? selectLabels = null,
            TimeStamp? align = null)
        {
            var args = TimeSeriesAux.BuildMultiRangeArgs(fromTimeStamp, toTimeStamp, filter, count, aggregation, timeBucket, withLabels, groupbyTuple, filterByTs, filterByValue, selectLabels, align);
            return ResponseParser.ParseMRangeResponse(await _db.ExecuteAsync(TS.MREVRANGE, args));
        }

        #endregion

        #region General

        /// <summary>
        /// Returns the information for a specific time-series key.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesInformation for the specific key.</returns>
        public TimeSeriesInformation Info(string key)
        {
            return ResponseParser.ToTimeSeriesInfo(_db.Execute(TS.INFO, key));
        }

        /// <summary>
        /// Returns the information for a specific time-series key.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <returns>TimeSeriesInformation for the specific key.</returns>
        public async Task<TimeSeriesInformation> InfoAsync(string key)
        {
            return ResponseParser.ToTimeSeriesInfo(await _db.ExecuteAsync(TS.INFO, key));
        }

        /// <summary>
        /// Get all the keys matching the filter list.
        /// </summary>
        /// <param name="filter">A sequence of filters</param>
        /// <returns>A list of keys with labels matching the filters.</returns>
        public IReadOnlyList<string> QueryIndex(IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return ResponseParser.ToStringArray(_db.Execute(TS.QUERYINDEX, args));
        }

        /// <summary>
        /// Get all the keys matching the filter list.
        /// </summary>
        /// <param name="filter">A sequence of filters</param>
        /// <returns>A list of keys with labels matching the filters.</returns>
        public async Task<IReadOnlyList<string>> QueryIndexAsync(IReadOnlyCollection<string> filter)
        {
            var args = new List<object>(filter);
            return ResponseParser.ToStringArray(await _db.ExecuteAsync(TS.QUERYINDEX, args));
        }

        #endregion



    }


}



