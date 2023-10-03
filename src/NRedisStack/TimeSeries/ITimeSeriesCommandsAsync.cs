using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
namespace NRedisStack
{
    public interface ITimeSeriesCommandsAsync
    {
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
        /// <remarks><seealso href="https://redis.io/commands/ts.create"/></remarks>
        Task<bool> CreateAsync(string key, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null);

        #endregion

        #region Update

        /// <summary>
        /// Update the retention, labels of an existing key.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="retentionTime">Optional: Maximum age for samples compared to last event time (in milliseconds)</param>
        /// <param name="chunkSizeBytes">Optional: Each time-series uses chunks of memory of fixed size for time series samples.
        /// You can alter the default TS_db chunk size by passing the chunk_size argument (in Bytes)</param>
        /// <param name="duplicatePolicy">Optinal: Define handling of duplicate samples behavior (avalible for RedisTimeseries >= 1.4)</param>
        /// <param name="labels">Optional: Collaction of label-value pairs that represent metadata labels of the key</param>
        /// <returns>If the operation executed successfully</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.alter"/></remarks>
        Task<bool> AlterAsync(string key, long? retentionTime = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null);

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
        /// <remarks><seealso href="https://redis.io/commands/ts.add"/></remarks>
        Task<TimeStamp> AddAsync(string key, TimeStamp timestamp, double value, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null, TsDuplicatePolicy? duplicatePolicy = null);

        /// <summary>
        /// Append new samples to multiple series.
        /// </summary>
        /// <param name="sequence">An Collection of (key, timestamp, value) tuples </param>
        /// <returns>List of timestamps of the new samples</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.madd"/></remarks>
        Task<IReadOnlyList<TimeStamp>> MAddAsync(IReadOnlyCollection<(string key, TimeStamp timestamp, double value)> sequence);


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
        /// <remarks><seealso href="https://redis.io/commands/ts.incrby"/></remarks>
        Task<TimeStamp> IncrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null);

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
        /// <remarks><seealso href="https://redis.io/commands/ts.decrby"/></remarks>
        Task<TimeStamp> DecrByAsync(string key, double value, TimeStamp? timestamp = null, long? retentionTime = null, IReadOnlyCollection<TimeSeriesLabel>? labels = null, bool? uncompressed = null, long? chunkSizeBytes = null);

        /// <summary>
        /// Delete data points for a given timeseries and interval range in the form of start and end delete timestamps.
        /// The given timestamp interval is closed (inclusive), meaning start and end data points will also be deleted.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range deletion.</param>
        /// <param name="toTimeStamp">End timestamp for the range deletion.</param>
        /// <returns>The count of deleted items</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.del"/></remarks>
        Task<long> DelAsync(string key, TimeStamp fromTimeStamp, TimeStamp toTimeStamp);

        #endregion

        #region Aggregation, Compaction, Downsampling

        /// <summary>
        /// Create a compaction rule.
        /// </summary>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="rule">TimeSeries rule:
        /// Key name for destination time series, Aggregation type and Time bucket for aggregation in milliseconds</param>
        /// <param name="alignTimestamp">ensures that there is a bucket that starts
        /// exactly at alignTimestamp and aligns all other buckets accordingly.
        /// It is expressed in milliseconds. The default value is 0 aligned with the epoch</param>
        /// <returns>If the operation executed successfully</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.createrule"/></remarks>
        Task<bool> CreateRuleAsync(string sourceKey, TimeSeriesRule rule, long alignTimestamp = 0);

        /// <summary>
        /// Deletes a compaction rule.
        /// </summary>
        /// <param name="sourceKey">Key name for source time series</param>
        /// <param name="destKey">Key name for destination time series</param>
        /// <returns>If the operation executed successfully</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.deleterule"/></remarks>
        Task<bool> DeleteRuleAsync(string sourceKey, string destKey);

        #endregion

        #region Query

        /// <summary>
        /// Get the last sample.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="latest">is used when a time series is a compaction. With LATEST, TS.MRANGE also reports
        /// the compacted value of the latest possibly partial bucket, given that this bucket's start time falls
        /// within [fromTimestamp, toTimestamp]. Without LATEST, TS.MRANGE does not report the latest possibly partial bucket.
        /// When a time series is not a compaction, LATEST is ignored.</param>
        /// <returns>TimeSeriesTuple that represents the last sample. Null if the series is empty. </returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.get"/></remarks>
        Task<TimeSeriesTuple?> GetAsync(string key, bool latest = false);

        /// <summary>
        /// Get the last samples matching the specific filter.
        /// </summary>
        /// <param name="latest">is used when a time series is a compaction. With LATEST, TS.MRANGE also reports
        /// the compacted value of the latest possibly partial bucket, given that this bucket's start time falls
        /// within [fromTimestamp, toTimestamp]. Without LATEST, TS.MRANGE does not report the latest possibly partial bucket.
        /// When a time series is not a compaction, LATEST is ignored.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="selectedLabels">Optional: returns a subset of the label-value pairs that represent metadata labels of the time series</param>
        /// <returns>The command returns the last sample for entries with labels matching the specified filter.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.mget"/></remarks>
        Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)>> MGetAsync(IReadOnlyCollection<string> filter, bool latest = false,
                                                                                                                               bool? withLabels = null, IReadOnlyCollection<string>? selectedLabels = null);

        /// <summary>
        /// Query a range.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="latest">is used when a time series is a compaction. With LATEST, TS.MRANGE also reports
        /// the compacted value of the latest possibly partial bucket, given that this bucket's start time falls
        /// within [fromTimestamp, toTimestamp]. Without LATEST, TS.MRANGE does not report the latest possibly partial bucket.
        /// When a time series is not a compaction, LATEST is ignored.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="bt">Optional: controls how bucket timestamps are reported.</param>
        /// <param name="empty">Optional: when specified, reports aggregations also for empty buckets</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.range"/></remarks>
        Task<IReadOnlyList<TimeSeriesTuple>> RangeAsync(string key,
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
                                                    bool empty = false);

        /// <summary>
        /// Query a range in reverse direction.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="fromTimeStamp">Start timestamp for the range query. "-" can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="latest">is used when a time series is a compaction. With LATEST, TS.MRANGE also reports
        /// the compacted value of the latest possibly partial bucket, given that this bucket's start time falls
        /// within [fromTimestamp, toTimestamp]. Without LATEST, TS.MRANGE does not report the latest possibly partial bucket.
        /// When a time series is not a compaction, LATEST is ignored.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="count">Optional: Returned list size.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="bt">Optional: controls how bucket timestamps are reported.</param>
        /// <param name="empty">Optional: when specified, reports aggregations also for empty buckets</param>
        /// <returns>A list of TimeSeriesTuple</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.revrange"/></remarks>
        Task<IReadOnlyList<TimeSeriesTuple>> RevRangeAsync(string key,
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
                                                    bool empty = false);

        /// <summary>
        /// Query a timestamp range across multiple time-series by filters.
        /// </summary>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="latest">is used when a time series is a compaction. With LATEST, TS.MRANGE also reports
        /// the compacted value of the latest possibly partial bucket, given that this bucket's start time falls
        /// within [fromTimestamp, toTimestamp]. Without LATEST, TS.MRANGE does not report the latest possibly partial bucket.
        /// When a time series is not a compaction, LATEST is ignored.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="bt">Optional: controls how bucket timestamps are reported.</param>
        /// <param name="empty">Optional: when specified, reports aggregations also for empty buckets</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.mrange"/></remarks>
        Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>> MRangeAsync(
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
            (string, TsReduce)? groupbyTuple = null);

        /// <summary>
        /// Query a timestamp range in reverse order across multiple time-series by filters.
        /// </summary>
        /// <param name="fromTimeStamp"> Start timestamp for the range query. - can be used to express the minimum possible timestamp.</param>
        /// <param name="toTimeStamp">End timestamp for range query, + can be used to express the maximum possible timestamp.</param>
        /// <param name="filter">A sequence of filters</param>
        /// <param name="latest">is used when a time series is a compaction. With LATEST, TS.MRANGE also reports
        /// the compacted value of the latest possibly partial bucket, given that this bucket's start time falls
        /// within [fromTimestamp, toTimestamp]. Without LATEST, TS.MRANGE does not report the latest possibly partial bucket.
        /// When a time series is not a compaction, LATEST is ignored.</param>
        /// <param name="filterByTs">Optional: List of timestamps to filter the result by specific timestamps</param>
        /// <param name="filterByValue">Optional: Filter result by value using minimum and maximum</param>
        /// <param name="withLabels">Optional: Include in the reply the label-value pairs that represent metadata labels of the time-series</param>
        /// <param name="selectLabels">Optional: Include in the reply only a subset of the key-value pair labels of a series.</param>
        /// <param name="count">Optional: Maximum number of returned results per time-series.</param>
        /// <param name="align">Optional: Timestamp for alignment control for aggregation.</param>
        /// <param name="aggregation">Optional: Aggregation type</param>
        /// <param name="timeBucket">Optional: Time bucket for aggregation in milliseconds</param>
        /// <param name="bt">Optional: controls how bucket timestamps are reported.</param>
        /// <param name="empty">Optional: when specified, reports aggregations also for empty buckets</param>
        /// <param name="groupbyTuple">Optional: Grouping by fields the results, and applying reducer functions on each group.</param>
        /// <returns>A list of (key, labels, values) tuples. Each tuple contains the key name, its labels and the values which satisfies the given range and filters.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.mrevrange"/></remarks>
        Task<IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>> MRevRangeAsync(
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
            (string, TsReduce)? groupbyTuple = null);

        #endregion

        #region General

        /// <summary>
        /// Returns the information for a specific time-series key.
        /// </summary>
        /// <param name="key">Key name for timeseries</param>
        /// <param name="debug">An optional flag to get a more detailed information about the chunks.</param>
        /// <returns>TimeSeriesInformation for the specific key.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.info"/></remarks>
        Task<TimeSeriesInformation> InfoAsync(string key, bool debug = false);

        /// <summary>
        /// Get all the keys matching the filter list.
        /// </summary>
        /// <param name="filter">A sequence of filters</param>
        /// <returns>A list of keys with labels matching the filters.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ts.queryindex"/></remarks>
        Task<IReadOnlyList<string>> QueryIndexAsync(IReadOnlyCollection<string> filter);

        #endregion
    }
}