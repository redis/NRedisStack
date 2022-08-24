using System;
using System.Text.Json;
using System.Collections.Generic;
using NRedisStack.Literals.Enums;


namespace NRedisStack.DataTypes
{
    /// <summary>
    /// This class represents the response for TS.INFO command.
    /// This object has Read-only properties and cannot be generated outside a TS.INFO response.
    /// </summary>
    public class TimeSeriesInformation
    {
        /// <summary>
        /// Total samples in the time-series.
        /// </summary>
        public long TotalSamples { get; private set; }

        /// <summary>
        /// Total number of bytes allocated for the time-series.
        /// </summary>
        public long MemoryUsage { get; private set; }

        /// <summary>
        /// First timestamp present in the time-series.
        /// </summary>
        public TimeStamp FirstTimeStamp { get; private set; }

        /// <summary>
        /// Last timestamp present in the time-series.
        /// </summary>
        public TimeStamp LastTimeStamp { get; private set; }

        /// <summary>
        /// Retention time, in milliseconds, for the time-series.
        /// </summary>
        public long RetentionTime { get; private set; }

        /// <summary>
        /// Number of Memory Chunks used for the time-series.
        /// </summary>
        public long ChunkCount { get; private set; }

        /// <summary>
        /// Maximum Number of samples per Memory Chunk.
        /// </summary>
        [ObsoleteAttribute("This method has been deprecated. Use ChunkSize instead.")]
        public long MaxSamplesPerChunk { get; private set; }

        /// <summary>
        /// Memory Chunk size in Bytes.
        /// </summary>
        public long ChunkSize { get; private set; }

        /// <summary>
        /// A readonly list of TimeSeriesLabel that represent metadata labels of the time-series.
        /// </summary>
        public IReadOnlyList<TimeSeriesLabel> Labels { get; private set; }

        /// <summary>
        /// Source key for the queries time series key.
        /// </summary>
        public string SourceKey { get; private set; }

        /// <summary>
        /// A readonly list of TimeSeriesRules that represent compaction Rules of the time-series.
        /// </summary>
        public IReadOnlyList<TimeSeriesRule> Rules { get; private set; }

        /// <summary>
        /// The policy will define handling of duplicate samples.
        /// </summary>
        public TsDuplicatePolicy? DuplicatePolicy {  get; private set; }

        /* TODO: add TS.INFO DEBUG Arguments (string keySelfName and Cunks:
        27) Chunks
        28) 1)  1) startTimestamp
                2) (integer) 0
                3) endTimestamp
                4) (integer) 0
                5) samples
                6) (integer) 0
                7) size
                8) (integer) 4096
                9) bytesPerSample
                10) "inf"))*/

        internal TimeSeriesInformation(long totalSamples, long memoryUsage, TimeStamp firstTimeStamp, TimeStamp lastTimeStamp, long retentionTime, long chunkCount, long chunkSize, IReadOnlyList<TimeSeriesLabel> labels, string sourceKey, IReadOnlyList<TimeSeriesRule> rules, TsDuplicatePolicy? policy)
        {
            TotalSamples = totalSamples;
            MemoryUsage = memoryUsage;
            FirstTimeStamp = firstTimeStamp;
            LastTimeStamp = lastTimeStamp;
            RetentionTime = retentionTime;
            ChunkCount = chunkCount;
            Labels = labels;
            SourceKey = sourceKey;
            Rules = rules;
            // backwards compatible with RedisTimeSeries < v1.4
            MaxSamplesPerChunk = chunkSize/16;
            ChunkSize = chunkSize;
            // configure what to do on duplicate sample > v1.4
            DuplicatePolicy = policy;
        }

        /// <summary>
        /// Implicit cast from TimeSeriesInformation to string.
        /// </summary>
        /// <param name="info">TimeSeriesInformation</param>
        public static implicit operator string(TimeSeriesInformation info) => JsonSerializer.Serialize(info);
    }
}
