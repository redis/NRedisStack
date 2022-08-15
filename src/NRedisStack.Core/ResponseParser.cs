using System;
using System.Collections.Generic;
using NRedisStack.Core.Literals.Enums;
using NRedisStack.Core.DataTypes;
using NRedisStack.Core.Extensions;
using StackExchange.Redis;
using NRedisStack.Core.Bloom.DataTypes;
using NRedisStack.Core.CuckooFilter.DataTypes;
using NRedisStack.Core.CountMinSketch.DataTypes;
using NRedisStack.Core.TopK.DataTypes;
using NRedisStack.Core.Tdigest.DataTypes;

namespace NRedisStack.Core
{
    public static class ResponseParser
    {
        public static bool OKtoBoolean(RedisResult result)
        {
            return result.ToString() == "OK";
        }

        public static bool[] ToBooleanArray(RedisResult result)
        {
            RedisResult[]? redisResults = ToArray(result);

            bool[] boolArr = new bool[redisResults.Length];
            for (int i = 0; i < redisResults.Length; i++)
            {
                boolArr[i] = redisResults[i].ToString() == "1";
            }

            return boolArr;
        }

        public static RedisResult[] ToArray(RedisResult result)
        {
            var redisResults = (RedisResult[]?)result;
            if(redisResults != null)
                return redisResults;
            throw new ArgumentNullException(nameof(redisResults));


        }

        public static long ToLong(RedisResult result)
        {
            if((long?) result == null)
                throw new ArgumentNullException(nameof(result));
            return (long) result;
        }

        public static double ToDouble(RedisResult result)
        {
            if((double?) result == null)
                throw new ArgumentNullException(nameof(result));
            return (double) result;
        }

        public static double[] ToDoubleArray(RedisResult result)
        {
            List<double> redisResults = new List<double>();
            foreach(var res in ToArray(result))
            {
                redisResults.Add(ToDouble(res));
            }

            return redisResults.ToArray();
        }

        public static long[] ToLongArray(RedisResult result)
        {
            List<long> redisResults = new List<long>();
            foreach(var res in ToArray(result))
            {
                redisResults.Add(ToLong(res));
            }

            return redisResults.ToArray();
        }

        public static TimeStamp ToTimeStamp(RedisResult result)
        {
            if (result.Type == ResultType.None) return null;
            return new TimeStamp((long)result);
        }

        public static IReadOnlyList<TimeStamp> ToTimeStampArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeStamp>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, timestamp => list.Add(ToTimeStamp(timestamp)));
            return list;
        }

        public static TimeSeriesTuple? ToTimeSeriesTuple(RedisResult result)
        {
            RedisResult[] redisResults = ToArray(result);
            if (redisResults.Length == 0) return null;
            return new TimeSeriesTuple(ToTimeStamp(redisResults[0]), (double)redisResults[1]);
        }

        public static Tuple<long, Byte[]> ToScanDumpTuple(RedisResult result)
        {
            RedisResult[] redisResults = ToArray(result);
            if (redisResults == null || redisResults.Length == 0) return null;
            return new Tuple<long, Byte[]>((long)redisResults[0], (Byte[])redisResults[1]);
        }

        public static HashEntry ToHashEntry(RedisResult result)
        {
            RedisResult[] redisResults = ToArray(result);
            if (redisResults.Length < 2)
                throw new ArgumentOutOfRangeException(nameof(result));

            return new HashEntry((RedisValue)(redisResults[0]), ((RedisValue)redisResults[1]));
        }

        public static HashEntry[] ToHashEntryArray(RedisResult result)
        {
            RedisResult[] redisResults = ToArray(result);

            var hash = new HashEntry[redisResults.Length / 2];
            if (redisResults.Length == 0) return hash;

            for (int i = 0; i < redisResults.Length - 1; i += 2)
                hash[i / 2] = new HashEntry(((RedisValue)redisResults[i]), ((RedisValue)redisResults[i + 1]));
            return hash;
        }

        public static IReadOnlyList<TimeSeriesTuple> ToTimeSeriesTupleArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeSeriesTuple>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, tuple => list.Add(ToTimeSeriesTuple(tuple)));
            return list;
        }

        public static IReadOnlyList<TimeSeriesLabel> ToLabelArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeSeriesLabel>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, labelResult =>
            {
                RedisResult[] labelTuple = (RedisResult[])labelResult;
                list.Add(new TimeSeriesLabel((string)labelTuple[0], (string)labelTuple[1]));
            });
            return list;
        }

        public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> ParseMGetesponse(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple values)>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, MRangeValue =>
            {
                RedisResult[] MRangeTuple = (RedisResult[])MRangeValue;
                string key = (string)MRangeTuple[0];
                IReadOnlyList<TimeSeriesLabel> labels = ToLabelArray(MRangeTuple[1]);
                TimeSeriesTuple value = ToTimeSeriesTuple(MRangeTuple[2]);
                list.Add((key, labels, value));
            });
            return list;
        }

        public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> ParseMRangeResponse(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, MRangeValue =>
            {
                RedisResult[] MRangeTuple = (RedisResult[])MRangeValue;
                string key = (string)MRangeTuple[0];
                IReadOnlyList<TimeSeriesLabel> labels = ToLabelArray(MRangeTuple[1]);
                IReadOnlyList<TimeSeriesTuple> values = ToTimeSeriesTupleArray(MRangeTuple[2]);
                list.Add((key, labels, values));
            });
            return list;
        }

        public static TimeSeriesRule ToRule(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            string destKey = (string)redisResults[0];
            long bucketTime = (long)redisResults[1];
            var aggregation = AggregationExtensions.AsAggregation((string)redisResults[2]);
            return new TimeSeriesRule(destKey, bucketTime, aggregation);
        }

        public static IReadOnlyList<TimeSeriesRule> ToRuleArray(RedisResult result)
        {
            RedisResult[] redisResults = (RedisResult[])result;
            var list = new List<TimeSeriesRule>();
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, rule => list.Add(ToRule(rule)));
            return list;
        }

        public static TsDuplicatePolicy? ToPolicy(RedisResult result)
        {
            var policyStatus = (string)result;
            if (String.IsNullOrEmpty(policyStatus) || policyStatus == "(nil)")
            {
                return null;
            }

            return DuplicatePolicyExtensions.AsPolicy(policyStatus.ToUpper());
        }

        public static BloomInformation ToBloomInfo(RedisResult result) //TODO: Think about a different implementation, because if the output of BF.INFO changes or even just the names of the labels then the parsing will not work
        {
            long capacity, size, numberOfFilters, numberOfItemsInserted, expansionRate;
            capacity = size = numberOfFilters = numberOfItemsInserted = expansionRate = -1;
            RedisResult[] redisResults = ToArray(result);

            for (int i = 0; i < redisResults.Length; ++i)
            {
                string? label = redisResults[i++].ToString();
                switch (label)
                {
                    case "Capacity":
                        capacity = (long)redisResults[i];
                        break;
                    case "Size":
                        size = (long)redisResults[i];
                        break;
                    case "Number of filters":
                        numberOfFilters = (long)redisResults[i];
                        break;
                    case "Number of items inserted":
                        numberOfItemsInserted = (long)redisResults[i];
                        break;
                    case "Expansion rate":
                        expansionRate = (long)redisResults[i];
                        break;
                }
            }

            return new BloomInformation(capacity, size, numberOfFilters, numberOfItemsInserted, expansionRate);
        }

        public static CuckooInformation ToCuckooInfo(RedisResult result) //TODO: Think about a different implementation, because if the output of BF.INFO changes or even just the names of the labels then the parsing will not work
        {
            long size, numberOfBuckets, numberOfFilter, numberOfItemsInserted,
                 numberOfItemsDeleted, bucketSize, expansionRate, maxIteration;

            size = numberOfBuckets = numberOfFilter =
            numberOfItemsInserted = numberOfItemsDeleted =
            bucketSize = expansionRate = maxIteration = -1;

            RedisResult[] redisResults = ToArray(result);

            for (int i = 0; i < redisResults.Length; ++i)
            {
                string? label = redisResults[i++].ToString();

                switch (label)
                {
                    case "Size":
                        size = (long)redisResults[i];
                        break;
                    case "Number of buckets":
                        numberOfBuckets = (long)redisResults[i];
                        break;
                    case "Number of filter":
                        numberOfFilter = (long)redisResults[i];
                        break;
                    case "Number of items inserted":
                        numberOfItemsInserted = (long)redisResults[i];
                        break;
                    case "Number of items deleted":
                        numberOfItemsDeleted = (long)redisResults[i];
                        break;
                    case "Bucket size":
                        bucketSize = (long)redisResults[i];
                        break;
                    case "Expansion rate":
                        expansionRate = (long)redisResults[i];
                        break;
                    case "Max iteration":
                        maxIteration = (long)redisResults[i];
                        break;
                }
            }

            return new CuckooInformation(size, numberOfBuckets, numberOfFilter, numberOfItemsInserted,
                                        numberOfItemsDeleted, bucketSize, expansionRate, maxIteration);
        }

        public static CmsInformation ToCmsInfo(RedisResult result) //TODO: Think about a different implementation, because if the output of CMS.INFO changes or even just the names of the labels then the parsing will not work
        {
            long width, depth, count;

            width = depth = count = -1;

            RedisResult[] redisResults = ToArray(result);

            for (int i = 0; i < redisResults.Length; ++i)
            {
                string? label = redisResults[i++].ToString();

                switch (label)
                {
                    case "width":
                        width = (long)redisResults[i];
                        break;
                    case "depth":
                        depth = (long)redisResults[i];
                        break;
                    case "count":
                        count = (long)redisResults[i];
                        break;
                }
            }

            return new CmsInformation(width, depth, count);
        }

        public static TopKInformation ToTopKInfo(RedisResult result) //TODO: Think about a different implementation, because if the output of CMS.INFO changes or even just the names of the labels then the parsing will not work
        {
            long k, width, depth;
            double decay;

            k = width = depth = -1;
            decay = -1.0;

            RedisResult[] redisResults = ToArray(result);

            for (int i = 0; i < redisResults.Length; ++i)
            {
                string? label = redisResults[i++].ToString();

                switch (label)
                {
                    case "k":
                        k = (long)redisResults[i];
                        break;
                    case "width":
                        width = (long)redisResults[i];
                        break;
                    case "depth":
                        depth = (long)redisResults[i];
                        break;
                    case "decay":
                        decay = (double)redisResults[i];
                        break;
                }
            }

            return new TopKInformation(k, width, depth, decay);
        }

        public static TdigestInformation ToTdigestInfo(RedisResult result) //TODO: Think about a different implementation, because if the output of CMS.INFO changes or even just the names of the labels then the parsing will not work
        {
            long compression, capacity, mergedNodes, unmergedNodes, totalCompressions;
            double mergedWeight, unmergedWeight;

            compression = capacity = mergedNodes = unmergedNodes = totalCompressions = -1;
            mergedWeight = unmergedWeight = -1.0;
            
            RedisResult[] redisResults = ToArray(result);

            for (int i = 0; i < redisResults.Length; ++i)
            {
                string? label = redisResults[i++].ToString();

                switch (label)
                {
                    case "Compression":
                        compression = (long)redisResults[i];
                        break;
                    case "Capacity":
                        capacity = (long)redisResults[i];
                        break;
                    case "Merged nodes":
                        mergedNodes = (long)redisResults[i];
                        break;
                    case "Unmerged nodes":
                        unmergedNodes = (long)redisResults[i];
                        break;
                    case "Merged weight":

                        mergedWeight = (double)redisResults[i];
                        break;
                    case "Unmerged weight":
                        unmergedWeight = (double)redisResults[i];
                        break;
                    case "Total compressions":
                        totalCompressions = (long)redisResults[i];
                        break;
                }
            }

            return new TdigestInformation(compression, capacity, mergedNodes, unmergedNodes,
                                          mergedWeight, unmergedWeight, totalCompressions);
        }

        public static TimeSeriesInformation ToTimeSeriesInfo(RedisResult result)
        {
            long totalSamples = -1, memoryUsage = -1, retentionTime = -1, chunkSize = -1, chunkCount = -1;
            TimeStamp firstTimestamp = null, lastTimestamp = null;
            IReadOnlyList<TimeSeriesLabel> labels = null;
            IReadOnlyList<TimeSeriesRule> rules = null;
            string sourceKey = null;
            TsDuplicatePolicy? duplicatePolicy = null;
            RedisResult[] redisResults = (RedisResult[])result;
            for (int i = 0; i < redisResults.Length; ++i)
            {
                string label = (string)redisResults[i++];
                switch (label)
                {
                    case "totalSamples":
                        totalSamples = (long)redisResults[i];
                        break;
                    case "memoryUsage":
                        memoryUsage = (long)redisResults[i];
                        break;
                    case "retentionTime":
                        retentionTime = (long)redisResults[i];
                        break;
                    case "chunkCount":
                        chunkCount = (long)redisResults[i];
                        break;
                    case "chunkSize":
                        chunkSize = (long)redisResults[i];
                        break;
                    case "maxSamplesPerChunk":
                        // If the property name is maxSamplesPerChunk then this is an old
                        // version of RedisTimeSeries and we used the number of samples before ( now Bytes )
                        chunkSize = chunkSize * 16;
                        break;
                    case "firstTimestamp":
                        firstTimestamp = ToTimeStamp(redisResults[i]);
                        break;
                    case "lastTimestamp":
                        lastTimestamp = ToTimeStamp(redisResults[i]);
                        break;
                    case "labels":
                        labels = ToLabelArray(redisResults[i]);
                        break;
                    case "sourceKey":
                        sourceKey = (string)redisResults[i];
                        break;
                    case "rules":
                        rules = ToRuleArray(redisResults[i]);
                        break;
                    case "duplicatePolicy":
                        // Avalible for > v1.4
                        duplicatePolicy = ToPolicy(redisResults[i]);
                        break;
                }
            }

            return new TimeSeriesInformation(totalSamples, memoryUsage, firstTimestamp,
            lastTimestamp, retentionTime, chunkCount, chunkSize, labels, sourceKey, rules, duplicatePolicy);
        }

        public static IReadOnlyList<string>? ToStringArray(RedisResult result)
        {
            RedisResult[] redisResults = ToArray(result);

            var list = new List<string>();
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, str => list.Add((string)str));
            return list;
        }
    }
}