using System.Diagnostics;
using System.Text;
using System.Text.Json;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using NRedisStack.Extensions;
using StackExchange.Redis;
using NRedisStack.Bloom.DataTypes;
using NRedisStack.Core.DataTypes;
using NRedisStack.CuckooFilter.DataTypes;
using NRedisStack.CountMinSketch.DataTypes;
using NRedisStack.TopK.DataTypes;
using NRedisStack.Tdigest.DataTypes;
using NRedisStack.Search;

namespace NRedisStack;

internal static class ResponseParser
{
    public static bool OKtoBoolean(this RedisResult result)
    {
        return result.ToString() == "OK";
    }

    public static bool[] ToBooleanArray(this RedisResult result)
    {
        RedisResult[] redisResults = result.ToArray();

        bool[] boolArr = new bool[redisResults.Length];
        for (int i = 0; i < redisResults.Length; i++)
        {
            boolArr[i] = redisResults[i].ToString() == "1";
        }

        return boolArr;
    }

    public static RedisResult[] ToArray(this RedisResult result)
    {
        var redisResults = (RedisResult[]?)result;
        if (redisResults != null)
            return redisResults;
        throw new ArgumentNullException(nameof(redisResults));
    }

    public static long ToLong(this RedisResult result)
    {
        if ((long?)result == null)
            throw new ArgumentNullException(nameof(result));
        return (long)result;
    }

    public static double ToDouble(this RedisResult result)
    {
        if (result.ToString() == "nan")
            return double.NaN;
        if ((double?)result == null)
            throw new ArgumentNullException(nameof(result));
        return (double)result;
    }

    public static double[] ToDoubleArray(this RedisResult result)
    {
        List<double> redisResults = [];
        foreach (var res in result.ToArray())
        {
            redisResults.Add(ToDouble(res));
        }

        return redisResults.ToArray();
    }

    public static long[] ToLongArray(this RedisResult result)
    {
        List<long> redisResults = [];
        foreach (var res in result.ToArray())
        {
            redisResults.Add(ToLong(res));
        }

        return redisResults.ToArray();
    }

    public static TimeStamp ToTimeStamp(this RedisResult result)
    {
        if (result.Resp2Type == ResultType.None) return null!;
        return new((long)result);
    }

    public static RedisKey ToRedisKey(this RedisResult result)
    {
        return new(result.ToString());
    }

    public static RedisValue ToRedisValue(this RedisResult result)
    {
        return new(result.ToString());
    }

    public static IReadOnlyList<TimeStamp> ToTimeStampArray(this RedisResult result)
    {
        RedisResult[] redisResults = (RedisResult[])result!;
        var list = new List<TimeStamp>(redisResults.Length);
        if (redisResults.Length == 0) return list;
        Array.ForEach(redisResults, timestamp => list.Add(ToTimeStamp(timestamp)));
        return list;
    }

    public static TimeSeriesTuple ToTimeSeriesTuple(this RedisResult result)
    {
        RedisResult[] redisResults = result.ToArray();
        if (redisResults.Length == 0) return null!;
        return new(ToTimeStamp(redisResults[0]), (double)redisResults[1]);
    }

    public static Tuple<long, byte[]> ToScanDumpTuple(this RedisResult result)
    {
        RedisResult[] redisResults = result.ToArray();
        if (redisResults == null || redisResults.Length == 0) return null!;
        return new((long)redisResults[0], (byte[])redisResults[1]!);
    }

    // TODO: check if this is needed:
    // public static HashEntry ToHashEntry(this RedisResult result)
    // {
    //     RedisResult[] redisResults = result.ToArray();
    //     if (redisResults.Length < 2)
    //         throw new ArgumentOutOfRangeException(nameof(result));

    //     return new HashEntry((RedisValue)(redisResults[0]), ((RedisValue)redisResults[1]));
    // }

    // public static HashEntry[] ToHashEntryArray(this RedisResult result)
    // {
    //     RedisResult[] redisResults = result.ToArray();

    //     var hash = new HashEntry[redisResults.Length / 2];
    //     if (redisResults.Length == 0) return hash;

    //     for (int i = 0; i < redisResults.Length - 1; i += 2)
    //         hash[i / 2] = new HashEntry(((RedisValue)redisResults[i]), ((RedisValue)redisResults[i + 1]));
    //     return hash;
    // }

    public static IReadOnlyList<TimeSeriesTuple> ToTimeSeriesTupleArray(this RedisResult result)
    {
        RedisResult[] redisResults = (RedisResult[])result!;
        var list = new List<TimeSeriesTuple>(redisResults.Length);
        if (redisResults.Length == 0) return list;
        Array.ForEach(redisResults, tuple => list.Add(ToTimeSeriesTuple(tuple)));
        return list;
    }

    public static List<TimeSeriesLabel> ToLabelArray(this RedisResult result)
    {
        if (result.Resp3Type is ResultType.Map) // RESP3; single map
        {
            var dict = ((RedisResult[])result!);
            var list = new List<TimeSeriesLabel>(dict.Length / 2);
            for (int i = 0; i < dict.Length; i += 2)
            {
                list.Add(new(dict[i].ToString(), dict[i + 1].ToString()));
            }
            return list;
        }
        else // jagged/nested array pairs
        {
            RedisResult[] redisResults = (RedisResult[])result!;
            var list = new List<TimeSeriesLabel>(redisResults.Length);
            if (redisResults.Length == 0) return list;
            Array.ForEach(redisResults, labelResult =>
            {
                var labelTuple = (RedisResult[])labelResult!;
                list.Add(new(labelTuple[0].ToString(), labelTuple[1].ToString()));
            });
            return list;
        }
    }

    // public static IReadOnlyList<TimeSeriesCunck> ToCunckArray(this RedisResult result)
    // {
    //     RedisResult[] redisResults = (RedisResult[])result;
    //     var list = new List<TimeSeriesCunck>(redisResults.Length);
    //     if (redisResults.Length == 0) return list;
    //     Array.ForEach(redisResults, chunckResult =>
    //     {
    //         RedisResult[] labelTuple = (RedisResult[])labelResult;
    //         list.Add(new TimeSeriesCunck((string)labelTuple[0], (string)labelTuple[1]));
    //     });
    //     return list;
    // }

    public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple value)> ParseMGetResponse(this RedisResult result)
    {
        var redisResults = (RedisResult[])result!;
        List<(string key, IReadOnlyList<TimeSeriesLabel> labels, TimeSeriesTuple values)> list;
        if (redisResults.Length is 0)
        {
            list = new(0);
        }
        else if (result.Resp3Type is ResultType.Map) // RESP3, map
        {
            list = new(redisResults.Length / 2);
            for (int i = 0; i < redisResults.Length; i += 2)
            {
                string key = redisResults[i].ToString();
                var pair = (RedisResult[])redisResults[i + 1]!;
                var labels = ToLabelArray(pair[0]);
                TimeSeriesTuple? value = ToTimeSeriesTuple(pair[1]);
                list.Add((key, labels, value));
            }
        }
        else // jagged
        {
            list = new(redisResults.Length);
            for (int i = 0; i < redisResults.Length; i++)
            {
                var triple = (RedisResult[])redisResults[i]!;
                string key = triple[0].ToString();
                IReadOnlyList<TimeSeriesLabel> labels = ToLabelArray(triple[1]);
                TimeSeriesTuple? value = ToTimeSeriesTuple(triple[2]);
                list.Add((key, labels, value));
            }
        }

        return list;
    }

    public static IReadOnlyList<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> ParseMRangeResponse(this RedisResult result)
    {
        var redisResults = (RedisResult[])result!;
        List<(string key, IReadOnlyList<TimeSeriesLabel> labels, IReadOnlyList<TimeSeriesTuple> values)> list;
        if (redisResults.Length is 0)
        {
            list = [];
        }
        else if (result.Resp3Type is ResultType.Map) // RESP3
        {
            // jagged array of [key, [labels, [aggregators, ] [groupings, ] values]]
            list = new(redisResults.Length / 2);
            for (int i = 0; i < redisResults.Length; i += 2)
            {
                string key = redisResults[i].ToString();
                var tuple = (RedisResult[])redisResults[i + 1]!;
                var labels = ToLabelArray(tuple[0]);
                // we choose to spoof RESP2-style labels from the additional RESP3 metadata, for consistency
                for (int j = 1; j < tuple.Length - 1; j++)
                {
                    if (tuple[j].Resp3Type is ResultType.Map)
                    {
                        var map = (RedisResult[])tuple[j]!;
                        for (int k = 0; k + 1 < map.Length; k += 2)
                        {
                            var metadataKey = map[k].ToString();
                            var value = map[k + 1];
                            switch (metadataKey)
                            {
                                case "group":
                                    labels.Add(new("group", CommaDelimit(value)));
                                    break;
                                case "reducers" when value.Resp3Type is ResultType.Array:
                                    labels.Add(new("__reducer__", CommaDelimit(value)));
                                    break;
                                case "sources" when value.Resp3Type is ResultType.Array:
                                    labels.Add(new("__source__", CommaDelimit(value)));
                                    break;
                            }
                        }
                    }
                }
                // take values from the array
                var values = ToTimeSeriesTupleArray(tuple[tuple.Length - 1]);
                list.Add((key, labels, values));
            }
        }
        else
        {
            // jagged array of [key, labels, values], where each value is [timestamp, value]
            list = new(redisResults.Length);
            for (int i = 0; i < redisResults.Length; i++)
            {
                var tuple = (RedisResult[])redisResults[i]!;
                string key = tuple[0].ToString();
                IReadOnlyList<TimeSeriesLabel> labels = ToLabelArray(tuple[1]);
                IReadOnlyList<TimeSeriesTuple> values = ToTimeSeriesTupleArray(tuple[2]);
                list.Add((key, labels, values));
            }
        }

        return list;
    }

    private static string CommaDelimit(RedisResult value)
    {
        if (value.IsNull) return null!;
        if (value.Length < 0) return value.ToString();
        switch (value.Length)
        {
            case 0: return "";
            case 1: return $"{value[0]}";
            case 2: return $"{value[0]},{value[1]}";
            case 3: return $"{value[0]},{value[1]},{value[2]}";
            case 4: return $"{value[0]},{value[1]},{value[2]},{value[3]}";
            case 5: return $"{value[0]},{value[1]},{value[2]},{value[3]},{value[4]}";
            case 6: return $"{value[0]},{value[1]},{value[2]},{value[3]},{value[4]},{value[5]}";
            default:
                var sb = new StringBuilder();
                for (int i = 0; i < value.Length; i++)
                {
                    if (i != 0) sb.Append(',');
                    sb.Append(value[i]);
                }
                return sb.ToString();
        }
    }

    public static TimeSeriesRule ToRule(this RedisResult key, ReadOnlySpan<RedisResult> values)
    {
        string destKey = key.ToString();
        long bucketTime = (long)values[0];
        var aggregation = AggregationExtensions.AsAggregation(values[1].ToString());
        return new(destKey, bucketTime, aggregation);
    }

    public static IReadOnlyList<TimeSeriesRule> ToRuleArray(this RedisResult result)
    {
        var redisResults = (RedisResult[])result!;
        List<TimeSeriesRule> list;
        if (redisResults.Length == 0)
        {
            list = new(0);
        }
        else if (result.Resp3Type is ResultType.Map) // RESP3
        {
            list = new(redisResults.Length / 2);
            for (int i = 0; i + 1 < redisResults.Length; i += 2)
            {
                list.Add(ToRule(redisResults[i], redisResults[i + 1].ToArray()));
            }
        }
        else
        {
            list = new List<TimeSeriesRule>(redisResults.Length);
            foreach (var rule in redisResults)
            {
                var values = (RedisResult[])rule!;
                list.Add(ToRule(values[0], values.AsSpan(1)));
            }
        }
        return list;
    }

    public static TsDuplicatePolicy? ToPolicy(this RedisResult result)
    {
        var policyStatus = result.ToString();
        if (String.IsNullOrEmpty(policyStatus) || policyStatus == "(nil)")
        {
            return null;
        }

        return DuplicatePolicyExtensions.AsPolicy(policyStatus.ToUpper());
    }

    public static BloomInformation ToBloomInfo(this RedisResult result) //TODO: Think about a different implementation, because if the output of BF.INFO changes or even just the names of the labels then the parsing will not work
    {
        long capacity, size, numberOfFilters, numberOfItemsInserted, expansionRate;
        capacity = size = numberOfFilters = numberOfItemsInserted = expansionRate = -1;
        RedisResult[] redisResults = result.ToArray();

        for (int i = 0; i < redisResults.Length; ++i)
        {
            string? label = redisResults[i++].ToString();
            // string.Compare(label, "Capacity", true)
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

        return new(capacity, size, numberOfFilters, numberOfItemsInserted, expansionRate);
    }

    public static CuckooInformation ToCuckooInfo(this RedisResult result) //TODO: Think about a different implementation, because if the output of BF.INFO changes or even just the names of the labels then the parsing will not work
    {
        long size, numberOfBuckets, numberOfFilters, numberOfItemsInserted,
            numberOfItemsDeleted, bucketSize, expansionRate, maxIterations;

        size = numberOfBuckets = numberOfFilters =
            numberOfItemsInserted = numberOfItemsDeleted =
                bucketSize = expansionRate = maxIterations = -1;

        RedisResult[] redisResults = result.ToArray();

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
                case "Number of filters":
                    numberOfFilters = (long)redisResults[i];
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
                case "Max iterations":
                    maxIterations = (long)redisResults[i];
                    break;
            }
        }

        return new(size, numberOfBuckets, numberOfFilters, numberOfItemsInserted,
            numberOfItemsDeleted, bucketSize, expansionRate, maxIterations);
    }

    public static CmsInformation ToCmsInfo(this RedisResult result) //TODO: Think about a different implementation, because if the output of CMS.INFO changes or even just the names of the labels then the parsing will not work
    {
        long width, depth, count;

        width = depth = count = -1;

        RedisResult[] redisResults = result.ToArray();

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

        return new(width, depth, count);
    }

    public static TopKInformation ToTopKInfo(this RedisResult result) //TODO: Think about a different implementation, because if the output of CMS.INFO changes or even just the names of the labels then the parsing will not work
    {
        long k, width, depth;
        double decay;

        k = width = depth = -1;
        decay = -1.0;

        RedisResult[] redisResults = result.ToArray();

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

        return new(k, width, depth, decay);
    }

    public static TdigestInformation ToTdigestInfo(this RedisResult result) //TODO: Think about a different implementation, because if the output of CMS.INFO changes or even just the names of the labels then the parsing will not work
    {
        long compression, capacity, mergedNodes, unmergedNodes, totalCompressions, memoryUsage;
        double mergedWeight, unmergedWeight, observations;

        compression = capacity = mergedNodes = unmergedNodes = totalCompressions = memoryUsage = -1;
        mergedWeight = unmergedWeight = observations = -1.0;

        RedisResult[] redisResults = result.ToArray();

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
                case "Observations":
                    observations = (double)redisResults[i];
                    break;
                case "Total compressions":
                    totalCompressions = (long)redisResults[i];
                    break;
                case "Memory usage":
                    memoryUsage = (long)redisResults[i];
                    break;
            }
        }

        return new(compression, capacity, mergedNodes, unmergedNodes,
            mergedWeight, unmergedWeight, observations, totalCompressions, memoryUsage);
    }

    [Obsolete]
    public static TimeSeriesInformation ToTimeSeriesInfo(this RedisResult result)
    {
        long totalSamples = -1, memoryUsage = -1, retentionTime = -1, chunkSize = -1, chunkCount = -1;
        TimeStamp? firstTimestamp = null, lastTimestamp = null;
        IReadOnlyList<TimeSeriesLabel>? labels = null;
        IReadOnlyList<TimeSeriesRule>? rules = null;
        string? sourceKey = null, keySelfName = null;
        TsDuplicatePolicy? duplicatePolicy = null;
        IReadOnlyList<TimeSeriesChunck>? chunks = null;
        var redisResults = (RedisResult[])result!;
        for (int i = 0; i < redisResults.Length; ++i)
        {
            string label = redisResults[i++].ToString();
            try
            {
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
                    // case "maxSamplesPerChunk":
                    //     // If the property name is maxSamplesPerChunk then this is an old
                    //     // version of RedisTimeSeries and we used the number of samples before ( now Bytes )
                    //     chunkSize = chunkSize * 16;
                    //     break;
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
                        sourceKey = redisResults[i].ToString();
                        break;
                    case "rules":
                        rules = ToRuleArray(redisResults[i]);
                        break;
                    case "duplicatePolicy":
                        // Avalible for > v1.4
                        duplicatePolicy = ToPolicy(redisResults[i]);
                        break;
                    case "keySelfName":
                        // Avalible for > v1.4
                        keySelfName = redisResults[i].ToString();
                        break;
                    case "Chunks":
                        // Avalible for > v1.4
                        chunks = ToTimeSeriesChunkArray(redisResults[i]);
                        break;
                }
            }
            catch (Exception e)
            {
                throw new InvalidOperationException($"{e.GetType().Name} parsing '{label}': {e.Message}", e);
            }
        }

        return new(totalSamples, memoryUsage, firstTimestamp,
            lastTimestamp, retentionTime, chunkCount, chunkSize, labels, sourceKey, rules, duplicatePolicy, keySelfName, chunks);
    }

    // TODO: check if this is needed
    // public static Dictionary<string, RedisValue> ToFtInfoAsDictionary(this RedisResult value)
    // {
    //     var res = (RedisResult[])value;
    //     var info = new Dictionary<string, RedisValue>();
    //     for (int i = 0; i < res.Length; i += 2)
    //     {
    //         var val = res[i + 1];
    //         if (val.Type != ResultType.Array)
    //         {
    //             info.Add((string)res[i], (RedisValue)val);
    //         }
    //     }
    //     return info;
    // }

    public static Dictionary<string, string> ToConfigDictionary(this RedisResult value)
    {
        Dictionary<string, string> dict;
        if (value.Length is 0)
        {
            dict = new(0);
        }
        else if (value.Resp3Type is ResultType.Map)
        {
            // RESP3: map
            dict = new(value.Length / 2);
            for (int i = 0; i + 1 < value.Length; i += 2)
            {
                dict.Add(value[i].ToString(), value[i + 1].ToString());
            }
        }
        else
        {
            // RESP2: jagged; [ [key, value] ]
            dict = new(value.Length);
            for (int i = 0 ; i < value.Length ; i += 2)
            {
                var inner = value[i];
                dict.Add(inner[0].ToString(), inner[1].ToString());
            }
        }
        return dict;
    }

    public static IReadOnlyList<TimeSeriesChunck> ToTimeSeriesChunkArray(this RedisResult result)
    {
        var redisResults = (RedisResult[])result!;
        var list = new List<TimeSeriesChunck>();
        if (redisResults.Length == 0) return list;
        Array.ForEach(redisResults, chunk => list.Add(ToTimeSeriesChunk(chunk)));
        return list;
    }

    public static TimeSeriesChunck ToTimeSeriesChunk(this RedisResult result)
    {
        long startTimestamp = -1, endTimestamp = -1, samples = -1, size = -1;
        string bytesPerSample = "";
        var redisResults = (RedisResult[])result!;
        for (int i = 0; i < redisResults.Length; ++i)
        {
            string label = redisResults[i++].ToString();
            switch (label)
            {
                case "startTimestamp":
                    startTimestamp = (long)redisResults[i];
                    break;
                case "endTimestamp":
                    endTimestamp = (long)redisResults[i];
                    break;
                case "samples":
                    samples = (long)redisResults[i];
                    break;
                case "size":
                    size = (long)redisResults[i];
                    break;
                case "bytesPerSample":
                    bytesPerSample = redisResults[i].ToString();
                    break;
            }
        }

        return new(startTimestamp, endTimestamp,
            samples, size, bytesPerSample);

    }

    public static List<string> ToStringList(this RedisResult result)
    {
        RedisResult[] redisResults = result.ToArray();

        var list = new List<string>();
        if (redisResults.Length == 0) return list;
        Array.ForEach(redisResults, str => list.Add(str.ToString()));
        return list;
    }

    public static long?[] ToNullableLongArray(this RedisResult result)
    {
        if (result.IsNull)
        {
            return [];
        }

        if (result.Resp2Type == ResultType.Integer)
        {
            return [(long?)result];
        }

        return ((RedisResult[])result!).Select(x => (long?)x).ToArray();
    }

    public static IEnumerable<HashSet<string>> ToHashSets(this RedisResult result)
    {
        if (result.IsNull)
        {
            return [];
        }

        var res = (RedisResult[])result!;
        var sets = new List<HashSet<string>>();
        if (res.All(x => x.Resp2Type != ResultType.Array))
        {
            var keys = res.Select(x => x.ToString());
            sets.Add([.. keys]);
            return sets;
        }

        foreach (var arr in res)
        {
            var set = new HashSet<string>();
            if (arr.Resp2Type == ResultType.Array)
            {
                var resultArr = (RedisResult[])arr!;
                foreach (var item in resultArr)
                {
                    set.Add(item.ToString());
                }
            }
            sets.Add(set);
        }

        return sets;
    }

    public static Dictionary<string, Dictionary<string, double>> ToFtSpellCheckResult(this RedisResult result)
    {
        var outerArr = (RedisResult[])result!;
        if (result.Resp3Type is ResultType.Map)
        {
            // RESP3 has different nesting, and the score/suggestionj are inverted
            for (int i = 0; i + 1 < outerArr.Length; i += 2)
            {
                if (outerArr[i].ToString() == "results" && outerArr[i + 1] is { Resp3Type: ResultType.Map } results)
                {
                    var innerArr = (RedisResult[])results!;
                    var returnTerms = new Dictionary<string, Dictionary<string, double>>(innerArr.Length / 2);
                    for (int j = 0; j < innerArr.Length; j += 2)
                    {
                        var term = innerArr[j].ToString();
                        var suggestions = (RedisResult[])innerArr[j + 1]!;
                        var entries = new Dictionary<string, double>(suggestions.Length);
                        foreach (var entry in suggestions)
                        {
                            if (entry.Resp3Type is ResultType.Map)
                            {
                                for (int k = 0; k + 1 < entry.Length; k += 2)
                                {
                                    var suggestion = entry[k].ToString();
                                    var score = (double)entry[k + 1];
                                    entries.Add(suggestion, score);
                                }
                            }
                        }
                        returnTerms.Add(term, entries);
                    }
                    return returnTerms; // we can skip any other elements
                }
            }

            return []; // didn't find any results
        }
        else
        {
            var returnTerms = new Dictionary<string, Dictionary<string, double>>(outerArr.Length);
            foreach (var term in outerArr)
            {
                var rawElements = (RedisResult[])term!;

                string termValue = rawElements[1].ToString();

                var list = (RedisResult[])rawElements[2]!;
                Dictionary<string, double> entries = new(list.Length);
                foreach (var entry in list)
                {
                    var entryElements = (RedisResult[])entry!;
                    string suggestion = entryElements[1].ToString();
                    double score = (double)entryElements[0];
                    entries.Add(suggestion, score);
                }

                returnTerms.Add(termValue, entries);
            }

            return returnTerms;
        }
    }

    public static List<Tuple<string, double>> ToStringDoubleTupleList(this RedisResult result) // TODO: consider create class Suggestion instead of List<Tuple<string, double>>
    {
        var results = (RedisResult[])result!;
        var list = new List<Tuple<string, double>>(results.Length / 2);
        for (int i = 0; i < results.Length; i += 2)
        {
            var suggestion = results[i].ToString();
            var score = (double)results[i + 1];
            list.Add(new(suggestion, score));
        }
        return list;
    }

    public static Dictionary<string, RedisResult> ToStringRedisResultDictionary(this RedisResult value)
    {
        var res = (RedisResult[])value!;
        var dict = new Dictionary<string, RedisResult>();
        foreach (var pair in res)
        {
            var arr = (RedisResult[])pair!;
            if (arr.Length > 1)
            {
                dict.Add(arr[0].ToString(), arr[1]);
            }
            else
            {
                dict.Add(arr[0].ToString(), null!);
            }
        }
        return dict;
    }

    public static Tuple<SearchResult, Dictionary<string, RedisResult>> ToProfileSearchResult(this RedisResult result, Query q)
    {
        var results = (RedisResult[])result!;

        var searchResult = results[0].ToSearchResult(q);
        var profile = results[1].ToStringRedisResultDictionary();
        return new(searchResult, profile);
    }

    public static Tuple<SearchResult, ProfilingInformation> ParseProfileSearchResult(this RedisResult result, Query q)
    {
        var results = (RedisResult[])result!;

        var searchResult = results[0].ToSearchResult(q);
        var profile = new ProfilingInformation(results[1]);
        return new(searchResult, profile);
    }

    public static SearchResult ToSearchResult(this RedisResult result, Query q)
    {
        return new(result, !q.NoContent, q.WithScores, q.WithPayloads/*, q.ExplainScore*/);
    }

    public static Tuple<AggregationResult, Dictionary<string, RedisResult>> ToProfileAggregateResult(this RedisResult result, AggregationRequest q)
    {
        var results = (RedisResult[])result!;
        var aggregateResult = results[0].ToAggregationResult(q);
        var profile = results[1].ToStringRedisResultDictionary();
        return new(aggregateResult, profile);
    }

    public static Tuple<AggregationResult, ProfilingInformation> ParseProfileAggregateResult(this RedisResult result, AggregationRequest q)
    {
        var results = (RedisResult[])result!;
        var aggregateResult = results[0].ToAggregationResult(q);
        var profile = new ProfilingInformation(results[1]);
        return new(aggregateResult, profile);
    }

    public static AggregationResult ToAggregationResult(this RedisResult result, AggregationRequest query)
    {
        if (query.IsWithCursor())
        {
            var results = (RedisResult[])result!;

            return new(results[0], (long)results[1]);
        }
        else
        {
            return new(result);
        }
    }

    internal static AggregationResult ToAggregationResult(this RedisResult result, string indexName, AggregationRequest query, IServer? server, int? database)
    {
        if (query.IsWithCursor())
        {
            var results = (RedisResult[])result!;

            return new AggregationResult.WithCursorAggregationResult(indexName, results[0], (long)results[1], server, database);
        }
        else
        {
            return new(result);
        }
    }

    public static Dictionary<string, RedisResult>[] ToDictionarys(this RedisResult result)
    {
        var resArr = (RedisResult[])result!;
        var dicts = new Dictionary<string, RedisResult>[resArr.Length];
        for (int i = 0; i < resArr.Length; i++)
        {
            dicts[i] = resArr[i].ToDictionary();
        }

        return dicts;

    }

    public static Tuple<RedisKey, RedisValueWithScore>? ToSortedSetPopResult(this RedisResult result)
    {
        if (result.IsNull)
        {
            return null;
        }

        var resultArray = (RedisResult[])result!;
        var resultKey = resultArray[0].ToRedisKey();
        var value = resultArray[1].ToRedisValue();
        var score = resultArray[2].ToDouble();
        var valuesWithScores = new RedisValueWithScore(value, score);

        return new(resultKey, valuesWithScores);
    }

    public static Tuple<RedisKey, List<RedisValueWithScore>>? ToSortedSetPopResults(this RedisResult result)
    {
        if (result.IsNull)
        {
            return null;
        }

        var resultArray = (RedisResult[])result!;
        var resultKey = resultArray[0].ToRedisKey();
        var resultSetItems = resultArray[1].ToArray();

        List<RedisValueWithScore> valuesWithScores = [];

        foreach (var resultSetItem in resultSetItems)
        {
            var resultSetItemArray = (RedisResult[])resultSetItem!;
            var value = resultSetItemArray[0].ToRedisValue();
            var score = resultSetItemArray[1].ToDouble();
            valuesWithScores.Add(new(value, score));
        }

        return new(resultKey, valuesWithScores);
    }

    public static Tuple<RedisKey, RedisValue>? ToListPopResult(this RedisResult result)
    {
        if (result.IsNull)
        {
            return null;
        }

        var resultArray = (RedisResult[])result!;
        var resultKey = resultArray[0].ToRedisKey();
        var value = resultArray[1].ToRedisValue();

        return new(resultKey, value);
    }

    public static Tuple<RedisKey, List<RedisValue>>? ToListPopResults(this RedisResult result)
    {
        if (result.IsNull)
        {
            return null;
        }

        var resultArray = (RedisResult[])result!;
        var resultKey = resultArray[0].ToRedisKey();
        var resultSetItems = resultArray[1].ToArray();

        List<RedisValue> values = [];

        foreach (var resultSetItem in resultSetItems)
        {
            var value = (RedisValue)resultSetItem!;
            values.Add(value);
        }

        return new(resultKey, values);
    }

    public static RedisStreamEntries[]? ToRedisStreamEntries(this RedisResult result)
    {
        if (result.IsNull)
        {
            return null;
        }

        var resultArray = (RedisResult[])result!;
        RedisStreamEntries[] redisStreamEntries;
        if (result.Resp3Type is ResultType.Map) // RESP3
        {
            redisStreamEntries = new RedisStreamEntries[resultArray.Length / 2];
            for (int i = 0; i + 1 < resultArray.Length; i += 2)
            {
                RedisKey streamKey = resultArray[i].ToRedisKey();
                StreamEntry[] streamEntries = ParseStreamEntries((RedisResult[])resultArray[i + 1]!);
                redisStreamEntries[i / 2] = new(streamKey, streamEntries);
            }
        }
        else
        {
            redisStreamEntries = new RedisStreamEntries[resultArray.Length];
            for (int i = 0; i < resultArray.Length; i++)
            {
                RedisResult[] streamResultArray = (RedisResult[])resultArray[i]!;
                RedisKey streamKey = streamResultArray[0].ToRedisKey();
                StreamEntry[] streamEntries = ParseStreamEntries(streamResultArray[1].ToArray());
                redisStreamEntries[i] = new(streamKey, streamEntries);
            }
        }

        return redisStreamEntries;
    }

    private static StreamEntry[] ParseStreamEntries(IReadOnlyList<RedisResult> results)
    {
        int count = results.Count;
        StreamEntry[] streamEntries = new StreamEntry[count];

        for (int i = 0; i < count; i++)
        {
            RedisResult[] streamEntryArray = (RedisResult[])results[i]!;
            RedisValue key = streamEntryArray[0].ToRedisValue();
            NameValueEntry[] nameValueEntries = ParseNameValueEntries(streamEntryArray[1].ToArray());
            streamEntries[i] = new(key, nameValueEntries);
        }

        return streamEntries;
    }

    private static NameValueEntry[] ParseNameValueEntries(IReadOnlyList<RedisResult> redisResults)
    {
        int count = redisResults.Count / 2;
        var nameValueEntries = new NameValueEntry[count];

        for (int i = 0; i < count; i++)
        {
            nameValueEntries[i] = new(
                redisResults[2 * i].ToRedisValue(),
                redisResults[2 * i + 1].ToRedisValue());
        }

        return nameValueEntries;
    }

    internal static T[] ParseSearchResultsMap<T>(RedisResult result, Func<string[], RedisResult[], T> reader, out long totalResults)
    {
        // common code for parsing a map with attributes, results, total_results
        var arr = (RedisResult[])result!;
        string[] attributes = [];
        T[] results = [];
        totalResults = 0;
        for (int i = 0; i + 1 < arr.Length; i += 2)
        {
            var key = (string)arr[i]!;
            var value = arr[i + 1];
            switch (key)
            {
                // we're relying on this coming back before "results"; if this ever starts failing, I guess
                // we can just move this to a separate pass
                case "attributes" when value.Resp3Type is ResultType.Array:
                    attributes = Array.ConvertAll((RedisResult[])value!, x => (string)x!);
                    break;
                case "total_results" when value.Resp3Type is ResultType.Integer:
                    totalResults = (long)arr[i + 1]!;
                    break;
                case "results" when value.Resp3Type is ResultType.Array:
                    var rawResults = (RedisResult[])value!;
                    results = new T[rawResults.Length];
                    for (int j = 0; j < rawResults.Length; j++)
                    {
                        results[j] = rawResults[j].Resp3Type is ResultType.Map
                            ? reader(attributes, (RedisResult[])rawResults[j]!)
                            : default(T)!;
                    }
                    break;
            }
        }
        return results;
    }

    internal static JsonType[] ParseJsonTypeArray(RedisResult result)
    {
        // RESP3 adds a layer of wrapping
        return result is { Resp3Type: ResultType.Array, Length: 1 } && result[0].Resp3Type is ResultType.Array
            ? ParseArray(result[0]) : ParseArray(result);

#if NET // modern .NET; not NS or NETFX
        static JsonType ParseType(RedisResult x) => Enum.Parse<JsonType>(x.ToString(), ignoreCase: true);
#else
        static JsonType ParseType(RedisResult x) => (JsonType)Enum.Parse(typeof(JsonType), x.ToString(), ignoreCase: true);
#endif
        static JsonType[] ParseArray(RedisResult result) // flexible, handles single values and arrays
        {
            switch (result.Resp2Type)
            {
                case ResultType.Array:
                    return Array.ConvertAll((RedisResult[])result!, ParseType);
                case ResultType.BulkString:
                    return [ParseType(result)];
                default:
                    return [];
            }
        }
    }

    public static double?[] ParseJsonDoubleArray(RedisResult res)
    {
        if (res.IsNull) return null!;
        if (res.Resp3Type is ResultType.Array)
        {
            // in RESP3, sent as an array of numbers
            if (res.Length == 0) return [];
            double?[] ret = new double?[res.Length];
            for (int i = 0; i < res.Length; i++)
            {
                ret[i] = res[i].IsNull ? null : (double)res[i];
            }
            return ret;
        }
        // in RESP2: sent as a JSON stringified array
        return JsonSerializer.Deserialize<double?[]>(res.ToString())!;
    }
}