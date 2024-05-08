using StackExchange.Redis;

namespace NRedisStack.Search.DataTypes;

public class InfoResult
{
    private readonly Dictionary<string, RedisResult> _all = new();
    public string IndexName => GetString("index_name")!;
    public Dictionary<string, RedisResult> IndexOption => GetRedisResultDictionary("index_options")!;
    public RedisResult[][] Attributes => GetRedisResultArrayArray("attributes")!;
    public long NumDocs => GetLong("num_docs");
    public string MaxDocId => GetString("max_doc_id")!;
    public long NumTerms => GetLong("num_terms");
    public long NumRecords => GetLong("num_records");
    public double InvertedSzMebibytes => GetDouble("inverted_sz_mb");
    public double VectorIndexSzMebibytes => GetDouble("vector_index_sz_mb"); // TODO: check if double or long

    public double TotalInvertedIndexBlocks => GetDouble("total_inverted_index_blocks");

    // public double InvertedCapOvh => GetDouble("inverted_cap_ovh");
    public double OffsetVectorsSzMebibytes => GetDouble("offset_vectors_sz_mb");
    public double DocTableSizeMebibytes => GetDouble("doc_table_size_mb");
    public double SortableValueSizeMebibytes => GetDouble("sortable_value_size_mb");

    public double KeyTableSizeMebibytes => GetDouble("key_table_size_mb");

    // public double SkipIndexSizeMebibytes => GetDouble("skip_index_size_mb");

    // public double ScoreIndexSizeMebibytes => GetDouble("score_index_size_mb");

    public double RecordsPerDocAvg => GetDouble("records_per_doc_avg");

    public double BytesPerRecordAvg => GetDouble("bytes_per_record_avg");

    public double OffsetsPerTermAvg => GetDouble("offsets_per_term_avg");

    public double OffsetBitsPerRecordAvg => GetDouble("offset_bits_per_record_avg");

    public long HashIndexingFailures => GetLong("hash_indexing_failures");

    public double TotalIndexingTime => GetDouble("total_indexing_time");

    public long Indexing => GetLong("indexing");

    public double PercentIndexed => GetDouble("percent_indexed");

    public long NumberOfUses => GetLong("number_of_uses");


    public Dictionary<string, RedisResult> GcStats => GetRedisResultDictionary("gc_stats")!;

    public Dictionary<string, RedisResult> CursorStats => GetRedisResultDictionary("cursor_stats")!;

    public InfoResult(RedisResult result)
    {
        var results = (RedisResult[])result!;

        for (var i = 0; i < results.Length; i += 2)
        {
            var key = (string)results[i]!;
            var value = results[i + 1];

            _all.Add(key, value);
        }
    }

    private string? GetString(string key) => _all.TryGetValue(key, out var value) ? (string)value! : default;

    private long GetLong(string key) => _all.TryGetValue(key, out var value) ? (long)value : default;

    private double GetDouble(string key)
    {
        if (!_all.TryGetValue(key, out var value)) return default;
        if ((string)value! == "-nan")
        {
            return default;
        }

        return (double)value;
    }

    private Dictionary<string, RedisResult>? GetRedisResultDictionary(string key)
    {
        if (!_all.TryGetValue(key, out var value)) return default;
        var values = (RedisResult[])value!;
        var result = new Dictionary<string, RedisResult>();

        for (var ii = 0; ii < values.Length; ii += 2)
        {
            result.Add((string)values[ii]!, values[ii + 1]);
        }

        return result;
    }

    private RedisResult[][]? GetRedisResultArrayArray(string key)
    {
        if (!_all.TryGetValue(key, out var value)) return default;
        return value.ToArrayArray();
    }
}