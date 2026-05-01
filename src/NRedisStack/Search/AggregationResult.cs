using System.Diagnostics;
using NRedisStack.Search.Aggregation;
using StackExchange.Redis;

namespace NRedisStack.Search;

public class AggregationResult
{
    // internal subclass for WITHCURSOR calls, which need to be issued to the same connection
    internal sealed class WithCursorAggregationResult : AggregationResult
    {
        internal WithCursorAggregationResult(string indexName, RedisResult result, long cursorId, IServer? server,
            int? database) : base(result, cursorId)
        {
            IndexName = indexName;
            Server = server;
            Database = database;
        }
        public string IndexName { get; }
        public IServer? Server { get; }
        public int? Database { get; }
    }

    public long TotalResults { get; }
    private readonly Dictionary<string, object>[] _results;
    private Dictionary<string, RedisValue>[]? _resultsAsRedisValues;

    public long CursorId { get; }

    internal AggregationResult(RedisResult result, long cursorId = -1)
    {
        var arr = (RedisResult[])result!;
        Dictionary<string, object>[]? results = null;
        if (result.Resp3Type is ResultType.Map)
        {
            results = ResponseParser.ParseSearchResultsMap(result, ParseRecordFromMap, out long totalResults);
            TotalResults = totalResults;
        }
        else
        {
            //  this statement below is not true as explained in the document https://redis.io/docs/latest/commands/ft.aggregate/#return
            // // the first element is always the number of results
            // TotalResults = (long)arr[0];

            results = new Dictionary<string, object>[arr.Length - 1];
            for (int i = 1; i < arr.Length; i++)
            {
                var raw = (RedisResult[])arr[i]!;
                var cur = new Dictionary<string, object>();
                for (int j = 0; j < raw.Length;)
                {
                    var key = (string)raw[j++]!;
                    var val = raw[j++];
                    cur.Add(key, ParseFieldValue(val));
                }

                results[i - 1] = cur;
            }
            TotalResults = results.Length;
        }
        CursorId = cursorId;
        _results = results ?? []; // if we didn't get results, make an empty array

        static object ParseFieldValue(RedisResult val) => val.Resp2Type switch
        {
            ResultType.Array => ConvertMultiBulkToObject((RedisResult[])val!),
            _ => (RedisValue)val
        };

        static Dictionary<string, object> ParseRecordFromMap(string[] attributes, RedisResult[] map)
        {
            var record = new Dictionary<string, object>();
            for (int i = 0; i + 1 < map.Length; i += 2)
            {
                var key = (string)map[i]!;
                var val = map[i+1];
                switch (key)
                {
                    case "values" when val.Resp3Type is ResultType.Array:
                        var values = (RedisResult[])val!;
                        for (int j = 0; j < values.Length && j < attributes.Length; j++)
                        {
                            record.Add(attributes[j], ParseFieldValue(values[j]));
                        }
                        break;
                    case "extra_attributes" when val.Resp3Type is ResultType.Map:
                        var extraAttributes = (RedisResult[])val!;
                        for (int j = 0; j + 1 < extraAttributes.Length; j += 2)
                        {
                            record.Add((string)extraAttributes[j]!, ParseFieldValue(extraAttributes[j + 1]));
                        }
                        break;

                }
            }
            return record;
        }
    }

    /// <summary>
    /// takes a Redis multi-bulk array represented by a RedisResult[] and recursively processes its elements.
    /// For each element in the array, it checks if it's another multi-bulk array, and if so, it recursively calls itself.
    /// If the element is not a multi-bulk array, it's added directly to a List&lt;object&gt;.
    /// The method returns a nested list structure, reflecting the hierarchy of the original multi-bulk array,
    /// with each element either being a direct value or a nested list.
    /// </summary>
    /// <param name="multiBulkArray"></param>
    /// <returns>object</returns>
    private static object ConvertMultiBulkToObject(IEnumerable<RedisResult> multiBulkArray)
    {
        return multiBulkArray.Select(item => item.Resp2Type == ResultType.Array
                ? ConvertMultiBulkToObject((RedisResult[])item!)
                : (RedisValue)item)
            .ToList();
    }

    /// <summary>
    /// Gets the results as a read-only list of dictionaries with string keys and RedisValue values.
    /// </summary>
    /// <remarks>
    /// This method is deprecated and will be removed in future versions. 
    /// Please use <see cref="GetRow"/> instead.
    /// </remarks>
    [Obsolete("This method is deprecated and will be removed in future versions. Please use 'GetRow' instead.")]
    public IReadOnlyList<Dictionary<string, RedisValue>> GetResults()
    {
        return GetResultsAsRedisValues();
    }

    /// <summary>
    /// Gets the aggregation result at the specified index.
    /// </summary>
    /// <param name="index">The zero-based index of the aggregation result to retrieve.</param>
    /// <returns>
    /// A dictionary containing the aggregation result as Redis values if the index is within bounds;
    /// otherwise, <c>null</c>.
    /// </returns>
    [Obsolete("This method is deprecated and will be removed in future versions. Please use 'GetRow' instead.")]
    public Dictionary<string, RedisValue>? this[int index]
   => index >= GetResultsAsRedisValues().Length ? null : GetResultsAsRedisValues()[index];

    public Row GetRow(int index)
    {
        return index >= _results.Length ? default : new Row(_results[index]);
    }

    private Dictionary<string, RedisValue>[] GetResultsAsRedisValues()
    {
        return _resultsAsRedisValues ??= _results.Select(dict => dict.ToDictionary(
            kvp => kvp.Key,
            kvp => kvp.Value is RedisValue value ? value : RedisValue.Null
        )).ToArray();
    }
}