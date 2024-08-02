using NRedisStack.Search.Aggregation;
using StackExchange.Redis;

namespace NRedisStack.Search;

public sealed class AggregationResult
{
    public long TotalResults { get; }
    private readonly Dictionary<string, RedisValue>[] _results;
    public long CursorId { get; }


    internal AggregationResult(RedisResult result, long cursorId = -1)
    {
        var arr = (RedisResult[])result!;

        //  this statement below is not true as explained in the document https://redis.io/docs/latest/commands/ft.aggregate/#return
        // // the first element is always the number of results
        // TotalResults = (long)arr[0];

        _results = new Dictionary<string, RedisValue>[arr.Length - 1];
        for (int i = 1; i < arr.Length; i++)
        {
            var raw = (RedisResult[])arr[i]!;
            var cur = new Dictionary<string, RedisValue>();
            for (int j = 0; j < raw.Length;)
            {
                var key = (string)raw[j++]!;
                var val = raw[j++];
                if (val.Type == ResultType.MultiBulk)
                    continue; // TODO: handle multi-bulk (maybe change to object?)
                cur.Add(key, (RedisValue)val);
            }

            _results[i - 1] = cur;
        }
        TotalResults = _results.Length;
        CursorId = cursorId;
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
    private object ConvertMultiBulkToObject(IEnumerable<RedisResult> multiBulkArray)
    {
        return multiBulkArray.Select(item => item.Type == ResultType.MultiBulk
                ? ConvertMultiBulkToObject((RedisResult[])item!)
                : item)
            .ToList();
    }

    public IReadOnlyList<Dictionary<string, RedisValue>> GetResults() => _results;

    public Dictionary<string, RedisValue>? this[int index]
        => index >= _results.Length ? null : _results[index];

    public Row GetRow(int index)
    {
        return index >= _results.Length ? default : new Row(_results[index]);
    }
}