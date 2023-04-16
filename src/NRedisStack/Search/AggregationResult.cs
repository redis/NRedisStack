using StackExchange.Redis;

namespace NRedisStack.Search.Aggregation
{
    public sealed class AggregationResult
    {
        public long TotalResults { get; }
        private readonly Dictionary<string, RedisValue>[] _results;
        public long CursorId { get; }


        internal AggregationResult(RedisResult result, long cursorId = -1)
        {
            var arr = (RedisResult[])result;

            // the first element is always the number of results
            TotalResults = (long)arr[0];

            _results = new Dictionary<string, RedisValue>[arr.Length - 1];
            for (int i = 1; i < arr.Length; i++)
            {
                var raw = (RedisResult[])arr[i];
                var cur = new Dictionary<string, RedisValue>();
                for (int j = 0; j < raw.Length;)
                {
                    var key = (string)raw[j++];
                    var val = raw[j++];
                    if (val.Type == ResultType.MultiBulk)
                        continue; // TODO: handle multi-bulk (maybe change to object?)
                    cur.Add(key, (RedisValue)val);
                }
                _results[i - 1] = cur;
            }

            CursorId = cursorId;
        }
        public IReadOnlyList<Dictionary<string, RedisValue>> GetResults() => _results;

        public Dictionary<string, RedisValue> this[int index]
            => index >= _results.Length ? null : _results[index];

        public Row GetRow(int index)
        {
            if (index >= _results.Length) return default;
            return new Row(_results[index]);
        }
    }
}