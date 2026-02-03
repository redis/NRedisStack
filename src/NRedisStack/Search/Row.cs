using System.Collections;
using StackExchange.Redis;

namespace NRedisStack.Search.Aggregation;

public readonly struct Row : IEnumerable<KeyValuePair<string, RedisValue>>
{
    private readonly Dictionary<string, object> _fields;

    internal Row(Dictionary<string, object> fields)
    {
        // note: only RedisValue fields are expected and exposed, due to how AggregationResult is constructed
        _fields = fields;
    }

    public bool ContainsKey(string key) => _fields.ContainsKey(key);
    public RedisValue this[string key] => _fields.TryGetValue(key, out var result) ? (result is RedisValue ? (RedisValue)result : RedisValue.Null) : RedisValue.Null;
    public object Get(string key) => _fields.TryGetValue(key, out var result) ? result : RedisValue.Null;

    public string? GetString(string key) => _fields.TryGetValue(key, out var result) ? result.ToString() : default;
    public long GetLong(string key) => _fields.TryGetValue(key, out var result) ? (long)(RedisValue)result : default;
    public double GetDouble(string key) => _fields.TryGetValue(key, out var result) ? (double)(RedisValue)result : default;

    /// <summary>
    /// Gets the number of fields in this row.
    /// </summary>
    public int FieldCount()
    {
        // only include RedisValue fields, since nested aggregates are not supported via this API
        var count = 0;
        foreach (var field in _fields)
        {
            if (field.Value is RedisValue)
            {
                count++;
            }
        }
        return count;
    }

    /// <summary>
    /// Access the fields as a sequence of key/value pairs.
    /// </summary>
    public Enumerator GetEnumerator() => new(in this);

    public struct Enumerator : IEnumerator<KeyValuePair<string, RedisValue>>
    {
        private Dictionary<string, object>.Enumerator _enumerator;
        internal Enumerator(in Row row) => _enumerator = row._fields?.GetEnumerator() ?? default;

        /// <inheritdoc/>
        public bool MoveNext()
        {
            while (_enumerator.MoveNext())
            {
                if (_enumerator.Current.Value is RedisValue)
                {
                    return true;
                }
            }

            return false;
        }

        /// <inheritdoc/>
        public KeyValuePair<string, RedisValue> Current
        {
            get
            {
                var pair = _enumerator.Current;
                return pair.Value is RedisValue value ? new(pair.Key, value) : default;
            }
        }

        void IEnumerator.Reset() => throw new NotSupportedException();
        
        object IEnumerator.Current => Current;

        void IDisposable.Dispose() => _enumerator.Dispose();
    }

    IEnumerator<KeyValuePair<string, RedisValue>> IEnumerable<KeyValuePair<string, RedisValue>>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}