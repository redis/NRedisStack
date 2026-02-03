using System.Collections;
using StackExchange.Redis;

namespace NRedisStack.Search.Aggregation;

public readonly struct Row : IEnumerable<KeyValuePair<string, object>>
{
    private readonly Dictionary<string, object> _fields;

    internal Row(Dictionary<string, object> fields)
    {
        _fields = fields;
    }

    public bool ContainsKey(string key) => _fields.ContainsKey(key);
    public RedisValue this[string key] => _fields.TryGetValue(key, out var result) ? (result is RedisValue ? (RedisValue)result : RedisValue.Null) : RedisValue.Null;
    public object Get(string key) => _fields.TryGetValue(key, out var result) ? result : RedisValue.Null;

    public string? GetString(string key) => _fields.TryGetValue(key, out var result) ? result.ToString() : default;
    public long GetLong(string key) => _fields.TryGetValue(key, out var result) ? (long)(RedisValue)result : default;
    public double GetDouble(string key) => _fields.TryGetValue(key, out var result) ? (double)(RedisValue)result : default;

    /// <summary>
    /// Access the fields as a sequence of key/value pairs.
    /// </summary>
    public Enumerator GetEnumerator() => new(in this);

    public struct Enumerator : IEnumerator<KeyValuePair<string, object>>
    {
        private Dictionary<string, object>.Enumerator _enumerator;
        internal Enumerator(in Row row) => _enumerator = row._fields?.GetEnumerator() ?? default;

        /// <inheritdoc/>
        public bool MoveNext() => _enumerator.MoveNext();

        /// <inheritdoc/>
        public KeyValuePair<string, object> Current => _enumerator.Current;

        void IEnumerator.Reset() => throw new NotSupportedException();
        
        object IEnumerator.Current => Current;

        void IDisposable.Dispose() => _enumerator.Dispose();
    }

    IEnumerator<KeyValuePair<string, object>> IEnumerable<KeyValuePair<string, object>>.GetEnumerator() => GetEnumerator();
    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}