using StackExchange.Redis;

namespace NRedisStack.Search.Aggregation;

public readonly struct Row
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
}