using System.Text.Json;

namespace NRedisStack.Json.DataTypes;

public struct KeyPathValue(string key, string path, object value)
{
    private string Key { get; set; } = key;
    private string Path { get; set; } = path;
    private object Value { get; set; } = value;

    public IEnumerable<string> ToArray()
    {
        return Value is string ? new[] { Key, Path, Value!.ToString()! } : new[] { Key, Path, JsonSerializer.Serialize(Value) };
    }
}