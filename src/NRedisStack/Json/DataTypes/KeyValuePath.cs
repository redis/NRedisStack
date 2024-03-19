using System.Text.Json;

namespace NRedisStack.Json.DataTypes;

public struct KeyPathValue
{
    public KeyPathValue(string key, string path, object value)
    {
        Key = key;
        Path = path;
        Value = value;
    }

    private string Key { get; set; }
    private string Path { get; set; }
    private object Value { get; set; }

    public IEnumerable<string> ToArray()
    {
        return Value is string ? new string[] { Key, Path, Value.ToString()! } : new string[] { Key, Path, JsonSerializer.Serialize(Value) };
    }
}