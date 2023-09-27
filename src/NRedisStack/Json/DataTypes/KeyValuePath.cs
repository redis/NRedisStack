using System.Text.Json;

namespace NRedisStack.Json.DataTypes;

public struct KeyPathValue
{
    public string Key { get; set; }
    public string Path { get; set; }
    public object Value { get; set; }

    public KeyPathValue(string key, string path, object value)
    {
        Key = key;
        Path = path;
        Value = value;
    }
    public string[] ToArray()
    {
        if (Value is string)
        {
            return new string[] { Key, Path, Value.ToString()! };
        }
        return new string[] { Key, Path, JsonSerializer.Serialize(Value) };
    }
}