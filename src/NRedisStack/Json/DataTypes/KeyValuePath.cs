using System.Text.Json;

namespace NRedisStack.Json.DataTypes;

public struct KeyValuePath
{
    public string Key { get; set; }
    public object Value { get; set; }
    public string Path { get; set; }

    public KeyValuePath(string key, object value, string path = "$")
    {
        if (key == null || value == null)
        {
            throw new ArgumentNullException("Key and value cannot be null.");
        }

        Key = key;
        Value = value;
        Path = path;
    }
    public string[] ToArray()
    {
        if (Value is string)
        {
            return new string[] { Key, Path, Value.ToString() };
        }
        return new string[] { Key, Path, JsonSerializer.Serialize(Value) };
    }
}