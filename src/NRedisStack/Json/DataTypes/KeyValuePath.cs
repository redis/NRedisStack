using System.Text.Json;

namespace NRedisStack.Json.DataTypes;

public struct KeyValuePath
{
    public string Key { get; set; }
    public object Value { get; set; }
    public string Path { get; set; }

    public KeyValuePath(string key, object value, string path = "$")
    {
        if (!(value is string) && !(value is object))
        {
            throw new ArgumentException("Value must be a string or an object.");
        }

        if (key == null || value == null)
        {
            throw new ArgumentNullException("Key and value cannot be null.");
        }

        Key = key;
        Value = value;
        Path = path;
    }

    public override string ToString()
    {
        if (Value is string)
            return $"{Key} {Path} {Value}";
        return $"{Key} {Path} {JsonSerializer.Serialize(Value)}";
    }
}