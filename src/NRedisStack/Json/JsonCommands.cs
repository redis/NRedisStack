using NRedisStack.Literals;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace NRedisStack;

public class JsonCommands
{
    IDatabase _db;
    public JsonCommands(IDatabase db)
    {
        _db = db;
    }
    private readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };


    /// <summary>
    /// Sets the JSON value at path in key.
    /// </summary>
    /// <param name="key">The key of the Json.</param>
    /// <param name="path">Json path.</param>
    /// <param name="obj">Json object.</param>
    /// <param name="item">The item to add.</param>
    /// <returns><see langword="true"/> if executed correctly, or Null reply if the specified NX or XX conditions were not met.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    public bool? Set(RedisKey key, string path, object obj, When when = When.Always)
    {
        string json = JsonSerializer.Serialize(obj);
        return Set(key, path, json, when);
    }

    /// <summary>
    /// Sets the JSON value at path in key.
    /// </summary>
    /// <param name="key">The key of the Json.</param>
    /// <param name="path">Json path.</param>
    /// <param name="json">Json object.</param>
    /// <param name="item">The item to add.</param>
    /// <returns><see langword="true"/> if executed correctly, or Null reply if the specified NX or XX conditions were not met.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.set"/></remarks>
    public bool? Set(RedisKey key, string path, string json, When when = When.Always)
    {
        RedisResult result;
        switch (when)
        {
            case When.Exists:
                result = _db.Execute(JSON.SET, key, path, json, "XX");
                break;
            case When.NotExists:
                result = _db.Execute(JSON.SET, key, path, json, "NX");
                break;
            default:
                result = _db.Execute(JSON.SET, key, path, json);
                break;
        }
        return (result.IsNull) ? null
                               : ResponseParser.OKtoBoolean(result);

    }

    /// <summary>
    /// Returns the value at path in JSON serialized form.
    /// </summary>
    /// <param name="key">The key of the Json.</param>
    /// <param name="indent">Sets the indentation string for nested levels</param>
    /// <param name="newLine">Sets the string that's printed at the end of each line.</param>
    /// <param name="space">Sets the string that's put between a key and a value.</param>
    /// <param name="paths">Paths in JSON serialized form</param>
    /// <returns><see langword="true"/> if executed correctly, or Null reply if the specified NX or XX conditions were not met.</returns>
    /// <remarks><seealso href="https://redis.io/commands/json.get"/></remarks>
    public RedisResult Get(RedisKey key,
                           RedisValue? indent = null,
                           RedisValue? newLine = null,
                           RedisValue? space = null,
                           RedisValue[]? paths = null)
    {

        List<object> args = new List<object>(){key};

        if (indent != null)
        {
            args.Add(JsonArgs.INDENT);
            args.Add(indent);
        }

        if (newLine != null)
        {
            args.Add(JsonArgs.NEWLINE);
            args.Add(newLine);
        }

        if (space != null)
        {
            args.Add(JsonArgs.SPACE);
            args.Add(space);
        }

        if (paths != null)
        {
            foreach (var path in paths)
            {
                args.Add(path);
            }
        }

        return _db.Execute(JSON.GET, args);
    }
}