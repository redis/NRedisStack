using NRedisStack.Core.Literals;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace NRedisStack.Core;

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
    public RedisResult Set(RedisKey key, RedisValue path, object obj, When when = When.Always)
    {
        string json = JsonSerializer.Serialize(obj);
        return Set(key, path, json, when);
    }

    public RedisResult Set(RedisKey key, RedisValue path, RedisValue json, When when = When.Always)
    {
        switch (when)
        {
            case When.Exists:
                return _db.Execute(JSON.SET, key, path, json, "XX");
            case When.NotExists:
                return _db.Execute(JSON.SET, key, path, json, "NX");
            default:
                return _db.Execute(JSON.SET, key, path, json);
        }
    }

    public RedisResult Get(RedisKey key,
                           RedisValue? indent = null,
                           RedisValue? newLine = null,
                           RedisValue? space = null,
                           RedisValue? path = null)
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

        if (path != null)
        {
            args.Add(path);
        }

        return _db.Execute(JSON.GET, args);
    }
}