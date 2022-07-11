using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace NRedisStack.Core.RedisStackCommands;

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
    public RedisResult Set(RedisKey key, string path, object obj, When when = When.Always)
    {
        string json = JsonSerializer.Serialize(obj);
        return Set(key, path, json, when);
    }

    public RedisResult Set(RedisKey key, string path, string json, When when = When.Always)
    {
        switch (when)
        {
            case When.Exists:
                return _db.Execute("JSON.SET", key, path, json, "XX");
            case When.NotExists:
                return _db.Execute("JSON.SET", key, path, json, "NX");
            default:
                return _db.Execute("JSON.SET", key, path, json);
        }
    }

    public RedisResult Get(RedisKey key, string indent = "",
                                      string newLine = "", string space = "", string path = "")
    {
        List<object> subcommands = new List<object>();
        subcommands.Add(key);
        if (indent != "")
        {
            subcommands.Add("INDENT");
            subcommands.Add(indent);
        }

        if (newLine != "")
        {
            subcommands.Add("NEWLINE");
            subcommands.Add(newLine);
        }

        if (space != "")
        {
            subcommands.Add("SPACE");
            subcommands.Add(space);
        }

        if (path != "")
        {
            subcommands.Add(path);
        }
        return _db.Execute("JSON.GET", subcommands.ToArray());
    }
}