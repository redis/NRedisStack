using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace NRedisStack.Core;

public static class RedisJsonCommands
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
    public static RedisResult JsonSet(this IDatabase db, RedisKey key, string path, object obj, When when = When.Always)
    {
        string json = JsonSerializer.Serialize(obj);
        return JsonSet(db, key, path, json, when);
    }

    public static RedisResult JsonSet(this IDatabase db, RedisKey key, string path, string json, When when = When.Always)
    {
        switch(when)
        {
            case When.Exists:
                return db.Execute("JSON.SET", key, path, json, "XX");
            case When.NotExists:
                return db.Execute("JSON.SET", key, path, json, "NX");
            default:
                return db.Execute("JSON.SET", key, path, json);
        }
    }

    public static RedisResult JsonGet(this IDatabase db, RedisKey key)
    {
        return db.Execute("JSON.GET", key);
    }
}