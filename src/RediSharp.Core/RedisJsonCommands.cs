using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Serialization;
namespace RediSharp.Core;

public static class RedisJsonCommands
{
    private static readonly JsonSerializerOptions Options = new()
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };
    public static RedisResult JsonSet(this IDatabase db, RedisKey key, string path, object obj, When when = When.Always)
    {
        var json = JsonSerializer.Serialize(obj);
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
}