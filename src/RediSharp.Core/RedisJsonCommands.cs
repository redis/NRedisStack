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
    public static RedisResult JsonSet(this IDatabase db, string key, string path, object obj)
    {
        var json = JsonSerializer.Serialize(obj);
        var result = db.Execute("JSON.SET", key, path, json);
        return result;
    }
}