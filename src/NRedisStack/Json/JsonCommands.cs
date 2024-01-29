using NRedisStack.Json.DataTypes;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace NRedisStack;

public class JsonCommands(IDatabase db) : JsonCommandsAsync(db), IJsonCommands
{
    /// <inheritdoc/>
    public RedisResult[] Resp(RedisKey key, string? path = null)
    {
        RedisResult result = db.Execute(JsonCommandBuilder.Resp(key, path));

        if (result.IsNull)
        {
            return Array.Empty<RedisResult>();
        }

        return (RedisResult[])result!;
    }

    /// <inheritdoc/>
    public bool Set(RedisKey key, RedisValue path, object obj, When when = When.Always,
        JsonSerializerOptions? serializerOptions = default)
    {
        string json = JsonSerializer.Serialize(obj, options: serializerOptions);
        return Set(key, path, json, when);
    }

    /// <inheritdoc/>
    public bool Set(RedisKey key, RedisValue path, RedisValue json, When when = When.Always)
    {
        return db.Execute(JsonCommandBuilder.Set(key, path, json, when)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool MSet(KeyPathValue[] KeyPathValueList)
    {
        return db.Execute(JsonCommandBuilder.MSet(KeyPathValueList)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Merge(RedisKey key, RedisValue path, RedisValue json)
    {
        return db.Execute(JsonCommandBuilder.Merge(key, path, json)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Merge(RedisKey key, RedisValue path, object obj, JsonSerializerOptions? serializerOptions = default)
    {
        string json = JsonSerializer.Serialize(obj, options: serializerOptions);
        return db.Execute(JsonCommandBuilder.Merge(key, path, json)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool SetFromFile(RedisKey key, RedisValue path, string filePath, When when = When.Always)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"File {filePath} not found.");
        }

        string fileContent = File.ReadAllText(filePath);
        return Set(key, path, fileContent, when);
    }

    /// <inheritdoc/>
    public int SetFromDirectory(RedisValue path, string filesPath, When when = When.Always)
    {
        int inserted = 0;
        string key;
        var files = Directory.EnumerateFiles(filesPath, "*.json");
        foreach (var filePath in files)
        {
            key = filePath.Substring(0, filePath.IndexOf("."));
            if (SetFromFile(key, path, filePath, when))
            {
                inserted++;
            }
        }

        inserted += Directory.EnumerateDirectories(filesPath).Sum(dirPath => SetFromDirectory(path, dirPath, when));

        return inserted;
    }

    /// <inheritdoc/>
    public long?[] StrAppend(RedisKey key, string value, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.StrAppend(key, value, path)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] StrLen(RedisKey key, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.StrLen(key, path)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public bool?[] Toggle(RedisKey key, string? path = null)
    {
        RedisResult result = db.Execute(JsonCommandBuilder.Toggle(key, path));

        if (result.IsNull)
        {
            return Array.Empty<bool?>();
        }

        return result.Type == ResultType.Integer
            ? [(long)result == 1]
            : ((RedisResult[])result!).Select(x => (bool?)((long)x == 1)).ToArray();
    }

    /// <inheritdoc/>
    public JsonType[] Type(RedisKey key, string? path = null)
    {
        RedisResult result = db.Execute(JsonCommandBuilder.Type(key, path));

        return result.Type switch
        {
            ResultType.MultiBulk => ((RedisResult[])result!)
                .Select(x => (JsonType)Enum.Parse(typeof(JsonType), x.ToString()!.ToUpper()))
                .ToArray(),
            ResultType.BulkString => [(JsonType)Enum.Parse(typeof(JsonType), result.ToString()!.ToUpper())],
            _ => Array.Empty<JsonType>()
        };
    }

    public long DebugMemory(string key, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.DebugMemory(key, path)).ToLong();
    }

    /// <inheritdoc/>
    public long?[] ArrAppend(RedisKey key, string? path = null, params object[] values)
    {
        return db.Execute(JsonCommandBuilder.ArrAppend(key, path, values)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrIndex(RedisKey key, string path, object value, long? start = null, long? stop = null)
    {
        return db.Execute(JsonCommandBuilder.ArrIndex(key, path, value, start, stop)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrInsert(RedisKey key, string path, long index, params object[] values)
    {
        return db.Execute(JsonCommandBuilder.ArrInsert(key, path, index, values)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrLen(RedisKey key, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.ArrLen(key, path)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public RedisResult[] ArrPop(RedisKey key, string? path = null, long? index = null)
    {
        RedisResult result = db.Execute(JsonCommandBuilder.ArrPop(key, path, index));

        if (result.Type == ResultType.MultiBulk)
        {
            return (RedisResult[])result!;
        }

        return result.Type == ResultType.BulkString ? [result] : Array.Empty<RedisResult>();
    }

    /// <inheritdoc/>
    public long?[] ArrTrim(RedisKey key, string path, long start, long stop) =>
        db.Execute(JsonCommandBuilder.ArrTrim(key, path, start, stop)).ToNullableLongArray();

    /// <inheritdoc/>
    public long Clear(RedisKey key, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.Clear(key, path)).ToLong();
    }

    /// <inheritdoc/>
    public long Del(RedisKey key, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.Del(key, path)).ToLong();
    }

    /// <inheritdoc/>
    public long Forget(RedisKey key, string? path = null) => Del(key, path);

    /// <inheritdoc/>
    public RedisResult Get(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null,
        RedisValue? space = null, RedisValue? path = null)
    {
        return db.Execute(JsonCommandBuilder.Get(key, indent, newLine, space, path));
    }

    /// <inheritdoc/>
    public RedisResult Get(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null,
        RedisValue? space = null)
    {
        return db.Execute(JsonCommandBuilder.Get(key, paths, indent, newLine, space));
    }

    /// <inheritdoc/>
    public T? Get<T>(RedisKey key, string path = "$", JsonSerializerOptions? serializerOptions = default)
    {
        var res = db.Execute(JsonCommandBuilder.Get<T>(key, path));
        if (res.Type != ResultType.BulkString || res.IsNull) return default;
        var arr = JsonSerializer.Deserialize<JsonArray>(res.ToString()!);
        return arr?.Count > 0 ? JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(arr[0]), serializerOptions) : default;
    }

    /// <inheritdoc/>
    public IEnumerable<T?> GetEnumerable<T>(RedisKey key, string path = "$")
    {
        RedisResult res = db.Execute(JsonCommandBuilder.Get<T>(key, path));
        return JsonSerializer.Deserialize<IEnumerable<T>>(res.ToString()!)!;
    }

    /// <inheritdoc/>
    public RedisResult[] MGet(RedisKey[] keys, string path)
    {
        return db.Execute(JsonCommandBuilder.MGet(keys, path)).ToArray();
    }

    /// <inheritdoc/>
    public double?[] NumIncrby(RedisKey key, string path, double value)
    {
        var res = db.Execute(JsonCommandBuilder.NumIncrby(key, path, value));
        return JsonSerializer.Deserialize<double?[]>(res.ToString()!)!;
    }

    /// <inheritdoc/>
    public IEnumerable<HashSet<string>> ObjKeys(RedisKey key, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.ObjKeys(key, path)).ToHashSets();
    }

    /// <inheritdoc/>
    public long?[] ObjLen(RedisKey key, string? path = null)
    {
        return db.Execute(JsonCommandBuilder.ObjLen(key, path)).ToNullableLongArray();
    }
}