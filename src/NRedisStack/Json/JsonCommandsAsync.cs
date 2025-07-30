﻿using NRedisStack.Json.DataTypes;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace NRedisStack;

public class JsonCommandsAsync(IDatabaseAsync db) : IJsonCommandsAsync
{
    public async Task<long?[]> ArrAppendAsync(RedisKey key, string? path = null, params object[] values)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.ArrAppend(key, path, values))).ToNullableLongArray();
    }

    public async Task<long?[]> ArrIndexAsync(RedisKey key, string path, object value, long? start = null,
        long? stop = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.ArrIndex(key, path, value, start, stop)))
            .ToNullableLongArray();
    }

    public async Task<long?[]> ArrInsertAsync(RedisKey key, string path, long index, params object[] values)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.ArrInsert(key, path, index, values))).ToNullableLongArray();
    }

    public async Task<long?[]> ArrLenAsync(RedisKey key, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.ArrLen(key, path))).ToNullableLongArray();
    }

    public async Task<RedisResult[]> ArrPopAsync(RedisKey key, string? path = null, long? index = null)
    {
        RedisResult result = await db.ExecuteAsync(JsonCommandBuilder.ArrPop(key, path, index));

        return result.Resp2Type switch
        {
            ResultType.Array => (RedisResult[])result!,
            ResultType.BulkString => [result],
            _ => []
        };
    }

    public async Task<long?[]> ArrTrimAsync(RedisKey key, string path, long start, long stop) =>
        (await db.ExecuteAsync(JsonCommandBuilder.ArrTrim(key, path, start, stop))).ToNullableLongArray();

    public async Task<long> ClearAsync(RedisKey key, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.Clear(key, path))).ToLong();
    }

    public async Task<long> DelAsync(RedisKey key, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.Del(key, path))).ToLong();
    }

    public Task<long> ForgetAsync(RedisKey key, string? path = null) => DelAsync(key, path);

    public async Task<RedisResult> GetAsync(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null,
        RedisValue? space = null,
        RedisValue? path = null)
    {
        return await db.ExecuteAsync(JsonCommandBuilder.Get(key, indent, newLine, space, path));
    }

    public async Task<RedisResult> GetAsync(RedisKey key, string[] paths, RedisValue? indent = null,
        RedisValue? newLine = null,
        RedisValue? space = null)
    {
        return await db.ExecuteAsync(JsonCommandBuilder.Get(key, paths, indent, newLine, space));
    }

    public async Task<T?> GetAsync<T>(RedisKey key, string path = "$", JsonSerializerOptions? serializerOptions = null)
    {
        var res = await db.ExecuteAsync(JsonCommandBuilder.Get<T>(key, path));
        if (res.Resp2Type == ResultType.BulkString && !res.IsNull)
        {
            var arr = JsonSerializer.Deserialize<JsonArray>(res.ToString());
            if (arr?.Count > 0)
            {
                return JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(arr[0]), serializerOptions);
            }
        }

        return default;
    }

    /// <inheritdoc/>
    public async Task<IEnumerable<T?>> GetEnumerableAsync<T>(RedisKey key, string path = "$")
    {
        RedisResult res = await db.ExecuteAsync(JsonCommandBuilder.Get<T>(key, path));
        return JsonSerializer.Deserialize<IEnumerable<T>>(res.ToString())!;
    }

    public async Task<RedisResult[]> MGetAsync(RedisKey[] keys, string path)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.MGet(keys, path))).ToArray();
    }

    public async Task<double?[]> NumIncrbyAsync(RedisKey key, string path, double value)
    {
        var res = await db.ExecuteAsync(JsonCommandBuilder.NumIncrby(key, path, value));
        return JsonSerializer.Deserialize<double?[]>(res.ToString())!;
    }

    public async Task<IEnumerable<HashSet<string>>> ObjKeysAsync(RedisKey key, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.ObjKeys(key, path))).ToHashSets();
    }

    public async Task<long?[]> ObjLenAsync(RedisKey key, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.ObjLen(key, path))).ToNullableLongArray();
    }

    public async Task<RedisResult[]> RespAsync(RedisKey key, string? path = null)
    {
        RedisResult result = await db.ExecuteAsync(JsonCommandBuilder.Resp(key, path));

        if (result.IsNull)
        {
            return [];
        }

        return (RedisResult[])result!;
    }

    /// <inheritdoc/>
    public Task<bool> SetAsync(RedisKey key, RedisValue path, object obj, When when = When.Always,
        JsonSerializerOptions? serializerOptions = default)
    {
        string json = JsonSerializer.Serialize(obj, options: serializerOptions);
        return SetAsync(key, path, json, when);
    }

    public async Task<bool> SetAsync(RedisKey key, RedisValue path, RedisValue json, When when = When.Always)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.Set(key, path, json, when))).OKtoBoolean();
    }

    public async Task<bool> MSetAsync(KeyPathValue[] KeyPathValueList)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.MSet(KeyPathValueList))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> MergeAsync(RedisKey key, RedisValue path, RedisValue json)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.Merge(key, path, json))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> MergeAsync(RedisKey key, RedisValue path, object obj,
        JsonSerializerOptions? serializerOptions = default)
    {
        string json = JsonSerializer.Serialize(obj, options: serializerOptions);
        return (await db.ExecuteAsync(JsonCommandBuilder.Merge(key, path, json))).OKtoBoolean();
    }

    public async Task<bool> SetFromFileAsync(RedisKey key, RedisValue path, string filePath, When when = When.Always)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"File {filePath} not found.");
        }

        string fileContent = File.ReadAllText(filePath);
        return await SetAsync(key, path, fileContent, when);
    }

    public async Task<int> SetFromDirectoryAsync(RedisValue path, string filesPath, When when = When.Always)
    {
        int inserted = 0;
        string key;
        var files = Directory.EnumerateFiles(filesPath, "*.json");
        foreach (var filePath in files)
        {
            key = filePath.Substring(0, filePath.IndexOf(".", StringComparison.Ordinal));
            if (await SetFromFileAsync(key, path, filePath, when))
            {
                inserted++;
            }
        }

        foreach (var dirPath in Directory.EnumerateDirectories(filesPath))
        {
            inserted += await SetFromDirectoryAsync(path, dirPath, when);
        }

        return inserted;
    }

    public async Task<long?[]> StrAppendAsync(RedisKey key, string value, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.StrAppend(key, value, path))).ToNullableLongArray();
    }

    public async Task<long?[]> StrLenAsync(RedisKey key, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.StrLen(key, path))).ToNullableLongArray();
    }

    public async Task<bool?[]> ToggleAsync(RedisKey key, string? path = null)
    {
        RedisResult result = await db.ExecuteAsync(JsonCommandBuilder.Toggle(key, path));

        if (result.IsNull)
        {
            return [];
        }

        if (result.Resp2Type == ResultType.Integer)
        {
            return [(long)result == 1];
        }

        return ((RedisResult[])result!).Select(x => (bool?)((long)x == 1)).ToArray();
    }

    public async Task<JsonType[]> TypeAsync(RedisKey key, string? path = null)
    {
        RedisResult result = await db.ExecuteAsync(JsonCommandBuilder.Type(key, path));

        if (result.Resp2Type == ResultType.Array)
        {
            return ((RedisResult[])result!).Select(x => (JsonType)Enum.Parse(typeof(JsonType), x.ToString().ToUpper())).ToArray();
        }

        if (result.Resp2Type == ResultType.BulkString)
        {
            return [(JsonType)Enum.Parse(typeof(JsonType), result.ToString().ToUpper())];
        }

        return [];
    }

    public async Task<long> DebugMemoryAsync(string key, string? path = null)
    {
        return (await db.ExecuteAsync(JsonCommandBuilder.DebugMemory(key, path))).ToLong();
    }
}