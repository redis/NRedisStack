﻿using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Nodes;

namespace NRedisStack;

public class JsonCommands : IJsonCommands
{
    IDatabase _db;
    public JsonCommands(IDatabase db)
    {
        _db = db;
    }

    /// <inheritdoc/>
    public RedisResult[] Resp(RedisKey key, string? path = null)
    {
        RedisResult result = _db.Execute(JsonCommandBuilder.Resp(key, path));

        if (result.IsNull)
        {
            return Array.Empty<RedisResult>();
        }

        return (RedisResult[])result!;
    }

    /// <inheritdoc/>
    public bool Set(RedisKey key, RedisValue path, object obj, When when = When.Always)
    {
        string json = JsonSerializer.Serialize(obj);
        return Set(key, path, json, when);
    }

    /// <inheritdoc/>
    public bool Set(RedisKey key, RedisValue path, RedisValue json, When when = When.Always)
    {
        return _db.Execute(JsonCommandBuilder.Set(key, path, json, when)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool SetFromFile(RedisKey key, RedisValue path, string filePath, When when = When.Always)
    {
        if(!File.Exists(filePath))
        {
            throw new FileNotFoundException($"File {filePath} not found.");
        }

        string fileContent  = File.ReadAllText(filePath);
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
            if(SetFromFile(key, path, filePath, when))
            {
                inserted++;
            }
        }

        foreach (var dirPath in Directory.EnumerateDirectories(filesPath))
        {
            inserted += SetFromDirectory(path, dirPath, when);
        }

        return inserted;
    }

    /// <inheritdoc/>
    public long?[] StrAppend(RedisKey key, string value, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.StrAppend(key, value, path)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] StrLen(RedisKey key, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.StrLen(key, path)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public bool?[] Toggle(RedisKey key, string? path = "$")
    {
        RedisResult result = _db.Execute(JsonCommandBuilder.Toggle(key, path!));

        if (result.IsNull)
        {
            return Array.Empty<bool?>();
        }

        if (result.Type == ResultType.Integer)
        {
            return new bool?[] { (long)result == 1 };
        }

        return ((RedisResult[])result!).Select(x => (bool?)((long)x == 1)).ToArray();
    }

    /// <inheritdoc/>
    public JsonType[] Type(RedisKey key, string? path = null)
    {
        RedisResult result = _db.Execute(JsonCommandBuilder.Type(key, path));

        if (result.Type == ResultType.MultiBulk)
        {
            return ((RedisResult[])result!).Select(x => Enum.Parse<JsonType>(x.ToString()!.ToUpper())).ToArray();
        }

        if (result.Type == ResultType.BulkString)
        {
            return new[] { Enum.Parse<JsonType>(result.ToString()!.ToUpper()) };
        }

        return Array.Empty<JsonType>();

    }

    public long DebugMemory(string key, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.DebugMemory(key, path)).ToLong();
    }

    public async Task<long?[]> ArrAppendAsync(RedisKey key, string? path = null, params object[] values)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.ArrAppend(key, path, values))).ToNullableLongArray();
    }

    public async Task<long?[]> ArrIndexAsync(RedisKey key, string path, object value, long? start = null, long? stop = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.ArrIndex(key, path, value, start, stop))).ToNullableLongArray();
    }

    public async Task<long?[]> ArrInsertAsync(RedisKey key, string path, long index, params object[] values)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.ArrInsert(key, path, index, values))).ToNullableLongArray();
    }

    public async Task<long?[]> ArrLenAsync(RedisKey key, string? path = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.ArrLen(key, path))).ToNullableLongArray();
    }

    public async Task<RedisResult[]> ArrPopAsync(RedisKey key, string? path = null, long? index = null)
    {
        RedisResult result = await _db.ExecuteAsync(JsonCommandBuilder.ArrPop(key, path, index));

        if (result.Type == ResultType.MultiBulk)
        {
            return (RedisResult[])result!;
        }

        if (result.Type == ResultType.BulkString)
        {
            return new[] { result };
        }

        return Array.Empty<RedisResult>();
    }

    public async Task<long?[]> ArrTrimAsync(RedisKey key, string path, long start, long stop) =>
        (await _db.ExecuteAsync(JsonCommandBuilder.ArrTrim(key, path, start, stop))).ToNullableLongArray();

    public async Task<long> ClearAsync(RedisKey key, string? path = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.Clear(key, path))).ToLong();
    }

    public async Task<long> DelAsync(RedisKey key, string? path = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.Del(key, path))).ToLong();
    }

    public Task<long> ForgetAsync(RedisKey key, string? path = null) => DelAsync(key, path);

    public async Task<RedisResult> GetAsync(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null,
        RedisValue? path = null)
    {
        return await _db.ExecuteAsync(JsonCommandBuilder.Get(key, indent, newLine, space, path));
    }

    public async Task<RedisResult> GetAsync(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null,
        RedisValue? space = null)
    {
        return await _db.ExecuteAsync(JsonCommandBuilder.Get(key, paths, indent, newLine, space));
    }

    public async Task<T?> GetAsync<T>(RedisKey key, string path = "$")
    {
        var res = await _db.ExecuteAsync(JsonCommandBuilder.Get<T>(key, path));
        if (res.Type == ResultType.BulkString)
        {
            var arr = JsonSerializer.Deserialize<JsonArray>(res.ToString()!);
            if (arr?.Count > 0)
            {
                return JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(arr[0]));
            }
        }

        return default;
    }

    public Task<IEnumerable<T?>> GetEnumerableAsync<T>(RedisKey key, string path = "$") // TODO: why is this here?
    {
        throw new NotImplementedException();
    }

    public async Task<RedisResult[]> MGetAsync(RedisKey[] keys, string path)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.MGet(keys, path))).ToArray();
    }

    public async Task<double?[]> NumIncrbyAsync(RedisKey key, string path, double value)
    {
        var res = await _db.ExecuteAsync(JsonCommandBuilder.NumIncrby(key, path, value));
        return JsonSerializer.Deserialize<double?[]>(res.ToString());
    }

    public async Task<IEnumerable<HashSet<string>>> ObjKeysAsync(RedisKey key, string? path = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.ObjKeys(key, path))).ToHashSets();
    }

    public async Task<long?[]> ObjLenAsync(RedisKey key, string? path = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.ObjLen(key, path))).ToNullableLongArray();
    }

    public async Task<RedisResult[]> RespAsync(RedisKey key, string? path = null)
    {
        RedisResult result = await _db.ExecuteAsync(JsonCommandBuilder.Resp(key, path));

        if (result.IsNull)
        {
            return Array.Empty<RedisResult>();
        }

        return (RedisResult[])result!;
    }

    public Task<bool> SetAsync(RedisKey key, RedisValue path, object obj, When when = When.Always)
    {
        string json = JsonSerializer.Serialize(obj);
        return SetAsync(key, path, json, when);
    }

    public async Task<bool> SetAsync(RedisKey key, RedisValue path, RedisValue json, When when = When.Always)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.Set(key, path, json, when))).OKtoBoolean();
    }

    /// <inheritdoc/> // TODO: check way asnyc methods dont have documenation
    public async Task<bool> SetFromFileAsync(RedisKey key, RedisValue path, string filePath, When when = When.Always)
    {
        if (!File.Exists(filePath))
        {
            throw new FileNotFoundException($"File {filePath} not found.");
        }

        string fileContent  = File.ReadAllText(filePath);
        return await SetAsync(key, path, fileContent, when);
    }

    /// <inheritdoc/>
    public async Task<int> SetFromDirectoryAsync(RedisValue path, string filesPath, When when = When.Always)
    {
        int inserted = 0;
        string key;
        var files = Directory.EnumerateFiles(filesPath, "*.json");
        foreach (var filePath in files)
        {
            key = filePath.Substring(0, filePath.IndexOf("."));
            if(await SetFromFileAsync(key, path, filePath, when))
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
        return (await _db.ExecuteAsync(JsonCommandBuilder.StrAppend(key, value, path))).ToNullableLongArray();
    }

    public async Task<long?[]> StrLenAsync(RedisKey key, string? path = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.StrLen(key, path))).ToNullableLongArray();
    }

    public async Task<bool?[]> ToggleAsync(RedisKey key, string? path = "$")
    {
        RedisResult result = await _db.ExecuteAsync(JsonCommandBuilder.Toggle(key, path!));

        if (result.IsNull)
        {
            return Array.Empty<bool?>();
        }

        if (result.Type == ResultType.Integer)
        {
            return new bool?[] { (long)result == 1 };
        }

        return ((RedisResult[])result!).Select(x => (bool?)((long)x == 1)).ToArray();
    }

    public async Task<JsonType[]> TypeAsync(RedisKey key, string? path = null)
    {
        RedisResult result = await _db.ExecuteAsync(JsonCommandBuilder.Type(key, path));

        if (result.Type == ResultType.MultiBulk)
        {
            return ((RedisResult[])result!).Select(x => Enum.Parse<JsonType>(x.ToString()!.ToUpper())).ToArray();
        }

        if (result.Type == ResultType.BulkString)
        {
            return new[] { Enum.Parse<JsonType>(result.ToString()!.ToUpper()) };
        }

        return Array.Empty<JsonType>();
    }

    public async Task<long> DebugMemoryAsync(string key, string? path = null)
    {
        return (await _db.ExecuteAsync(JsonCommandBuilder.DebugMemory(key, path))).ToLong();
    }

    /// <inheritdoc/>
    public long?[] ArrAppend(RedisKey key, string? path = null, params object[] values)
    {
        return _db.Execute(JsonCommandBuilder.ArrAppend(key, path, values)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrIndex(RedisKey key, string path, object value, long? start = null, long? stop = null)
    {
        return _db.Execute(JsonCommandBuilder.ArrIndex(key, path, value, start, stop)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrInsert(RedisKey key, string path, long index, params object[] values)
    {
        return _db.Execute(JsonCommandBuilder.ArrInsert(key, path, index, values)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrLen(RedisKey key, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.ArrLen(key, path)).ToNullableLongArray();
    }

    /// <inheritdoc/>
    public RedisResult[] ArrPop(RedisKey key, string? path = null, long? index = null)
    {
        RedisResult result = _db.Execute(JsonCommandBuilder.ArrPop(key, path, index));

        if (result.Type == ResultType.MultiBulk)
        {
            return (RedisResult[])result!;
        }

        if (result.Type == ResultType.BulkString)
        {
            return new[] { result };
        }

        return Array.Empty<RedisResult>();
    }

    /// <inheritdoc/>
    public long?[] ArrTrim(RedisKey key, string path, long start, long stop) =>
        _db.Execute(JsonCommandBuilder.ArrTrim(key, path, start, stop)).ToNullableLongArray();

    /// <inheritdoc/>
    public long Clear(RedisKey key, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.Clear(key, path)).ToLong();
    }

    /// <inheritdoc/>
    public long Del(RedisKey key, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.Del(key, path)).ToLong();
    }

    /// <inheritdoc/>
    public long Forget(RedisKey key, string? path = null) => Del(key, path);

    /// <inheritdoc/>
    public RedisResult Get(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null, RedisValue? path = null)
    {
        return _db.Execute(JsonCommandBuilder.Get(key, indent, newLine, space, path));
    }

    /// <inheritdoc/>
    public RedisResult Get(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null)
    {
        return _db.Execute(JsonCommandBuilder.Get(key, paths, indent, newLine, space));
    }

    /// <inheritdoc/>
    public T? Get<T>(RedisKey key, string path = "$")
    {
        var res = _db.Execute(JsonCommandBuilder.Get<T>(key, path));
        if (res.Type == ResultType.BulkString)
        {
            var arr = JsonSerializer.Deserialize<JsonArray>(res.ToString()!);
            if (arr?.Count > 0)
            {
                return JsonSerializer.Deserialize<T>(JsonSerializer.Serialize(arr[0]));
            }
        }

        return default;
    }

    /// <inheritdoc/>
    public IEnumerable<T?> GetEnumerable<T>(RedisKey key, string path = "$")
    {
        RedisResult res = _db.Execute(JsonCommandBuilder.Get<T>(key, path));
        return JsonSerializer.Deserialize<IEnumerable<T>>(res.ToString());
    }

    /// <inheritdoc/>
    public RedisResult[] MGet(RedisKey[] keys, string path)
    {
        return _db.Execute(JsonCommandBuilder.MGet(keys, path)).ToArray();
    }

    /// <inheritdoc/>
    public double?[] NumIncrby(RedisKey key, string path, double value)
    {
        var res = _db.Execute(JsonCommandBuilder.NumIncrby(key, path, value));
        return JsonSerializer.Deserialize<double?[]>(res.ToString());
    }

    /// <inheritdoc/>
    public IEnumerable<HashSet<string>> ObjKeys(RedisKey key, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.ObjKeys(key, path)).ToHashSets();
    }

    /// <inheritdoc/>
    public long?[] ObjLen(RedisKey key, string? path = null)
    {
        return _db.Execute(JsonCommandBuilder.ObjLen(key, path)).ToNullableLongArray();
    }
}