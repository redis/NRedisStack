using NRedisStack.Literals;
using StackExchange.Redis;
using System.Text.Json;
using System.Text.Json.Nodes;
using static NRedisStack.Auxiliary;

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
        RedisResult result;
        if (string.IsNullOrEmpty(path))
        {
            result = _db.Execute(JSON.RESP, key);
        }
        else
        {
            result = _db.Execute(JSON.RESP, key, path);
        }

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
        var result = when switch
        {
            When.Exists => _db.Execute(JSON.SET, key, path, json, "XX"),
            When.NotExists => _db.Execute(JSON.SET, key, path, json, "NX"),
            _ => _db.Execute(JSON.SET, key, path, json)
        };

        if (result.IsNull || result.ToString() != "OK")
        {
            return false;
        }

        return true;
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
        RedisResult result;
        if (path == null)
        {
            result = _db.Execute(JSON.STRAPPEND, key, JsonSerializer.Serialize(value));
        }
        else
        {
            result = _db.Execute(JSON.STRAPPEND, key, path, JsonSerializer.Serialize(value));
        }

        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] StrLen(RedisKey key, string? path = null)
    {
        RedisResult result;
        if (path != null)
        {
            result = _db.Execute(JSON.STRLEN, key, path);
        }
        else
        {
            result = _db.Execute(JSON.STRLEN, key);
        }

        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public bool?[] Toggle(RedisKey key, string? path = null)
    {
        RedisResult result;
        if (path != null)
        {
            result = _db.Execute(JSON.TOGGLE, key, path);
        }
        else
        {
            result = _db.Execute(JSON.TOGGLE, key, "$");
        }

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
        RedisResult result;
        if (path == null)
        {
            result = _db.Execute(JSON.TYPE, key);
        }
        else
        {
            result = _db.Execute(JSON.TYPE, key, path);
        }

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
        if (path != null)
        {
            return (long)_db.Execute(JSON.DEBUG, JSON.MEMORY, key, path);
        }
        return (long)_db.Execute(JSON.DEBUG, JSON.MEMORY, key);
    }

    public async Task<long?[]> ArrAppendAsync(RedisKey key, string? path = null, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));

        var args = new List<object> { key };
        if (path != null)
        {
            args.Add(path);
        }

        args.AddRange(values.Select(x => JsonSerializer.Serialize(x)));

        var result = await _db.ExecuteAsync(JSON.ARRAPPEND, args.ToArray());
        return result.ToNullableLongArray();
    }

    public async Task<long?[]> ArrIndexAsync(RedisKey key, string path, object value, long? start = null, long? stop = null)
    {
        if (start == null && stop != null)
            throw new ArgumentException("stop cannot be defined without start");

        var args = AssembleNonNullArguments(key, path, JsonSerializer.Serialize(value), start, stop);
        var result = await _db.ExecuteAsync(JSON.ARRINDEX, args);
        return result.ToNullableLongArray();
    }

    public async Task<long?[]> ArrInsertAsync(RedisKey key, string path, long index, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));
        var args = new List<object> { key, path, index };
        foreach (var val in values)
        {
            args.Add(JsonSerializer.Serialize(val));
        }

        var result = await _db.ExecuteAsync(JSON.ARRINSERT, args);
        return result.ToNullableLongArray();
    }

    public async Task<long?[]> ArrLenAsync(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        var result = await _db.ExecuteAsync(JSON.ARRLEN, args);
        return result.ToNullableLongArray();
    }

    public async Task<RedisResult[]> ArrPopAsync(RedisKey key, string? path = null, long? index = null)
    {
        if (path == null && index != null)
            throw new ArgumentException("index cannot be defined without path");

        var args = AssembleNonNullArguments(key, path, index);
        var res = await _db.ExecuteAsync(JSON.ARRPOP, args)!;

        if (res.Type == ResultType.MultiBulk)
        {
            return (RedisResult[])res!;
        }

        if (res.Type == ResultType.BulkString)
        {
            return new[] { res };
        }

        return Array.Empty<RedisResult>();
    }

    public async Task<long?[]> ArrTrimAsync(RedisKey key, string path, long start, long stop) =>
        (await _db.ExecuteAsync(JSON.ARRTRIM, key, path, start, stop)).ToNullableLongArray();

    public async Task<long> ClearAsync(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return (long)await _db.ExecuteAsync(JSON.CLEAR, args);
    }

    public async Task<long> DelAsync(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return (long)await _db.ExecuteAsync(JSON.DEL, args);
    }

    public Task<long> ForgetAsync(RedisKey key, string? path = null) => DelAsync(key, path);

    public Task<RedisResult> GetAsync(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null,
        RedisValue? path = null)
    {
        List<object> args = new List<object>() { key };

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

        return _db.ExecuteAsync(JSON.GET, args);
    }

    public Task<RedisResult> GetAsync(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null,
        RedisValue? space = null)
    {
        List<object> args = new List<object>() { key };

        foreach (var path in paths)
        {
            args.Add(path);
        }

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

        return _db.ExecuteAsync(JSON.GET, args);
    }

    public async Task<T?> GetAsync<T>(RedisKey key, string path = "$")
    {
        var res = await _db.ExecuteAsync(JSON.GET, key, path);
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

    public Task<IEnumerable<T?>> GetEnumerableAsync<T>(RedisKey key, string path = "$")
    {
        throw new NotImplementedException();
    }

    public async Task<RedisResult[]> MGetAsync(RedisKey[] keys, string path)
    {
        var args = new List<object>();
        foreach (var key in keys)
        {
            args.Add(key);
        }

        args.Add(path);
        var res = await _db.ExecuteAsync(JSON.MGET, args);
        if (res.IsNull)
        {
            return Array.Empty<RedisResult>();
        }
        return (RedisResult[])res!;
    }

    public async Task<double?[]> NumIncrbyAsync(RedisKey key, string path, double value)
    {
        var res = await _db.ExecuteAsync(JSON.NUMINCRBY, key, path, value);
        return JsonSerializer.Deserialize<double?[]>(res.ToString());
    }

    public async Task<IEnumerable<HashSet<string>>> ObjKeysAsync(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return (await _db.ExecuteAsync(JSON.OBJKEYS, args)).ToHashSets();
    }

    public async Task<long?[]> ObjLenAsync(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return (await _db.ExecuteAsync(JSON.OBJLEN, args)).ToNullableLongArray();
    }

    public async Task<RedisResult[]> RespAsync(RedisKey key, string? path = null)
    {
        RedisResult result;
        if (string.IsNullOrEmpty(path))
        {
            result = await _db.ExecuteAsync(JSON.RESP, key);
        }
        else
        {
            result = await _db.ExecuteAsync(JSON.RESP, key, path);
        }

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
        var t = when switch
        {
            When.Exists => _db.ExecuteAsync(JSON.SET, key, path, json, "XX"),
            When.NotExists => _db.ExecuteAsync(JSON.SET, key, path, json, "NX"),
            _ => _db.ExecuteAsync(JSON.SET, key, path, json)
        };

        var result = await t;

        if (result.IsNull || result.ToString() != "OK")
        {
            return false;
        }

        return true;
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
        RedisResult result;
        if (path == null)
        {
            result = await _db.ExecuteAsync(JSON.STRAPPEND, key, JsonSerializer.Serialize(value));
        }
        else
        {
            result = await _db.ExecuteAsync(JSON.STRAPPEND, key, path, JsonSerializer.Serialize(value));
        }

        return result.ToNullableLongArray();
    }

    public async Task<long?[]> StrLenAsync(RedisKey key, string? path = null)
    {
        RedisResult result;
        if (path != null)
        {
            result = await _db.ExecuteAsync(JSON.STRLEN, key, path);
        }
        else
        {
            result = await _db.ExecuteAsync(JSON.STRLEN, key);
        }

        return result.ToNullableLongArray();
    }

    public async Task<bool?[]> ToggleAsync(RedisKey key, string? path = null)
    {
        RedisResult result;
        if (path != null)
        {
            result = await _db.ExecuteAsync(JSON.TOGGLE, key, path);
        }
        else
        {
            result = await _db.ExecuteAsync(JSON.TOGGLE, key, "$");
        }

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
        RedisResult result;
        if (path == null)
        {
            result = await _db.ExecuteAsync(JSON.TYPE, key);
        }
        else
        {
            result = await _db.ExecuteAsync(JSON.TYPE, key, path);
        }

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
        if (path != null)
        {
            return (long)await _db.ExecuteAsync(JSON.DEBUG, JSON.MEMORY, key, path);
        }
        return (long)await _db.ExecuteAsync(JSON.DEBUG, JSON.MEMORY, key);
    }

    /// <inheritdoc/>
    public long?[] ArrAppend(RedisKey key, string? path = null, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));

        var args = new List<object> { key };
        if (path != null)
        {
            args.Add(path);
        }

        args.AddRange(values.Select(x => JsonSerializer.Serialize(x)));

        var result = _db.Execute(JSON.ARRAPPEND, args.ToArray());
        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrIndex(RedisKey key, string path, object value, long? start = null, long? stop = null)
    {
        if (start == null && stop != null)
            throw new ArgumentException("stop cannot be defined without start");

        var args = AssembleNonNullArguments(key, path, JsonSerializer.Serialize(value), start, stop);
        var result = _db.Execute(JSON.ARRINDEX, args);
        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrInsert(RedisKey key, string path, long index, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));
        var args = new List<object> { key, path, index };
        foreach (var val in values)
        {
            args.Add(JsonSerializer.Serialize(val));
        }

        var result = _db.Execute(JSON.ARRINSERT, args);
        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrLen(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        var result = _db.Execute(JSON.ARRLEN, args);
        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public RedisResult[] ArrPop(RedisKey key, string? path = null, long? index = null)
    {
        if (path == null && index != null)
            throw new ArgumentException("index cannot be defined without path");

        var args = AssembleNonNullArguments(key, path, index);
        var res = _db.Execute(JSON.ARRPOP, args)!;

        if (res.Type == ResultType.MultiBulk)
        {
            return (RedisResult[])res!;
        }

        if (res.Type == ResultType.BulkString)
        {
            return new[] { res };
        }

        return Array.Empty<RedisResult>();
    }

    /// <inheritdoc/>
    public long?[] ArrTrim(RedisKey key, string path, long start, long stop) =>
        _db.Execute(JSON.ARRTRIM, key, path, start, stop).ToNullableLongArray();

    /// <inheritdoc/>
    public long Clear(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return (long)_db.Execute(JSON.CLEAR, args);
    }

    /// <inheritdoc/>
    public long Del(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return (long)_db.Execute(JSON.DEL, args);
    }

    /// <inheritdoc/>
    public long Forget(RedisKey key, string? path = null) => Del(key, path);

    /// <inheritdoc/>
    public RedisResult Get(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null, RedisValue? path = null)
    {
        List<object> args = new List<object>() { key };

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

    /// <inheritdoc/>
    public RedisResult Get(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null)
    {
        List<object> args = new List<object>() { key };

        foreach (var path in paths)
        {
            args.Add(path);
        }

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

        return _db.Execute(JSON.GET, args);
    }

    /// <inheritdoc/>
    public T? Get<T>(RedisKey key, string path = "$")
    {
        var res = _db.Execute(JSON.GET, key, path);
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
        var res = _db.Execute(JSON.GET, key, path);
        return JsonSerializer.Deserialize<IEnumerable<T>>(res.ToString());
    }

    /// <inheritdoc/>
    public RedisResult[] MGet(RedisKey[] keys, string path)
    {
        var args = new List<object>();
        foreach (var key in keys)
        {
            args.Add(key);
        }

        args.Add(path);
        return (RedisResult[])_db.Execute(JSON.MGET, args)!;
    }

    /// <inheritdoc/>
    public double?[] NumIncrby(RedisKey key, string path, double value)
    {
        var res = _db.Execute(JSON.NUMINCRBY, key, path, value);
        return JsonSerializer.Deserialize<double?[]>(res.ToString());
    }

    /// <inheritdoc/>
    public IEnumerable<HashSet<string>> ObjKeys(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return _db.Execute(JSON.OBJKEYS, args).ToHashSets();
    }

    /// <inheritdoc/>
    public long?[] ObjLen(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return _db.Execute(JSON.OBJLEN, args).ToNullableLongArray();
    }
}