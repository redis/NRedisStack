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
    public long?[] StringAppend(RedisKey key, string? path, string value)
    {
        RedisResult result;
        if (path == null)
        {
            result = _db.Execute( JSON.STRAPPEND, key, JsonSerializer.Serialize(value));
        }
        else
        {
            result = _db.Execute( JSON.STRAPPEND, key, path, JsonSerializer.Serialize(value));
        }

        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] StringLength(RedisKey key, string? path = null)
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
            return new bool?[] {(long)result == 1};
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
            return new [] {Enum.Parse<JsonType>(result.ToString()!.ToUpper())};
        }
        
        return Array.Empty<JsonType>();
        
    }

    /// <inheritdoc/>
    public long?[] ArrayAppend(RedisKey key, string? path, params object[] values)
    {
        var args = new List<object>{key};
        if (path != null)
        {
            args.Add(path);
        }
        
        args.AddRange(values.Select(x=>JsonSerializer.Serialize(x)));

        var result = _db.Execute(JSON.ARRAPPEND, args.ToArray());
        return result.ToNullableLongArray();
    }
    
    /// <inheritdoc/>
    public long?[] ArrayIndex(RedisKey key, string? path, object value, long? start = null, long? stop = null)
    {
        var args = AssembleNonNullArguments(key, path,  JsonSerializer.Serialize(value), start, stop);
        var result = _db.Execute(JSON.ARRINDEX, args);
        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public long?[] ArrayInsert(RedisKey key, string path, long index, params object[] values)
    {
        var args = new List<object> { key, path, index };
        foreach (var val in values)
        {
            args.Add(JsonSerializer.Serialize(val));
        }
            
        var result = _db.Execute(JSON.ARRINSERT, args);
        return result.ToNullableLongArray();
   }

    /// <inheritdoc/>
    public long?[] ArrayLength(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        var result = _db.Execute(JSON.ARRLEN, args);
        return result.ToNullableLongArray();
    }

    /// <inheritdoc/>
    public RedisResult[] ArrayPop(RedisKey key, string? path = null, long? index = null)
    {
        var args = AssembleNonNullArguments(key, path, index);
        var res = _db.Execute(JSON.ARRPOP, args)!;

        if (res.Type == ResultType.MultiBulk)
        {
            return (RedisResult[])res!;
        }

        if (res.Type == ResultType.BulkString)
        {
            return new [] { res };
        }

        return Array.Empty<RedisResult>();
    }

    /// <inheritdoc/>
    public long?[] ArrayTrim(RedisKey key, string path, long start, long stop) => 
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

    /// <inheritdoc/>
    public T? Get<T>(RedisKey key, string path = "$")
    {
        var res = _db.Execute(JSON.GET, key, path);
        if (res.Type == ResultType.BulkString)
        {
            var arr = JsonSerializer.Deserialize<JsonArray>(res.ToString()!);
            if(arr?.Count > 0 )
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
    public IEnumerable<HashSet<string>> ObjectKeys(RedisKey key, string? path = null)
    {
        var sets = new List<HashSet<string>>();
        var args = AssembleNonNullArguments(key, path);
        var res = (RedisResult[])_db.Execute(JSON.OBJKEYS, args)!;
        if (res.All(x=>x.Type!=ResultType.MultiBulk))
        {
            var keys = res.Select(x => x.ToString()!);
            sets.Add(keys.ToHashSet());
            return sets;
        }

        foreach (var result in res)
        {
            var set = new HashSet<string>();
            if (result.Type == ResultType.MultiBulk)
            {
                var resultArr = (RedisResult[])result!;
                foreach (var item in resultArr)
                {
                    set.Add(item.ToString()!);
                }
            }
            sets.Add(set);
        }

        return sets;
    }

    /// <inheritdoc/>
    public long?[] ObjectLength(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return _db.Execute(JSON.OBJLEN, args).ToNullableLongArray();
    }
}