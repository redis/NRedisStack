using NRedisStack.Literals;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System.Text.Json;
using static NRedisStack.Auxiliary;

namespace NRedisStack;

public sealed class JsonCommandBuilder
{
    private static readonly JsonCommandBuilder _instance = new JsonCommandBuilder();
    public static JsonCommandBuilder Instance { get { return _instance; } }

    private JsonCommandBuilder() { }

    public SerializedCommand Resp(RedisKey key, string? path = null)
    {
        if (string.IsNullOrEmpty(path))
        {
            return new SerializedCommand(JSON.RESP, key);
        }

        return new SerializedCommand(JSON.RESP, key, path);
    }

    public SerializedCommand Set(RedisKey key, RedisValue path, RedisValue json, When when = When.Always)
    {
        return when switch
        {
            When.Exists => new SerializedCommand(JSON.SET, key, path, json, "XX"),
            When.NotExists => new SerializedCommand(JSON.SET, key, path, json, "NX"),
            _ => new SerializedCommand(JSON.SET, key, path, json)
        };
    }

    public SerializedCommand StrAppend(RedisKey key, string value, string? path = null)
    {
        if (path == null)
        {
            return new SerializedCommand(JSON.STRAPPEND, key, JsonSerializer.Serialize(value));
        }

        return new SerializedCommand(JSON.STRAPPEND, key, path, JsonSerializer.Serialize(value));
    }

    public SerializedCommand StrLen(RedisKey key, string? path = null)
    {
        return (path != null) ? new SerializedCommand(JSON.STRLEN, key, path)
                              : new SerializedCommand(JSON.STRLEN, key);
    }

    public SerializedCommand Toggle(RedisKey key, string? path = null)
    {
        return (path != null) ? new SerializedCommand(JSON.TOGGLE, key, path)
                              : new SerializedCommand(JSON.TOGGLE, key, "$");
    }

    public SerializedCommand Type(RedisKey key, string? path = null)
    {
        return (path != null) ? new SerializedCommand(JSON.TYPE, key, path)
                              : new SerializedCommand(JSON.TYPE, key);
    }

    public SerializedCommand DebugMemory(string key, string? path = null)
    {
        return (path != null) ? new SerializedCommand(JSON.DEBUG, JSON.MEMORY, key, path)
                              : new SerializedCommand(JSON.DEBUG, JSON.MEMORY, key);
    }

    public SerializedCommand ArrAppend(RedisKey key, string? path = null, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));

        var args = new List<object> { key };
        if (path != null)
        {
            args.Add(path);
        }

        args.AddRange(values.Select(x => JsonSerializer.Serialize(x)));

        return new SerializedCommand(JSON.ARRAPPEND, args.ToArray());
    }

    public SerializedCommand ArrIndex(RedisKey key, string path, object value, long? start = null, long? stop = null)
    {
        if (start == null && stop != null)
            throw new ArgumentException("stop cannot be defined without start");

        var args = AssembleNonNullArguments(key, path, JsonSerializer.Serialize(value), start, stop);
        return new SerializedCommand(JSON.ARRINDEX, args);
    }

    public SerializedCommand ArrInsert(RedisKey key, string path, long index, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));
        var args = new List<object> { key, path, index };
        foreach (var val in values)
        {
            args.Add(JsonSerializer.Serialize(val));
        }

        return new SerializedCommand(JSON.ARRINSERT, args);
    }

    public SerializedCommand ArrLen(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new SerializedCommand(JSON.ARRLEN, args);
    }

    public SerializedCommand ArrPop(RedisKey key, string? path = null, long? index = null)
    {
        if (path == null && index != null)
            throw new ArgumentException("index cannot be defined without path");

        var args = AssembleNonNullArguments(key, path, index);
        return new SerializedCommand(JSON.ARRPOP, args)!;
    }

    public SerializedCommand ArrTrim(RedisKey key, string path, long start, long stop) =>
        new SerializedCommand(JSON.ARRTRIM, key, path, start, stop);

    public SerializedCommand Clear(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new SerializedCommand(JSON.CLEAR, args);
    }

    public SerializedCommand Del(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new SerializedCommand(JSON.DEL, args);
    }

    public SerializedCommand Get(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null, RedisValue? path = null)
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

        return new SerializedCommand(JSON.GET, args);
    }

    public SerializedCommand Get(RedisKey key, string[] paths, RedisValue? indent = null, RedisValue? newLine = null, RedisValue? space = null)
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

        foreach (var path in paths)
        {
            args.Add(path);
        }

        return new SerializedCommand(JSON.GET, args);
    }

    public SerializedCommand Get<T>(RedisKey key, string path = "$")
    {
        return new SerializedCommand(JSON.GET, key, path);

    }

    public SerializedCommand MGet(RedisKey[] keys, string path)
    {
        var args = new List<object>();
        foreach (var key in keys)
        {
            args.Add(key);
        }

        args.Add(path);
        return new SerializedCommand(JSON.MGET, args);
    }

    public SerializedCommand NumIncrby(RedisKey key, string path, double value)
    {
        return new SerializedCommand(JSON.NUMINCRBY, key, path, value);
    }

    public SerializedCommand ObjKeys(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new SerializedCommand(JSON.OBJKEYS, args);
    }

    public SerializedCommand ObjLen(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new SerializedCommand(JSON.OBJLEN, args);
    }
}