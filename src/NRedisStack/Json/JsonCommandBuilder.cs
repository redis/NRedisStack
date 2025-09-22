using NRedisStack.Json.DataTypes;
using NRedisStack.Json.Literals;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System.Text.Json;
using static NRedisStack.Auxiliary;

namespace NRedisStack;

public static class JsonCommandBuilder
{
    public static SerializedCommand Resp(RedisKey key, string? path = null)
    {
        return string.IsNullOrEmpty(path)
            ? new(JSON.RESP, key)
            : new SerializedCommand(JSON.RESP, key, path!);
    }

    public static SerializedCommand Set(RedisKey key, RedisValue path, RedisValue json, When when = When.Always)
    {
        return when switch
        {
            When.Exists => new(JSON.SET, key, path, json, "XX"),
            When.NotExists => new(JSON.SET, key, path, json, "NX"),
            _ => new(JSON.SET, key, path, json)
        };
    }

    public static SerializedCommand MSet(KeyPathValue[] KeyPathValueList)
    {
        if (KeyPathValueList.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(KeyPathValueList));

        var args = KeyPathValueList.SelectMany(x => x.ToArray()).ToArray();
        return new(JSON.MSET, args);
    }

    public static SerializedCommand Merge(RedisKey key, RedisValue path, RedisValue json)
    {
        return new(JSON.MERGE, key, path, json);
    }

    public static SerializedCommand StrAppend(RedisKey key, string value, string? path = null)
    {
        return path == null
            ? new(JSON.STRAPPEND, key, JsonSerializer.Serialize(value))
            : new SerializedCommand(JSON.STRAPPEND, key, path, JsonSerializer.Serialize(value));
    }

    public static SerializedCommand StrLen(RedisKey key, string? path = null)
    {
        return path != null
            ? new(JSON.STRLEN, key, path)
            : new SerializedCommand(JSON.STRLEN, key);
    }

    public static SerializedCommand Toggle(RedisKey key, string? path = null)
    {
        return path != null
            ? new(JSON.TOGGLE, key, path)
            : new SerializedCommand(JSON.TOGGLE, key, "$");
    }

    public static SerializedCommand Type(RedisKey key, string? path = null)
    {
        return (path != null)
            ? new(JSON.TYPE, key, path)
            : new SerializedCommand(JSON.TYPE, key);
    }

    public static SerializedCommand DebugMemory(string key, string? path = null)
    {
        return (path != null)
            ? new(JSON.DEBUG, JSON.MEMORY, (RedisKey)key, path)
            : new SerializedCommand(JSON.DEBUG, JSON.MEMORY, (RedisKey)key);
    }

    public static SerializedCommand ArrAppend(RedisKey key, string? path = null, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));

        var args = new List<object> { key };
        if (path != null)
        {
            args.Add(path);
        }

        args.AddRange(values.Select(x => JsonSerializer.Serialize(x)));

        return new(JSON.ARRAPPEND, args.ToArray());
    }

    public static SerializedCommand ArrIndex(RedisKey key, string path, object value, long? start = null,
        long? stop = null)
    {
        if (start == null && stop != null)
            throw new ArgumentException("stop cannot be defined without start");

        var args = AssembleNonNullArguments(key, path, JsonSerializer.Serialize(value), start, stop);
        return new(JSON.ARRINDEX, args);
    }

    public static SerializedCommand ArrInsert(RedisKey key, string path, long index, params object[] values)
    {
        if (values.Length < 1)
            throw new ArgumentOutOfRangeException(nameof(values));
        var args = new List<object> { key, path, index };
        args.AddRange(values.Select(val => JsonSerializer.Serialize(val)));

        return new(JSON.ARRINSERT, args);
    }

    public static SerializedCommand ArrLen(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new(JSON.ARRLEN, args);
    }

    public static SerializedCommand ArrPop(RedisKey key, string? path = null, long? index = null)
    {
        if (path == null && index != null)
            throw new ArgumentException("index cannot be defined without path");

        var args = AssembleNonNullArguments(key, path, index);
        return new(JSON.ARRPOP, args);
    }

    public static SerializedCommand ArrTrim(RedisKey key, string path, long start, long stop) =>
        new(JSON.ARRTRIM, key, path, start, stop);

    public static SerializedCommand Clear(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new(JSON.CLEAR, args);
    }

    public static SerializedCommand Del(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new(JSON.DEL, args);
    }

    public static SerializedCommand Get(RedisKey key, RedisValue? indent = null, RedisValue? newLine = null,
        RedisValue? space = null, RedisValue? path = null)
    {
        List<object> args = [key];

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

        return new(JSON.GET, args);
    }

    public static SerializedCommand Get(RedisKey key, string[] paths, RedisValue? indent = null,
        RedisValue? newLine = null, RedisValue? space = null)
    {
        List<object> args = [key];

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

        args.AddRange(paths);

        return new(JSON.GET, args);
    }

    public static SerializedCommand Get<T>(RedisKey key, string path = "$")
    {
        return new(JSON.GET, key, path);
    }

    public static SerializedCommand MGet(RedisKey[] keys, string path)
    {
        var args = keys.Cast<object>().ToList();

        args.Add(path);
        return new(JSON.MGET, args);
    }

    public static SerializedCommand NumIncrby(RedisKey key, string path, double value)
    {
        return new(JSON.NUMINCRBY, key, path, value);
    }

    public static SerializedCommand ObjKeys(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new(JSON.OBJKEYS, args);
    }

    public static SerializedCommand ObjLen(RedisKey key, string? path = null)
    {
        var args = AssembleNonNullArguments(key, path);
        return new(JSON.OBJLEN, args);
    }
}