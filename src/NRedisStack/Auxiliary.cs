using NRedisStack.Core;
using NRedisStack.Core.Literals;
using NRedisStack.Json.Literals;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search.Literals;
using StackExchange.Redis;

namespace NRedisStack;

public static class Auxiliary
{
    private static string? _libraryName = $"NRedisStack(.NET_v{Environment.Version})";
    private static bool _setInfo = true;
    public static bool IsCluster(this IDatabase db) => db.Multiplexer.GetEndPoints().Length > 1;
    public static bool IsEnterprise(this IDatabase db) // TODO: check if there is a better way to check if the server is Redis Enterprise without sending a command each time
    {
        // DPING command is available only in Redis Enterprise
        try
        {
            db.Execute("DPING");
            return true;
        }
        catch (RedisServerException)
        {
            return false;
        }
    }

    public static void ResetInfoDefaults()
    {
        _setInfo = true;
        _libraryName = $"NRedisStack(.NET_v{Environment.Version})";
    }

    public static List<object> MergeArgs(RedisKey key, params RedisValue[] items)
    {
        var args = new List<object>(items.Length + 1) { key };
        args.AddRange(items.Cast<object>());
        return args;
    }

    public static object[] AssembleNonNullArguments(params object?[] arguments)
    {
        var args = new List<object>();
        foreach (var arg in arguments)
        {
            if (arg != null)
            {
                args.Add(arg);
            }
        }

        return args.ToArray();
    }

    // TODO: add all the signatures of GetDatabase
    public static IDatabase GetDatabase(this ConnectionMultiplexer redis,
                                        string? LibraryName)
    {
        var _db = redis.GetDatabase();
        if (LibraryName == null) // the user wants to disable the library name and version sending
            _setInfo = false;

        else // the user set his own the library name
            _libraryName = $"NRedisStack({LibraryName};.NET_v{Environment.Version})";

        return _db;
    }

    internal static void SetInfoInPipeline(this IDatabase db)
    {
        if (_setInfo)
        {
            _setInfo = false;
            if (_libraryName == null) return;
            Pipeline pipeline = new Pipeline(db);
            _ = pipeline.Db.ClientSetInfoAsync(SetInfoAttr.LibraryName, _libraryName!);
            _ = pipeline.Db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion());
            pipeline.Execute();
        }
    }

    // public static RedisResult Execute(this IDatabase db, SerializedCommand command)
    // {
    //     if (_setInfo)
    //     {
    //         _setInfo = false;
    //         db.SetInfoInPipeline();
    //     }
    //     return (db.IsCluster()) ? db.ClusterExecute(command)
    //                             : db.Execute(command.Command, command.Args);
    // }

    // public async static Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command)
    // {
    //     if (_setInfo)
    //     {
    //         _setInfo = false;
    //         ((IDatabase)db).SetInfoInPipeline();
    //     }
    //     return (db.IsCluster()) ? db.ClusterExecuteAsync(command)
    //                             : db.ExecuteAsync(command.Command, command.Args);
    // }

    public static RedisResult Execute(this IDatabase db, SerializedCommand command)
    {
        db.SetInfoInPipeline();

        if (!db.IsEnterprise() || !db.IsCluster())
            return db.Execute(command.Command, command.Args);

        switch (command.Policy)
        {
            case RequestPolicy.Default: // add is cluster
                return db.Execute(command.Command, command.Args);
            case RequestPolicy.AllNodes:
                return db.ExecuteAllNodes(command);
            case RequestPolicy.AllShards:
                return db.ExecuteAllShards(command);
            case RequestPolicy.AnyShard:
                return db.ExecuteAnyShard(command);
            case RequestPolicy.MultiShard:
                throw new NotImplementedException("MultiShard policy is not implemented yet");
            case RequestPolicy.Special:
                throw new NotImplementedException("Special policy is not implemented yet");
            default:
                throw new NotImplementedException("Unknown policy");
        }
    }

    public static async Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command)
    {
        ((IDatabase)db).SetInfoInPipeline();

        if (!((IDatabase)db).IsCluster())
            return await db.ExecuteAsync(command.Command, command.Args);

        switch (command.Policy)
        {
            case RequestPolicy.Default:
                return await db.ExecuteAsync(command.Command, command.Args);
            case RequestPolicy.AllNodes:
                return await db.ExecuteAllNodesAsync(command);
            case RequestPolicy.AllShards:
                return await db.ExecuteAllShardsAsync(command);
            case RequestPolicy.AnyShard:
                return await db.ExecuteAnyShardAsync(command);
            case RequestPolicy.MultiShard:
                // return db.ExecuteMultiShard(command);
                throw new NotImplementedException("MultiShard policy is not implemented yet");
            case RequestPolicy.Special:
                throw new NotImplementedException("Special policy is not implemented yet");
            default:
                throw new NotImplementedException("Unknown policy");
        }
    }

    public static RedisResult ExecuteAllNodes(this IDatabase db, SerializedCommand command)
    {
        var redis = db.Multiplexer;
        var endpoints = redis.GetEndPoints();
        var results = new RedisResult[endpoints.Length];

        for (int i = 0; i < endpoints.Length; i++)
        {
            results[i] = redis.GetServer(endpoints[i]).Execute(command.Command, command.Args);
        }

        return results.ToRedisResult(command.Command);
    }

    public static async Task<RedisResult> ExecuteAllNodesAsync(this IDatabaseAsync db, SerializedCommand command)
    {
        var redis = db.Multiplexer;
        var endpoints = redis.GetEndPoints();
        var results = new RedisResult[endpoints.Length];

        for (int i = 0; i < endpoints.Length; i++)
        {
            results[i] = await redis.GetServer(endpoints[i]).ExecuteAsync(command.Command, command.Args);
        }

        return results.ToRedisResult(command.Command);
    }

    public static RedisResult ExecuteAllShards(this IDatabase db, string command)
            => db.ExecuteAllShards(new SerializedCommand(command));

    public static RedisResult ExecuteAllShards(this IDatabase db, SerializedCommand command)
    {
        var redis = db.Multiplexer;
        var endpoints = redis.GetEndPoints();
        var results = new List<RedisResult>();

        foreach (var endpoint in endpoints)
        {
            var server = redis.GetServer(endpoint);
            if (!server.IsReplica)
            {
                results.Add(server.Execute(command.Command, command.Args));
            }
        }

        return results.ToArray().ToRedisResult(command.Command);


    }

    public async static Task<RedisResult> ExecuteAllShardsAsync(this IDatabaseAsync db, string command)
            => await db.ExecuteAllShardsAsync(new SerializedCommand(command));

    public async static Task<RedisResult> ExecuteAllShardsAsync(this IDatabaseAsync db, SerializedCommand command)
    {
        var redis = db.Multiplexer;
        var endpoints = redis.GetEndPoints();
        var results = new List<RedisResult>();

        foreach (var endpoint in endpoints)
        {
            var server = redis.GetServer(endpoint);
            if (!server.IsReplica)
            {
                results.Add(await server.ExecuteAsync(command.Command, command.Args));
            }
        }
        var toRedisResult = results.ToArray().ToRedisResult(command.Command);

        return toRedisResult;
        return RedisResult.Create(results.ToArray());
    }

    public static RedisResult ExecuteAnyShard(this IDatabase db, SerializedCommand command)
    {
        var server = GetAnyPrimary(db.Multiplexer);
        return server.Execute(command.Command, command.Args);
    }

    public static async Task<RedisResult> ExecuteAnyShardAsync(this IDatabaseAsync db, SerializedCommand command)
    {
        var server = GetAnyPrimary(db.Multiplexer);
        return await server.ExecuteAsync(command.Command, command.Args);
    }

    public static IServer GetAnyPrimary(IConnectionMultiplexer muxer)
    {
        foreach (var endpoint in muxer.GetEndPoints())
        {
            var server = muxer.GetServer(endpoint);
            if (!server.IsReplica) return server;
        }
        throw new InvalidOperationException("Requires a primary endpoint (found none)");
    }

    public static RedisResult ToRedisResult(this RedisResult[] results, string command)
    {
        switch (command)
        {
            case FT.ALIASADD:
            case FT.ALIASDEL:
            case FT.ALIASUPDATE:
            case FT.ALTER:
            case FT.CREATE:
            case FT.DROPINDEX:
            case FT.DICTADD:
            case FT.DICTDEL:
            case RedisCoreCommands.FLUSHALL:
                return results.OKArraytoResult();
            case JSON.MGET:
                // TODO: implement
                break;
        }
        return results[0]; // TODO: check if this is the correct behavior
    }

    public static RedisResult OKArraytoResult(this RedisResult[] results)
    {
        foreach (var result in results)
        {
            if (result.ToString() != "OK")
            {
                return result; // return the problematic result
            }
        }
        return results[0]; // return the first result (which is OK, like the others)
    }

    // TODO: check if implementing MultiShard and Special policies is nessesary

    public static string GetNRedisStackVersion()
    {
        Version version = typeof(Auxiliary).Assembly.GetName().Version!;
        return $"{version.Major}.{version.Minor}.{version.Build}";
    }
}