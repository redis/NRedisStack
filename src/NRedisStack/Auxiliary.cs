using NRedisStack.Core;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack;

public static class Auxiliary
{
    private static string? _libraryName = $"NRedisStack(.NET_v{Environment.Version})";
    private static bool _setInfo = true;
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

    /// <summary>
    /// Announce the library name/version on first use, once per process.
    /// </summary>
    /// <remarks>
    /// This takes <see cref="IDatabaseAsync"/> rather than <see cref="IDatabase"/> deliberately: an async-only
    /// database (such as the retry wrapper from <c>WithRetry</c>) does not implement <see cref="IDatabase"/>, so
    /// demanding the sync interface here would make every async command fail. The two commands are issued
    /// fire-and-forget - they are enqueued ahead of the command that triggered this, and overlapped
    /// fire-and-forget is well-defined - so nothing has to be awaited, and no batch is needed to group them.
    /// </remarks>
    internal static void SetLibraryInfoOnce(this IDatabaseAsync db)
    {
        if (!_setInfo) return;

        _setInfo = false; // one attempt only, successful or not
        var libraryName = _libraryName;
        if (libraryName == null) return;

        try
        {
            Observe(db.ClientSetInfoAsync(SetInfoAttr.LibraryName, libraryName, CommandFlags.FireAndForget));
            Observe(db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion(), CommandFlags.FireAndForget));
        }
        catch
        {
            // reporting who we are is never worth failing the caller's command over
        }

        static void Observe(Task pending)
        {
            // fire-and-forget should already have completed, on this thread, with nothing to observe;
            // anything else is unexpected enough to be worth consuming, so that a failure cannot resurface
            // later as an unobserved task exception unrelated to anything the caller did
            if (!pending.IsCompletedSuccessfully)
            {
                _ = Awaited(pending);
            }
        }

        static async Task Awaited(Task pending)
        {
            try
            {
                await pending.ConfigureAwait(false);
            }
            catch
            {
                // as above: best-effort
            }
        }
    }

    public static RedisResult Execute(this IDatabase db, SerializedCommand command)
    {
        db.SetLibraryInfoOnce();
        return db.Execute(command.Command, command.Args, flags: command.EffectiveFlags);
    }

    internal static RedisResult Execute(this IServer server, int? db, SerializedCommand command)
    {
        return server.Execute(db, command.Command, command.Args, flags: command.EffectiveFlags);
    }

    public static async Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command)
    {
        db.SetLibraryInfoOnce();
        return await db.ExecuteAsync(command.Command, command.Args, flags: command.EffectiveFlags);
    }

    /// <summary>
    /// Dispatch a command with additional flags beyond its category.
    /// </summary>
    /// <remarks>
    /// Deliberately does not announce the library name: this overload exists for the announcement itself.
    /// </remarks>
    internal static Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command, CommandFlags flags)
        => db.ExecuteAsync(command.Command, command.Args, flags: command.EffectiveFlags | flags);

    internal static async Task<RedisResult> ExecuteAsync(this IServer server, int? db, SerializedCommand command)
    {
        return await server.ExecuteAsync(db, command.Command, command.Args, flags: command.EffectiveFlags);
    }

    public static List<RedisResult> ExecuteBroadcast(this IDatabase db, string command)
        => db.ExecuteBroadcast(SerializedCommand.Uncategorized(command));

    public static List<RedisResult> ExecuteBroadcast(this IDatabase db, CommandFlags category, string command)
        => db.ExecuteBroadcast(new SerializedCommand(category, command));

    public static List<RedisResult> ExecuteBroadcast(this IDatabase db, SerializedCommand command)
    {
        var redis = db.Multiplexer;
        var endpoints = redis.GetEndPoints();
        var results = new List<RedisResult>(endpoints.Length);

        foreach (var endPoint in endpoints)
        {
            var server = redis.GetServer(endPoint);

            if (server.IsReplica)
            {
                continue; // Skip replica nodes
            }
            // Send your command to the master node

            results.Add(server.Multiplexer.GetDatabase().Execute(command));
        }
        return results;
    }

    public static async Task<List<RedisResult>> ExecuteBroadcastAsync(this IDatabaseAsync db, string command)
        => await db.ExecuteBroadcastAsync(SerializedCommand.Uncategorized(command));

    public static async Task<List<RedisResult>> ExecuteBroadcastAsync(this IDatabaseAsync db, CommandFlags category, string command)
        => await db.ExecuteBroadcastAsync(new SerializedCommand(category, command));

    private static async Task<List<RedisResult>> ExecuteBroadcastAsync(this IDatabaseAsync db, SerializedCommand command)
    {
        var redis = db.Multiplexer;
        var endpoints = redis.GetEndPoints();
        var results = new List<RedisResult>(endpoints.Length);

        foreach (var endPoint in endpoints)
        {
            var server = redis.GetServer(endPoint);

            if (server.IsReplica)
            {
                continue; // Skip replica nodes
            }
            // Send your command to the master node

            results.Add(await server.Multiplexer.GetDatabase().ExecuteAsync(command));
        }
        return results;
    }

    public static string GetNRedisStackVersion()
    {
        Version version = typeof(Auxiliary).Assembly.GetName().Version!;
        return $"{version.Major}.{version.Minor}.{version.Build}";
    }
}