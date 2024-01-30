using NRedisStack.Core;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack
{
    public static class Auxiliary
    {
        private static string? _libraryName = $"NRedisStack(.NET_v{Environment.Version})";
        private static bool _setInfo = true;
        public static bool IsCluster(this IDatabase db) => db.Multiplexer.GetEndPoints().Length > 1;
        public static void ResetInfoDefaults()
        {
            _setInfo = true;
            _libraryName = $"NRedisStack(.NET_v{Environment.Version})";
        }
        public static List<object> MergeArgs(RedisKey key, params RedisValue[] items)
        {
            var args = new List<object>(items.Length + 1) { key };
            foreach (var item in items) args.Add(item);
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

        private static void SetInfoInPipeline(this IDatabase db)
        {
            if (_libraryName == null) return;
            Pipeline pipeline = new Pipeline(db);
            _ = pipeline.Db.ClientSetInfoAsync(SetInfoAttr.LibraryName, _libraryName!);
            _ = pipeline.Db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion());
            pipeline.Execute();
        }

        public static RedisResult Execute(this IDatabase db, SerializedCommand command)
        {
            if (_setInfo)
            {
                _setInfo = false;
                db.SetInfoInPipeline();
            }
            return db.Execute(command.Command, command.Args);
        }

        // public static List<RedisResult> ClusterExecute(this IDatabase db, SerializedCommand command)
        // {
        //     if (_setInfo)
        //     {
        //         _setInfo = false;
        //         db.SetInfoInPipeline();
        //     }
        //     switch (command.Policy)
        //     {
        //         case RequestPolicy.Default:
        //             return db.Execute(command.Command, command.Args);
        //         case RequestPolicy.AllNodes:
        //             return db.ExecuteAllNodes(command.Command, command.Args);
        //         case RequestPolicy.AllShards:
        //             return db.ExecuteAllShards(command.Command, command.Args, CommandFlags.DemandMaster);
        //         case RequestPolicy.AnyShard:
        //             return db.ExecuteAnyShard(command.Command, command.Args, CommandFlags.PreferMaster);
        //         case RequestPolicy.MultiShard:
        //             return db.ExecuteMultiShard(command.Command, command.Args, CommandFlags.DemandMaster);
        //         case RequestPolicy.Special:
        //             throw new NotImplementedException("Special policy is not implemented yet");
        //         default:
        //             throw new NotImplementedException("Unknown policy");
        //     }
        // }

        // TODO: add Execute for each RequestPolicy (check if I can use SE.Redis CommandFlags)
        public async static Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command)
        {
            if (_setInfo)
            {
                _setInfo = false;
                ((IDatabase)db).SetInfoInPipeline();
            }
            return await db.ExecuteAsync(command.Command, command.Args);
        }

        public static List<RedisResult> ExecuteAllShards(this IDatabase db, string command)
                => db.ExecuteAllShards(new SerializedCommand(command));

        public static List<RedisResult> ExecuteAllShards(this IDatabase db, SerializedCommand command)
        {
            var redis = db.Multiplexer;
            var endpoints = redis.GetEndPoints();
            var results = new List<RedisResult>(endpoints.Length);

            foreach (var endPoint in endpoints)
            {
                var server = redis.GetServer(endPoint);

                if (!server.IsReplica)
                {
                    results.Add(server.Multiplexer.GetDatabase().Execute(command));
                }
            }
            return results;
        }

        public static List<RedisResult> ExecuteAllNodes(this IDatabase db, SerializedCommand command)
        {
            var redis = db.Multiplexer;
            var endpoints = redis.GetEndPoints();
            var results = new List<RedisResult>();

            foreach (var endPoint in endpoints)
            {
                var server = redis.GetServer(endPoint);
                results.Add(server.Multiplexer.GetDatabase().Execute(command));
            }

            return results;
        }

        public async static Task<List<RedisResult>> ExecuteAllShardsAsync(this IDatabaseAsync db, string command)
                => await db.ExecuteAllShardsAsync(new SerializedCommand(command));

        public async static Task<List<RedisResult>> ExecuteAllShardsAsync(this IDatabaseAsync db, SerializedCommand command)
        {
            var redis = db.Multiplexer;
            var endpoints = redis.GetEndPoints();
            var results = new List<RedisResult>(endpoints.Length);

            foreach (var endPoint in endpoints)
            {
                var server = redis.GetServer(endPoint);

                if (!server.IsReplica)
                {
                    results.Add(await server.Multiplexer.GetDatabase().ExecuteAsync(command));
                }

            }
            return results;
        }

        public static string GetNRedisStackVersion()
        {
            Version version = typeof(Auxiliary).Assembly.GetName().Version!;
            return $"{version.Major}.{version.Minor}.{version.Build}";
        }
    }
}
