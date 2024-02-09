using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack
{
    public static class Auxiliary
    {
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

        // public static IDatabase GetDatabase(this ConnectionMultiplexer redis) => redis.GetDatabase("", "");


        public static RedisResult Execute(this IDatabase db, SerializedCommand command)
        {
            return db.Execute(command.Command, command.Args);
        }

        public async static Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command)
        {
            var compareVersions = db.Multiplexer.GetServer(db.Multiplexer.GetEndPoints()[0]).Version.CompareTo(new Version(7, 1, 242));
            return await db.ExecuteAsync(command.Command, command.Args);
        }

        public static List<RedisResult> ExecuteBroadcast(this IDatabase db, string command)
                => db.ExecuteBroadcast(new SerializedCommand(command));

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

        public async static Task<List<RedisResult>> ExecuteBroadcastAsync(this IDatabaseAsync db, string command)
                => await db.ExecuteBroadcastAsync(new SerializedCommand(command));

        public async static Task<List<RedisResult>> ExecuteBroadcastAsync(this IDatabaseAsync db, SerializedCommand command)
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
}
