using System.Xml.Linq;
using NRedisStack.Core;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack
{
    public static class Auxiliary
    {
        private static bool _setInfo = false;
        private static string _libraryName = "";
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

        // TODO: add all the signatures of GetDatabase
        public static IDatabase GetDatabase(this ConnectionMultiplexer redis,
                                            string? LibraryName = "")
        {
            var _db = redis.GetDatabase();
            if (LibraryName == null)
            {
                return _db;
            }

            if (LibraryName != "")
                _libraryName = $"NRedisStack({LibraryName});.NET-{Environment.Version})";
            else
                _libraryName = $"NRedisStack;.NET-{Environment.Version}";

            return _db;
        }

        private static void SetInfoInPipeline(this IDatabase _db, string? LibraryName)
        {
            Pipeline pipeline = new Pipeline(_db);
            _ = pipeline.Db.ClientSetInfoAsync(SetInfoAttr.LibraryName, LibraryName!);
            _ = pipeline.Db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()!);
            pipeline.Execute();
        }

        public static RedisResult Execute(this IDatabase db, SerializedCommand command)
        {
            if(!_setInfo)
            {
                db.SetInfoInPipeline(_libraryName);
                _setInfo = true;
            }
            return db.Execute(command.Command, command.Args);
        }

        public async static Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command)
        {
            if(!_setInfo)
            {
                // TODO: check if I can do it in pipeline
                _ = db.ClientSetInfoAsync(SetInfoAttr.LibraryName, _libraryName);
                _ = db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion());
                _setInfo = true;
            }
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
            XDocument csprojDocument = GetCsprojDocument();

            // Find the Version element and get its value.
            var versionElement = csprojDocument.Root!
                .Descendants("Version")
                .FirstOrDefault();

            return versionElement!.Value;
        }

        public static string GetStackExchangeRedisVersion()
        {
            XDocument csprojDocument = GetCsprojDocument();

            // Find the PackageReference element with Include="StackExchange.Redis" and get its Version attribute.
            var stackExchangeRedisVersion = csprojDocument.Root!
                .Descendants("PackageReference")
                .Where(element => element.Attribute("Include")?.Value == "StackExchange.Redis")
                .Select(element => element.Attribute("Version")?.Value)
                .FirstOrDefault();

            return stackExchangeRedisVersion!;
        }

        private static XDocument GetCsprojDocument()
        {
            string csprojFilePath = Path.Combine(Directory.GetCurrentDirectory(), "..", "..", "..", "..", "..", "src", "NRedisStack", "NRedisStack.csproj");

            // Load the .csproj file.
            var csprojDocument = XDocument.Load(csprojFilePath);
            return csprojDocument;
        }
    }
}