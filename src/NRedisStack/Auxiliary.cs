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

        public static RedisResult Execute(this IDatabase db, SerializedCommand command)
        {
            return db.Execute(command.Command, command.Args);
        }

        public async static Task<RedisResult> ExecuteAsync(this IDatabaseAsync db, SerializedCommand command)
        {
            return await db.ExecuteAsync(command.Command, command.Args);
        }
    }
}