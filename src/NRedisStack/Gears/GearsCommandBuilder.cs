using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using NRedisStack.Gears.Literals;
namespace NRedisStack
{

    public static class GearsCommandBuilder
    {
        public static SerializedCommand TFunctionLoad(string libraryCode, string? config = null, bool replace = false)
        {
            var args = new List<object>() { "LOAD" };

            if (replace)
            {
                args.Add(GearsArgs.REPLACE);
            }

            if (config != null)
            {
                args.Add(GearsArgs.CONFIG);
                args.Add(config);
            }
            args.Add(libraryCode);
            return new SerializedCommand("TFUNCTION", args);
        }

        public static SerializedCommand TFunctionDelete(string libraryName)
        {
            return new SerializedCommand("TFUNCTION", "DELETE", libraryName);
        }
    }
}
