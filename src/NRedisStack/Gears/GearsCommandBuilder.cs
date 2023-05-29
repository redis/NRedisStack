using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using NRedisStack.Gears.Literals;
namespace NRedisStack
{

    public static class GearsCommandBuilder
    {
        public static SerializedCommand TFunctionLoad(string libraryCode, string? config = null, bool replace = false)
        {
            var args = new List<string>();

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
            return new SerializedCommand("TFUNCTION LOAD", args); // TODO: check if its supposed to be "TFUNCTION", "LOAD"
        }
    }
}
