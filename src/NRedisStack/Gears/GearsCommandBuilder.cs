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

        public static SerializedCommand TFunctionList(bool withCode = false, int verbose = 0, string? libraryName = null)
        {
            var args = new List<object>() { "LIST" };

            if (withCode)
            {
                args.Add("WITHCODE");
            }

            if (verbose > 0)
            {
                args.Add(new string('v', Math.Min(3, verbose)));
            }

            if (libraryName != null)
            {
                args.Add("LIBRARY");
                args.Add(libraryName);
            }

            return new SerializedCommand("TFUNCTION", args);
        }

        public static SerializedCommand TFCall(string libraryName, string functionName, string[]? keys = null, string[]? args = null, bool async = false)
        {
            string command = async ? "TFCALLASYNC" : "TFCALL";
            var commandArgs = new List<object>() {libraryName, functionName};

            if (keys != null)
            {
                commandArgs.Add(keys.Length);
                commandArgs.AddRange(keys);
            }
            else
            {
                commandArgs.Add(0);
            }

            if (args != null)
            {
                commandArgs.AddRange(args);
            }

            return new SerializedCommand(command, commandArgs);
        }
    }
}
