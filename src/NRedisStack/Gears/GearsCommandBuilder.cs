using NRedisStack.RedisStackCommands;
using NRedisStack.Gears.Literals;
namespace NRedisStack
{

    public static class GearsCommandBuilder
    {
        public static SerializedCommand TFunctionLoad(string libraryCode, bool replace = false, string? config = null)
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

            if (verbose > 0 && verbose < 4)
            {
                args.Add(new string('v', verbose));
            }
            else if (verbose != 0) // verbose == 0 is the default so we don't need to throw an error
            {
                throw new ArgumentOutOfRangeException(nameof(verbose), "verbose must be between 1 and 3");
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
            var commandArgs = new List<object>() {$"{libraryName}.{functionName}"};

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
