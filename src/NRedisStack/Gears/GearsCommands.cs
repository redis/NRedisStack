using StackExchange.Redis;
namespace NRedisStack
{
    [Obsolete]
    public static class GearsCommands //: GearsCommandsAsync, IGearsCommands
    {

        /// <summary>
        /// Load a new library to RedisGears.
        /// </summary>
        /// <param name="libraryCode">the library code.</param>
        /// <param name="config">a string representation of a JSON object
        /// that will be provided to the library on load time,
        /// for more information refer to
        /// <see href="https://github.com/RedisGears/RedisGears/blob/master/docs/function_advance_topics.md#library-configuration">
        /// library configuration</see></param>
        /// <param name="replace">an optional argument, instructs RedisGears to replace the function if its already exists.</param>
        /// <returns><see langword="true"/> if everything was done correctly, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tfunction-load/"/></remarks> //TODO: check this link when it's available
        [Obsolete]
        public static bool TFunctionLoad(this IDatabase db, string libraryCode, bool replace = false, string? config = null)
        {
            return db.Execute(GearsCommandBuilder.TFunctionLoad(libraryCode, replace, config)).OKtoBoolean();
        }

        /// <summary>
        /// Delete a library from RedisGears.
        /// </summary>
        /// <param name="libraryName">the name of the library to delete.</param>
        /// <returns><see langword="true"/> if the library was deleted successfully, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tfunction-delete/"/></remarks> //TODO: check this link when it's available
        [Obsolete]
        public static bool TFunctionDelete(this IDatabase db, string libraryName)
        {
            return db.Execute(GearsCommandBuilder.TFunctionDelete(libraryName)).OKtoBoolean();
        }

        /// <summary>
        /// List the functions with additional information about each function.
        /// </summary>
        /// <param name="withCode">Show libraries code.</param>
        /// <param name="verbose">output verbosity level, higher number will increase verbosity level</param>
        /// <param name="libraryName">specifying a library name (can be used
        /// multiple times to show multiple libraries in a single command)</param>
        /// <returns>Information about the requested libraries.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tfunction-list/"/></remarks> //TODO: check this link when it's available
        [Obsolete]
        public static Dictionary<string, RedisResult>[] TFunctionList(this IDatabase db, bool withCode = false, int verbose = 0, string? libraryName = null)
        {
            return db.Execute(GearsCommandBuilder.TFunctionList(withCode, verbose, libraryName)).ToDictionarys();
        }

        /// <summary>
        /// Trigger a sync function.
        /// </summary>
        /// <param name="libraryName">The library name contains the function.</param>
        /// <param name="functionName">The function name to run.</param>
        /// <param name="keys">keys that will be touched by the function.</param>
        /// <param name="args">Additional argument to pass to the function.</param>
        /// <returns>The return value from the sync &amp; async function on error in case of failure.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tfcall"/></remarks>
        [Obsolete]
        public static RedisResult TFCall_(this IDatabase db, string libraryName, string functionName, string[]? keys = null, string[]? args = null)
        {
            return db.Execute(GearsCommandBuilder.TFCall(libraryName, functionName, keys, args, async: false));
        }

        /// <summary>
        /// Trigger a async (Coroutine) function.
        /// </summary>
        /// <param name="libraryName">The library name contains the function.</param>
        /// <param name="functionName">The function name to run.</param>
        /// <param name="keys">keys that will be touched by the function.</param>
        /// <param name="args">Additional argument to pass to the function.</param>
        /// <returns>The return value from the sync &amp; async function on error in case of failure.</returns>
        /// <remarks><seealso href="https://redis.io/commands/tfcallasync"/></remarks>
        [Obsolete]
        public static RedisResult TFCallAsync_(this IDatabase db, string libraryName, string functionName, string[]? keys = null, string[]? args = null)
        {
            return db.Execute(GearsCommandBuilder.TFCall(libraryName, functionName, keys, args, async: true));
        }
    }
}
