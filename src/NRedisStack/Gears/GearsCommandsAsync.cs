using StackExchange.Redis;
namespace NRedisStack
{

    public static class GearsCommandsAsync //: IGearsCommandsAsync
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
        /// <remarks><seealso href="https://redis.io/commands/"/></remarks> //TODO: add link to the command when it's available
        public static async Task<bool> TFunctionLoadAsync(this IDatabase db, string libraryCode, string? config = null, bool replace = false)
        {
            return (await db.ExecuteAsync(GearsCommandBuilder.TFunctionLoad(libraryCode, config, replace))).OKtoBoolean();
        }
    }
}
