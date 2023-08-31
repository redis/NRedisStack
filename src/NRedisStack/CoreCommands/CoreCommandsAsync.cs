using NRedisStack.Core;
using StackExchange.Redis;
namespace NRedisStack
{

    public static class CoreCommandsAsync //: ICoreCommandsAsync
    {
        /// <summary>
        /// Sets information specific to the client or connection.
        /// </summary>
        /// <param name="attr">which attribute to set</param>
        /// <param name="value">the attribute value</param>
        /// <returns><see langword="true"/> if the attribute name was successfully set, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/client-setinfo/"/></remarks>
        public static async Task<bool> ClientSetInfoAsync(this IDatabase db, SetInfoAttr attr, string value)
        {
            return (await db.ExecuteAsync(CoreCommandBuilder.ClientSetInfo(attr, value))).OKtoBoolean();
        }
    }
}
