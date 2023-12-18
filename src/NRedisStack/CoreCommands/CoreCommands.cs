using NRedisStack.Core;
using StackExchange.Redis;
namespace NRedisStack
{

    public static class CoreCommands
    {
        /// <summary>
        /// Sets information specific to the client or connection.
        /// </summary>
        /// <param name="attr">which attribute to set</param>
        /// <param name="value">the attribute value</param>
        /// <returns><see langword="true"/> if the attribute name was successfully set, Error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/client-setinfo/"/></remarks>
        public static bool ClientSetInfo(this IDatabase db, SetInfoAttr attr, string value)
        {
            var compareVersions = db.Multiplexer.GetServer(db.Multiplexer.GetEndPoints()[0]).Version.CompareTo(new Version(7, 1, 242));
            if (compareVersions < 0) // the server does not support the CLIENT SETNAME command
                return false;
            return db.Execute(CoreCommandBuilder.ClientSetInfo(attr, value)).OKtoBoolean();
        }
    }
}
