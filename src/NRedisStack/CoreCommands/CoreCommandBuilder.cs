using NRedisStack.RedisStackCommands;
using NRedisStack.Core.Literals;
using NRedisStack.Core;
using StackExchange.Redis;

namespace NRedisStack
{

    public static class CoreCommandBuilder
    {
        public static SerializedCommand ClientSetInfo(SetInfoAttr attr, string value)
        {
            string attrValue = attr switch
            {
                SetInfoAttr.LibraryName => CoreArgs.lib_name,
                SetInfoAttr.LibraryVersion => CoreArgs.lib_ver,
                _ => throw new System.NotImplementedException(),
            };

            return new SerializedCommand(RedisCoreCommands.CLIENT, RedisCoreCommands.SETINFO, attrValue, value);
        }

        /// <summary>
        /// Build a <c>SerializedCommand</c> for the BZMPOP command.
        /// </summary>
        /// <param name="keys">Keys for the sorted sets to pop from.</param>
        /// <param name="timeout">Server timeout for the blocking operation. If set to <c>0</c> then it waits
        /// indefinitely.</param>
        /// <param name="count">Maximum number of items to pop.</param>
        /// <param name="order">In what order to pop items. A value of <c>Order.Ascending</c> means to pop the minimum
        /// scores from the sorted set, a value of <c>Order.Descending</c> means to pop the maximum scores.</param>
        /// <returns>The serialized command that can be executed against the server.</returns>
        /// <remarks><seealso href="https://redis.io/commands/bzmpop"/></remarks>
        public static SerializedCommand BzmPop(RedisKey[] keys, int timeout = 0, long count = 1, Order order = Order.Ascending)
        {
            List<object> args = [
                timeout,
                keys.Length,
                .. keys.Cast<object>(),
                order == Order.Ascending ? CoreArgs.MIN : CoreArgs.MAX,
                CoreArgs.COUNT,
                count
            ];

            return new SerializedCommand(RedisCoreCommands.BZMPOP, args);
        }
    }
}
