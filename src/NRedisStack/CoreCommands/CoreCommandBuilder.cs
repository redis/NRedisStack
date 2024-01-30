using NRedisStack.RedisStackCommands;
using NRedisStack.Core.Literals;
using NRedisStack.Core;
using NRedisStack.Core.DataTypes;
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

        public static SerializedCommand BzmPop(int timeout, RedisKey[] keys, MinMaxModifier minMaxModifier, long? count)
        {
            if (keys.Length == 0)
            {
                throw new ArgumentException("At least one key must be provided.");
            }

            List<object> args = new List<object> {
                timeout,
                keys.Length
            };

            args.AddRange(keys.Cast<object>());

            args.Add(minMaxModifier == MinMaxModifier.Min ? CoreArgs.MIN : CoreArgs.MAX);

            if (count != null)
            {
                args.Add(CoreArgs.COUNT);
                args.Add(count);
            }

            return new SerializedCommand(RedisCoreCommands.BZMPOP, args);
        }
    }
}
