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

        public static SerializedCommand BZMPop(double timeout, RedisKey[] keys, MinMaxModifier minMaxModifier, long? count)
        {
            if (keys.Length == 0)
            {
                throw new ArgumentException("At least one key must be provided.");
            }

            List<object> args = new List<object>();

            args.Add(timeout);
            args.Add(keys.Length);
            args.AddRange(keys.Cast<object>());
            args.Add(minMaxModifier == MinMaxModifier.Min ? CoreArgs.MIN : CoreArgs.MAX);

            if (count != null)
            {
                args.Add(CoreArgs.COUNT);
                args.Add(count);
            }

            return new SerializedCommand(RedisCoreCommands.BZMPOP, args);
        }

        public static SerializedCommand BZPopMin(RedisKey[] keys, double timeout)
        {
            return BlockingCommandWithKeysAndTimeout(RedisCoreCommands.BZPOPMIN, keys, timeout);
        }

        public static SerializedCommand BZPopMax(RedisKey[] keys, double timeout)
        {
            return BlockingCommandWithKeysAndTimeout(RedisCoreCommands.BZPOPMAX, keys, timeout);
        }

        public static SerializedCommand BLMPop(double timeout, RedisKey[] keys, ListSide listSide, long? count)
        {
            if (keys.Length == 0)
            {
                throw new ArgumentException("At least one key must be provided.");
            }

            List<object> args = new List<object>();

            args.Add(timeout);
            args.Add(keys.Length);
            args.AddRange(keys.Cast<object>());
            args.Add(listSide == ListSide.Left ? CoreArgs.LEFT : CoreArgs.RIGHT);

            if (count != null)
            {
                args.Add(CoreArgs.COUNT);
                args.Add(count);
            }

            return new SerializedCommand(RedisCoreCommands.BLMPOP, args);
        }

        public static SerializedCommand BLPop(RedisKey[] keys, double timeout)
        {
            return BlockingCommandWithKeysAndTimeout(RedisCoreCommands.BLPOP, keys, timeout);
        }

        public static SerializedCommand BRPop(RedisKey[] keys, double timeout)
        {
            return BlockingCommandWithKeysAndTimeout(RedisCoreCommands.BRPOP, keys, timeout);
        }

        public static SerializedCommand BLMove(RedisKey source, RedisKey destination, ListSide sourceSide, ListSide destinationSide, double timeout)
        {
            List<object> args = new List<object>();
            args.Add(source);
            args.Add(destination);
            args.Add(sourceSide == ListSide.Left ? CoreArgs.LEFT : CoreArgs.RIGHT);
            args.Add(destinationSide == ListSide.Left ? CoreArgs.LEFT : CoreArgs.RIGHT);
            args.Add(timeout);

            return new SerializedCommand(RedisCoreCommands.BLMOVE, args);
        }

        public static SerializedCommand BRPopLPush(RedisKey source, RedisKey destination, double timeout)
        {
            List<object> args = new List<object>();
            args.Add(source);
            args.Add(destination);
            args.Add(timeout);

            return new SerializedCommand(RedisCoreCommands.BRPOPLPUSH, args);
        }

        public static SerializedCommand XRead(RedisKey[] keys, RedisValue[] positions, int? count, int? timeoutMilliseconds)
        {
            if (keys.Length == 0)
            {
                throw new ArgumentException("At least one key must be provided.");
            }

            if (keys.Length != positions.Length)
            {
                throw new ArgumentException("The number of keys and positions must be the same.");
            }

            List<object> args = new List<object>();

            if (count != null)
            {
                args.Add(CoreArgs.COUNT);
                args.Add(count);
            }

            if (timeoutMilliseconds != null)
            {
                args.Add(CoreArgs.BLOCK);
                args.Add(timeoutMilliseconds);
            }

            args.Add(CoreArgs.STREAMS);
            args.AddRange(keys.Cast<object>());
            args.AddRange(positions.Cast<object>());

            return new SerializedCommand(RedisCoreCommands.XREAD, args);
        }

        public static SerializedCommand XReadGroup(RedisValue groupName, RedisValue consumerName, RedisKey[] keys, RedisValue[] positions, int? count, int? timeoutMilliseconds, bool? noAcknowledge)
        {
            if (keys.Length == 0)
            {
                throw new ArgumentException("At least one key must be provided.");
            }

            if (keys.Length != positions.Length)
            {
                throw new ArgumentException("The number of keys and positions must be the same.");
            }

            List<object> args = new List<object>();

            args.Add(CoreArgs.GROUP);
            args.Add(groupName);
            args.Add(consumerName);

            if (count != null)
            {
                args.Add(CoreArgs.COUNT);
                args.Add(count);
            }

            if (timeoutMilliseconds != null)
            {
                args.Add(CoreArgs.BLOCK);
                args.Add(timeoutMilliseconds);
            }

            if (noAcknowledge != null && noAcknowledge.Value)
            {
                args.Add(CoreArgs.NOACK);
            }

            args.Add(CoreArgs.STREAMS);
            args.AddRange(keys.Cast<object>());
            args.AddRange(positions.Cast<object>());

            return new SerializedCommand(RedisCoreCommands.XREADGROUP, args);
        }

        private static SerializedCommand BlockingCommandWithKeysAndTimeout(String command, RedisKey[] keys, double timeout)
        {
            if (keys.Length == 0)
            {
                throw new ArgumentException("At least one key must be provided.");
            }

            List<object> args = new List<object>();
            args.AddRange(keys.Cast<object>());
            args.Add(timeout);

            return new SerializedCommand(command, args);
        }
    }
}
