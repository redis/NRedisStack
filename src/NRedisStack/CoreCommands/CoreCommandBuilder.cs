using NRedisStack.RedisStackCommands;
using NRedisStack.Core.Literals;
using NRedisStack.Core;

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
    }
}
