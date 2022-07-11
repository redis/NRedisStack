using StackExchange.Redis;

namespace NRedisStack.Core.RedisStackCommands
{
    public static class ModulPrefixes
    {
        static public BloomCommands BF(this IDatabase db)
        {
            return new BloomCommands(db);
        }

        static public SearchCommands FT(this IDatabase db)
        {
            return new SearchCommands(db);
        }

        static public JsonCommands JSON(this IDatabase db)
        {
            return new JsonCommands(db);
        }

        static public TimeSeriesCommands TS(this IDatabase db)
        {
            return new TimeSeriesCommands(db);
        }
    }
}