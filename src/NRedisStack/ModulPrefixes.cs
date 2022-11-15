using StackExchange.Redis;

namespace NRedisStack.RedisStackCommands
{
    public static class ModulPrefixes
    {
        static public IBloomCommands BF(this IDatabase db) => new BloomCommands(db);

        static public ICuckooCommands CF(this IDatabase db) => new CuckooCommands(db);

        static public ICmsCommands CMS(this IDatabase db) => new CmsCommands(db);

        static public TopKCommands TOPK(this IDatabase db) => new TopKCommands(db);

        static public TdigestCommands TDIGEST(this IDatabase db) => new TdigestCommands(db);

        static public SearchCommands FT(this IDatabase db) => new SearchCommands(db);

        static public IJsonCommands JSON(this IDatabase db) => new JsonCommands(db);

        static public TimeSeriesCommands TS(this IDatabase db) => new TimeSeriesCommands(db);
    }
}