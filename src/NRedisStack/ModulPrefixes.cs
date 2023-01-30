using StackExchange.Redis;

namespace NRedisStack.RedisStackCommands
{
    public static class ModulPrefixes
    {
        public static BloomCommands BF(this IDatabase db) => new BloomCommands(db);

        public static CuckooCommands CF(this IDatabase db) => new CuckooCommands(db);

        public static CmsCommands CMS(this IDatabase db) => new CmsCommands(db);

        public static GraphCommands GRAPH(this IDatabase db) => new GraphCommands(db);

        public static TopKCommands TOPK(this IDatabase db) => new TopKCommands(db);

        public static TdigestCommands TDIGEST(this IDatabase db) => new TdigestCommands(db);

        public static SearchCommands FT(this IDatabase db) => new SearchCommands(db);

        public static JsonCommands JSON(this IDatabase db) => new JsonCommands(db);

        public static TimeSeriesCommands TS(this IDatabase db) => new TimeSeriesCommands(db);
    }
}