using StackExchange.Redis;

namespace NRedisStack.RedisStackCommands
{
    public static class ModulPrefixes
    {
        static public IBloomCommands BF(this IDatabase db) => new BloomCommands(db);

        static public ICuckooCommands CF(this IDatabase db) => new CuckooCommands(db);

        static public ICmsCommands CMS(this IDatabase db) => new CmsCommands(db);

        static public GraphCommands GRAPH(this IDatabase db) => new GraphCommands(db);

        static public ITopKCommands TOPK(this IDatabase db) => new TopKCommands(db);

        static public ITdigestCommands TDIGEST(this IDatabase db) => new TdigestCommands(db);

        static public ISearchCommands FT(this IDatabase db) => new SearchCommands(db);

        static public IJsonCommands JSON(this IDatabase db) => new JsonCommands(db);

        static public ITimeSeriesCommands TS(this IDatabase db) => new TimeSeriesCommands(db);
    }
}