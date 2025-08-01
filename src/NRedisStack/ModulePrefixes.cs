using StackExchange.Redis;

namespace NRedisStack.RedisStackCommands;

public static class ModulePrefixes
{
    public static BloomCommands BF(this IDatabase db) => new(db);

    public static CuckooCommands CF(this IDatabase db) => new(db);

    public static CmsCommands CMS(this IDatabase db) => new(db);

    public static TopKCommands TOPK(this IDatabase db) => new(db);

    public static TdigestCommands TDIGEST(this IDatabase db) => new(db);

    public static SearchCommands FT(this IDatabase db, int? searchDialect = 2) => new(db, searchDialect);

    public static JsonCommands JSON(this IDatabase db) => new(db);

    public static TimeSeriesCommands TS(this IDatabase db) => new(db);
}