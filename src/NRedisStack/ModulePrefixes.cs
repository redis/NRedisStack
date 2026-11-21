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

    // The async-only counterparts, for databases that are not an IDatabase: a retry-wrapped database
    // (WithRetry), a batch, or a transaction. An IDatabase still binds to the overloads above, being the
    // more specific receiver, so existing calls keep getting the sync-capable commands.

    public static BloomCommandsAsync BF(this IDatabaseAsync db) => new(db);

    public static CuckooCommandsAsync CF(this IDatabaseAsync db) => new(db);

    public static CmsCommandsAsync CMS(this IDatabaseAsync db) => new(db);

    public static TopKCommandsAsync TOPK(this IDatabaseAsync db) => new(db);

    public static TdigestCommandsAsync TDIGEST(this IDatabaseAsync db) => new(db);

    public static SearchCommandsAsync FT(this IDatabaseAsync db, int? searchDialect = 2) => new(db, searchDialect);

    public static JsonCommandsAsync JSON(this IDatabaseAsync db) => new(db);

    public static TimeSeriesCommandsAsync TS(this IDatabaseAsync db) => new(db);
}
