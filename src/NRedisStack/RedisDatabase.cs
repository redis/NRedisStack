using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

namespace NRedisStack;

internal class RedisDatabase : DatabaseWrapper, IRedisDatabase
{
    public RedisDatabase(IDatabase db) : base(db) { }

    public BloomCommands BF() => new BloomCommands(_db);

    public CuckooCommands CF() => new CuckooCommands(_db);

    public CmsCommands CMS() => new CmsCommands(_db);

    public TopKCommands TOPK() => new TopKCommands(_db);

    public TdigestCommands TDIGEST() => new TdigestCommands(_db);

    public SearchCommands FT(int? searchDialect = 2) => new SearchCommands(_db, searchDialect);

    public JsonCommands JSON() => new JsonCommands(_db);

    public TimeSeriesCommands TS() => new TimeSeriesCommands(_db);
}