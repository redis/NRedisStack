using System.Text.RegularExpressions;
using Xunit;
using StackExchange.Redis;
using StackExchange.Redis.Caching;

namespace NRedisStack.Tests.CountMinSketch;

/// <summary>
/// Client-side caching over a module's commands. <c>CMS.QUERY</c> and <c>CMS.INFO</c> are declared read-only
/// (<c>CommandRetryReadOnly</c>), which is what makes an unknown command eligible for the cache; this checks that
/// the opt-in works, that the cache key covers the value arguments, and that both invalidation paths - a local
/// write through the same multiplexer, and a remote write announced by the server - reach a module key.
/// </summary>
/// <remarks>
/// There are no public cache counters in this version, so hits are measured server-side: a read answered from the
/// cache is one the server never sees in <c>INFO commandstats</c>. Test collections do not run in parallel here,
/// so the deltas are exact.
/// </remarks>
[RunPerProtocol(RunProtocol.Resp3)] // invalidations are RESP3 pushes; a cache on RESP2 refuses to connect
public class CountMinSketchCachingTests(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    private const string Prefix = "cmscache:";

    private ConnectionMultiplexer ConnectCached()
    {
        var options = DefaultConnectionConfig.Clone();
        options.ClientCache = new CacheOptions { Prefixes = [Prefix] };
        return GetConnection(options);
    }

    private static long Calls(IServer server, string command)
    {
        foreach (var group in server.Info("commandstats"))
        {
            foreach (var pair in group)
            {
                if (pair.Key == "cmdstat_" + command)
                {
                    var m = Regex.Match(pair.Value, @"calls=(\d+)");
                    return long.Parse(m.Groups[1].Value);
                }
            }
        }
        return 0;
    }

    [Fact]
    public async Task ReadOnlyCommandsAreServedFromTheCache()
    {
        var plain = GetCleanDatabase();
        RedisKey key = Prefix + CreateKeyName();
        await plain.CountMinSketch.InitByDimAsync(key, 1000, 5);
        await plain.CountMinSketch.IncrByAsync(key, "foo", 5);

        using var conn = ConnectCached();
        var db = conn.GetDatabase();
        var server = conn.GetServer(conn.GetEndPoints()[0]);
        var cms = db.CountMinSketch;

        // tracking is on for the cached connection
        var clientList = (string)server.Execute("CLIENT", "LIST")!;
        Assert.Contains(Regex.Matches(clientList, @"flags=(\S+)").Cast<Match>(), m => m.Groups[1].Value.Contains('t'));

        // warm both entries: one round trip each
        long[] first = await cms.QueryAsync(key, ["foo"]);
        Assert.Equal<long[]>([5], first);
        Assert.Equal(1000, (await cms.InfoAsync(key)).Width);

        var queries = Calls(server, "cms.query");
        var infos = Calls(server, "cms.info");
        for (int i = 0; i < 50; i++)
        {
            long[] q = await cms.QueryAsync(key, ["foo"]);
            Assert.Equal<long[]>([5], q);
            Assert.Equal(1000, (await cms.InfoAsync(key)).Width);
        }
        Assert.Equal(0, Calls(server, "cms.query") - queries);
        Assert.Equal(0, Calls(server, "cms.info") - infos);

        // the control: the same loop, asking for fresh answers, reaches the server every time
        queries = Calls(server, "cms.query");
        for (int i = 0; i < 50; i++)
        {
            await cms.QueryAsync(key, ["foo"], CommandFlags.NoClientCache);
        }
        Assert.Equal(50, Calls(server, "cms.query") - queries);

        // the cache key covers the value arguments: a different item is a different entry, not an alias
        queries = Calls(server, "cms.query");
        long[] other = await cms.QueryAsync(key, ["bar"]);
        Assert.Equal<long[]>([0], other);
        Assert.Equal(1, Calls(server, "cms.query") - queries);

        await plain.KeyDeleteAsync(key);
    }

    [Fact]
    public async Task AWriteThroughTheSameMultiplexerInvalidatesBeforeItIsSent()
    {
        var plain = GetCleanDatabase();
        RedisKey key = Prefix + CreateKeyName();
        await plain.CountMinSketch.InitByDimAsync(key, 1000, 5);

        using var conn = ConnectCached();
        var db = conn.GetDatabase();
        var server = conn.GetServer(conn.GetEndPoints()[0]);
        var cms = db.CountMinSketch;

        Assert.Equal(5, await cms.IncrByAsync(key, "foo", 5));
        long[] q = await cms.QueryAsync(key, ["foo"]);
        Assert.Equal<long[]>([5], q);
        q = await cms.QueryAsync(key, ["foo"]); // cached
        Assert.Equal<long[]>([5], q);

        // read-your-writes: the local write drops the entry, so the next read is a miss with the new value
        var queries = Calls(server, "cms.query");
        Assert.Equal(10, await cms.IncrByAsync(key, "foo", 5));
        q = await cms.QueryAsync(key, ["foo"]);
        Assert.Equal<long[]>([10], q);
        Assert.Equal(1, Calls(server, "cms.query") - queries);

        await plain.KeyDeleteAsync(key);
    }

    [Fact]
    public async Task AWriteFromAnotherClientIsAnnouncedForAModuleKey()
    {
        // the module-specific risk: invalidation only happens if the module marks its write as a key modification.
        // Redis does that when a module closes a key it opened for writing, but it is a thing to verify, not assume.
        var plain = GetCleanDatabase();
        RedisKey key = Prefix + CreateKeyName();
        await plain.CountMinSketch.InitByDimAsync(key, 1000, 5);
        await plain.CountMinSketch.IncrByAsync(key, "foo", 5);

        using var conn = ConnectCached();
        var cms = conn.GetDatabase().CountMinSketch;

        long[] q = await cms.QueryAsync(key, ["foo"]);
        Assert.Equal<long[]>([5], q);
        q = await cms.QueryAsync(key, ["foo"]); // cached
        Assert.Equal<long[]>([5], q);

        await plain.CountMinSketch.IncrByAsync(key, "foo", 5); // another client

        // the push arrives shortly after the write, with no fixed bound
        var deadline = DateTime.UtcNow.AddSeconds(5);
        long seen;
        do
        {
            seen = (await cms.QueryAsync(key, ["foo"]))[0];
            if (seen == 10) break;
            await Task.Delay(20);
        } while (DateTime.UtcNow < deadline);
        Assert.Equal(10, seen);

        await plain.KeyDeleteAsync(key);
    }
}
