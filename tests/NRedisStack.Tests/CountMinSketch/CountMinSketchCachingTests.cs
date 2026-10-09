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

    private static async ValueTask<long> Single(RespCountMinSketch cms, RedisKey key)
    {
        using var lease = await cms.QueryAsync(key, ["foo"]);
        return lease.Span[0];
    }

    private static string Render(RedisResult result)
        => result.Resp3Type is ResultType.Array or ResultType.Map or ResultType.Set
            ? "[" + string.Join(", ", ((RedisResult[])result!).Select(Render)) + "]"
            : result.ToString() ?? "(null)";

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
        using (var first = await cms.QueryAsync(key, ["foo"])) Assert.Equal<long[]>([5], first.ToArray());
        Assert.Equal(1000, (await cms.InfoAsync(key)).Width);

        var queries = Calls(server, "cms.query");
        var infos = Calls(server, "cms.info");
        for (int i = 0; i < 50; i++)
        {
            using (var q = await cms.QueryAsync(key, ["foo"])) Assert.Equal<long[]>([5], q.ToArray());
            Assert.Equal(1000, (await cms.InfoAsync(key)).Width);
        }
        Assert.Equal(0, Calls(server, "cms.query") - queries);
        Assert.Equal(0, Calls(server, "cms.info") - infos);

        // the control: the same loop, asking for fresh answers, reaches the server every time
        queries = Calls(server, "cms.query");
        for (int i = 0; i < 50; i++)
        {
            (await cms.QueryAsync(key, ["foo"], CommandFlags.NoClientCache)).Dispose();
        }
        Assert.Equal(50, Calls(server, "cms.query") - queries);

        // the cache key covers the value arguments: a different item is a different entry, not an alias
        queries = Calls(server, "cms.query");
        using (var other = await cms.QueryAsync(key, ["bar"])) Assert.Equal<long[]>([0], other.ToArray());
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
        Assert.Equal(5, await Single(cms, key));
        Assert.Equal(5, await Single(cms, key)); // cached

        // read-your-writes: the local write drops the entry, so the next read is a miss with the new value
        var queries = Calls(server, "cms.query");
        Assert.Equal(10, await cms.IncrByAsync(key, "foo", 5));
        Assert.Equal(10, await Single(cms, key));
        Assert.Equal(1, Calls(server, "cms.query") - queries);

        await plain.KeyDeleteAsync(key);
    }

    // Observed: Redis Stack 6.2.6 (RedisBloom 2.2.x) does NOT announce CMS.INCRBY to tracking clients - the cached
    // value stays stale until the cache's TimeToLive - while 7.2 and later do. So on 6.2 a client-side cache over
    // CMS reads can serve another client's superseded value for up to TimeToLive; your own writes are still seen.
    [SkipIfRedisFact(Comparison.LessThan, "7.2.0")]
    public async Task AWriteFromAnotherClientIsAnnouncedForAModuleKey()
    {
        // the module-specific risk: invalidation only happens if the module marks its write as a key modification.
        // Redis does that when a module closes a key it opened for writing, but it is a thing to verify, not assume
        // - and, as the gate above records, older module builds do not.
        var plain = GetCleanDatabase();
        RedisKey key = Prefix + CreateKeyName();
        await plain.CountMinSketch.InitByDimAsync(key, 1000, 5);
        await plain.CountMinSketch.IncrByAsync(key, "foo", 5);

        using var conn = ConnectCached();
        var server = conn.GetServer(conn.GetEndPoints()[0]);
        var cms = conn.GetDatabase().CountMinSketch;

        Assert.Equal(5, await Single(cms, key));
        Assert.Equal(5, await Single(cms, key)); // cached

        // the write lands on the same server we are reading from: 5 + 5
        Assert.Equal(10, await plain.CountMinSketch.IncrByAsync(key, "foo", 5)); // another client

        // the push arrives shortly after the write, with no fixed bound
        var queriesBefore = Calls(server, "cms.query");
        var started = DateTime.UtcNow;
        var deadline = started.AddSeconds(5);
        long seen;
        int polls = 0;
        do
        {
            seen = await Single(cms, key);
            polls++;
            if (seen == 10) break;
            await Task.Delay(20);
        } while (DateTime.UtcNow < deadline);

        if (seen != 10)
        {
            // distinguish "the entry was never invalidated" (polls were cache hits: the server saw none of
            // them) from "the server answered 5" (polls reached the server), and show what the server
            // thinks about tracking on this connection
            var hits = Calls(server, "cms.query") - queriesBefore;
            var trackingInfo = Render(server.Execute("CLIENT", "TRACKINGINFO"));
            var info = string.Join("; ", server.Info().SelectMany(g => g).Where(p => p.Key.StartsWith("tracking_")).Select(p => $"{p.Key}={p.Value}"));
            Assert.Fail($"still {seen} after {(DateTime.UtcNow - started).TotalMilliseconds:F0}ms and {polls} polls, of which {hits} reached the server; CLIENT TRACKINGINFO: {trackingInfo}; INFO: {info}");
        }

        await plain.KeyDeleteAsync(key);
    }
}
