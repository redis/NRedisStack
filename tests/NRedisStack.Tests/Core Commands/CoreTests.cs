using Xunit;
using NRedisStack.Core;
using StackExchange.Redis;

namespace NRedisStack.Tests.Core;

public class CoreTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "CORE_TESTS";
    public CoreTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [SkipIfRedisVersion(Comparison.LessThan, "7.1.242")]
    public void TestSetInfo()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase();

        db.Execute("FLUSHALL");
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith("lib-name=SE.Redis lib-ver=2.6.122.38350\n", info);

        Assert.True(db.ClientSetInfo(SetInfoAttr.LibraryName, "anylibname"));
        Assert.True(db.ClientSetInfo(SetInfoAttr.LibraryVersion, "1.2.3"));
        info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith("lib-name=anylibname lib-ver=1.2.3\n", info);
    }

    [SkipIfRedisVersion(Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoAsync()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");
        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith("lib-name=SE.Redis lib-ver=2.6.122.38350\n", info);

        Assert.True( await db.ClientSetInfoAsync(SetInfoAttr.LibraryName, "anylibname"));
        Assert.True( await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, "1.2.3"));
        info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith("lib-name=anylibname lib-ver=1.2.3\n", info);
    }
}