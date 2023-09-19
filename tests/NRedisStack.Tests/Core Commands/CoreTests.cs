using Xunit;
using NRedisStack.Core;
using NRedisStack;
using static NRedisStack.Auxiliary;
using StackExchange.Redis;
using System.Xml.Linq;
using System.Reflection;


namespace NRedisStack.Tests.Core;

public class CoreTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "CORE_TESTS";
    public CoreTests(RedisFixture redisFixture) : base(redisFixture) { }

    // [SkipIfRedis(Comparison.LessThan, "7.1.242")]
    // public void TestSetInfo()
    // {
    //     var redis = ConnectionMultiplexer.Connect("localhost");
    //     var db = redis.GetDatabase("name"); // TODO: find a way that the user will not need to pass the library name and version if he wants to use the default values.

    //     db.Execute("FLUSHALL");
    //     var info = db.Execute("CLIENT", "INFO").ToString();
    //     Assert.EndsWith($"lib-name=NRedisStack(SE.Redis-v{GetStackExchangeRedisVersion()};.NET-{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);

    //     Assert.True(db.ClientSetInfo(SetInfoAttr.LibraryName, "anylibname"));
    //     Assert.True(db.ClientSetInfo(SetInfoAttr.LibraryVersion, "1.2.3"));
    //     info = db.Execute("CLIENT", "INFO").ToString();
    //     Assert.EndsWith("lib-name=anylibname lib-ver=1.2.3\n", info);
    // }

    // [SkipIfRedis(Comparison.LessThan, "7.1.242")]
    // public async Task TestSetInfoAsync()
    // {
    //     var redis = ConnectionMultiplexer.Connect("localhost");

    //     var db = redis.GetDatabase(""); // TODO: find a way that the user will not need to pass the library name and version if he wants to use the default values.
    //     db.Execute("FLUSHALL");
    //     var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
    //     Assert.EndsWith($"lib-name=NRedisStack(SE.Redis-v{GetStackExchangeRedisVersion()};.NET-{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);

    //     Assert.True(await db.ClientSetInfoAsync(SetInfoAttr.LibraryName, "anylibname"));
    //     Assert.True(await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, "1.2.3"));
    //     info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
    //     Assert.EndsWith("lib-name=anylibname lib-ver=1.2.3\n", info);
    // }
}