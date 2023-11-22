using Xunit;
using NRedisStack.Core;
using static NRedisStack.Auxiliary;


namespace NRedisStack.Tests.Core;

public class NRedisStackConfigurationTests : AbstractNRedisStackTest, IDisposable
{
    public NRedisStackConfigurationTests(RedisFixture redisFixture) : base(redisFixture) { }

    // TODO: add tests

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestDefaultLibName()
    {
        var options = RedisUriParser.ParseConfigFromUri("redis://localhost");
        var db = NRedisStackConnectionMultiplexer.Connect(options).GetDatabase();
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }
}