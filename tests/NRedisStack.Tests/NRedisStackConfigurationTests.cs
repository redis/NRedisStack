using Xunit;
using NRedisStack.Core;
using static NRedisStack.Auxiliary;
using System.Net;


namespace NRedisStack.Tests.Core;

public class NRedisStackConfigurationTests : AbstractNRedisStackTest, IDisposable
{
    public NRedisStackConfigurationTests(RedisFixture redisFixture) : base(redisFixture) { }

    // TODO: add tests

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestDefaultLibName()
    {
        var options = new NRedisStackConfigurationOptions("redis://localhost");
        var db = NRedisStackConnectionMultiplexer.Connect(options).GetDatabase();
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestDefaultLibNameAsync()
    {
        var options = new NRedisStackConfigurationOptions("redis://localhost");
        var db =(await NRedisStackConnectionMultiplexer.ConnectAsync(options)).GetDatabase();
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestNRedisStackConfigurationOptions()
    {
        var options = new NRedisStackConfigurationOptions("redis://username:password@localhost:6379?allowadmin=true&clientname=client&sentinel_primary_name=sentinel&retry=3");
        Assert.Equal("username", options.GetConfigurationOptions().User);
        Assert.Equal("password", options.GetConfigurationOptions().Password);
        var endpoint = (DnsEndPoint)options.GetConfigurationOptions().EndPoints.First();
        Assert.Equal("localhost", endpoint.Host);
        Assert.Equal(6379, endpoint.Port);
        Assert.Equal("client", options.GetConfigurationOptions().ClientName);
        Assert.Equal("sentinel", options.GetConfigurationOptions().ServiceName);
        Assert.Equal(3, options.GetConfigurationOptions().ConnectRetry);
    }
}