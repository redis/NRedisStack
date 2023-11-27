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
        var db = ConnectionManager.Connect("redis://localhost").GetDatabase();
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestDefaultLibNameAsync()
    {
        var db = (await ConnectionManager.ConnectAsync("redis://localhost")).GetDatabase();
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestDefaultLibNameStackExchangeString()
    {
        var db = ConnectionManager.Connect("localhost").GetDatabase(); // StackExchange.Redis connection string (without the redis:// at the start)
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestDefaultLibNameStackExchangeStringAsync()
    {
        var db = (await ConnectionManager.ConnectAsync("localhost")).GetDatabase(); // StackExchange.Redis connection string (without the redis:// at the start)
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestNRedisStackConfigurationOptions()
    {
        var options = Configuration.Parse("redis://username:password@localhost:6379?allowadmin=true&clientname=client&sentinel_primary_name=sentinel&retry=3");
        Assert.Equal("username", options.GetOptions().User);
        Assert.Equal("password", options.GetOptions().Password);
        var endpoint = (DnsEndPoint)options.GetOptions().EndPoints.First();
        Assert.Equal("localhost", endpoint.Host);
        Assert.Equal(6379, endpoint.Port);
        Assert.Equal("client", options.GetOptions().ClientName);
        Assert.Equal("sentinel", options.GetOptions().ServiceName);
        Assert.Equal(3, options.GetOptions().ConnectRetry);
    }
}