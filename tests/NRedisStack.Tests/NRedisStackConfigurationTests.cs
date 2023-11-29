using Xunit;
using NRedisStack.Core;
using static NRedisStack.Auxiliary;
using System.Net;
using StackExchange.Redis;
using System.Runtime.InteropServices;


namespace NRedisStack.Tests.Core;

public class NRedisStackConfigurationTests : AbstractNRedisStackTest, IDisposable
{
    public NRedisStackConfigurationTests(RedisFixture redisFixture) : base(redisFixture) { }

    // TODO: add tests

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestConnectionWithConfigurationOptions()
    {
        var SEconfigOptions = new ConfigurationOptions() { EndPoints = { "localhost" } };
        var db = ConnectionManager.Connect(SEconfigOptions).GetDatabase();
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestConnectionWithConfigurationOptionsAsync()
    {
        var SEconfigOptions = new ConfigurationOptions() { EndPoints = { "localhost" } };
        var db = (await ConnectionManager.ConnectAsync(SEconfigOptions)).GetDatabase();
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

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

    #region Configuration parsing tests
    [Fact]
    public void TestNRedisStackConfigurationOptions()
    {
        var options = Configuration.Parse("redis://username:password@localhost:6379?allowadmin=true&clientname=client&sentinel_primary_name=sentinel&retry=3&timeout=1000&abortconnect=false&asynctimeout=1000&protocol=2");
        Assert.Equal("username", options.GetOptions().User);
        Assert.Equal("password", options.GetOptions().Password);
        var endpoint = (DnsEndPoint)options.GetOptions().EndPoints.First();
        Assert.Equal("localhost", endpoint.Host);
        Assert.Equal(6379, endpoint.Port);
        Assert.Equal("client", options.GetOptions().ClientName);
        Assert.Equal("sentinel", options.GetOptions().ServiceName);
        Assert.Equal(3, options.GetOptions().ConnectRetry);
        Assert.Equal(1000, options.GetOptions().ConnectTimeout);
        Assert.Equal(1000, options.GetOptions().AsyncTimeout);
        Assert.True(options.GetOptions().AllowAdmin);
        Assert.False(options.GetOptions().AbortOnConnectFail);
    }

    [Fact]
    public void TestRespConfiguration()
    {
        var options = Configuration.Parse("redis://localhost:6379?protocol=2");
        // options.GetOptions().Protocol = RedisProtocol.Resp2;
        Assert.Equal(RedisProtocol.Resp2, options.GetOptions().Protocol);
        options = Configuration.Parse("redis://localhost:6379?protocol=3");
        Assert.Equal(RedisProtocol.Resp3, options.GetOptions().Protocol);
        Assert.Throws<FormatException>(() => Configuration.Parse("redis://localhost:6379?protocol=0"));
    }

    [Fact]
    public void TestWithMultipleEndpoints()
    {
        var options = Configuration.Parse("rediss://username:password@localhost:6379?endpoint=notSoLocalHost:6379&endpoint=reallyNotSoLocalHost:6379");
        Assert.True(options.GetOptions().EndPoints.Any(x => x is DnsEndPoint endpoint && endpoint.Host == "notSoLocalHost" && endpoint.Port == 6379)!);
        Assert.True(options.GetOptions().EndPoints.Any(x => x is DnsEndPoint endpoint && endpoint.Host == "reallyNotSoLocalHost" && endpoint.Port == 6379)!);
    }

    [Fact]
    public void TestEmptyUri()
    {
        var options = Configuration.Parse("");
        Assert.Equal("localhost", ((DnsEndPoint)options.GetOptions().EndPoints.First()).Host);
        Assert.Equal(6379, ((DnsEndPoint)options.GetOptions().EndPoints.First()).Port);
    }
    #endregion
}