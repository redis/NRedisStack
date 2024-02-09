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
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestConnectionWithConfigurationOptionsAsync()
    {
        var SEconfigOptions = new ConfigurationOptions() { EndPoints = { "localhost" } };
        var db = (await ConnectionManager.ConnectAsync(SEconfigOptions)).GetDatabase();
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestDefaultLibName()
    {
        var db = ConnectionManager.Connect("redis://localhost").GetDatabase();
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestDefaultLibNameAsync()
    {
        var db = (await ConnectionManager.ConnectAsync("redis://localhost")).GetDatabase();
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestLibNameSet()
    {
        var configuration = Configuration.Parse("redis://localhost?lib_name=MyLib");
        var db = ConnectionManager.Connect(configuration).GetDatabase();
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(MyLib;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestLibNameSetAsync()
    {
        var configuration = Configuration.Parse("redis://localhost?lib_name=MyLib");
        var db = (await ConnectionManager.ConnectAsync(configuration)).GetDatabase();
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(MyLib;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestDefaultLibNameStackExchangeString()
    {
        var db = ConnectionManager.Connect("localhost").GetDatabase(); // StackExchange.Redis connection string (without the redis:// at the start)
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestDefaultLibNameStackExchangeStringAsync()
    {
        var db = (await ConnectionManager.ConnectAsync("localhost")).GetDatabase(); // StackExchange.Redis connection string (without the redis:// at the start)
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, GetNRedisStackVersion()); // delete this line after the library version will be available and auto set
        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    #region Configuration parsing tests
    [Fact]
    public void TestNRedisStackConfigurationOptions()
    {
        var configuration = Configuration.Parse("redis://username:password@localhost:6379?allowadmin=true&clientname=client&sentinel_primary_name=sentinel&retry=3&timeout=1000&abortconnect=false&asynctimeout=1000&protocol=2");
        Assert.Equal("username", configuration.Options.User);
        Assert.Equal("password", configuration.Options.Password);
        var endpoint = (DnsEndPoint)configuration.Options.EndPoints.First();
        Assert.Equal("localhost", endpoint.Host);
        Assert.Equal(6379, endpoint.Port);
        Assert.Equal("client", configuration.Options.ClientName);
        Assert.Equal("sentinel", configuration.Options.ServiceName);
        Assert.Equal(3, configuration.Options.ConnectRetry);
        Assert.Equal(1000, configuration.Options.ConnectTimeout);
        Assert.Equal(1000, configuration.Options.AsyncTimeout);
        Assert.True(configuration.Options.AllowAdmin);
        Assert.False(configuration.Options.AbortOnConnectFail);
    }

    [Fact]
    public void TestRespConfiguration()
    {
        var configuration = Configuration.Parse("redis://localhost:6379?protocol=2");
        Assert.Equal(RedisProtocol.Resp2, configuration.Options.Protocol);
        configuration = Configuration.Parse("redis://localhost:6379?protocol=3");
        Assert.Equal(RedisProtocol.Resp3, configuration.Options.Protocol);
        Assert.Throws<FormatException>(() => Configuration.Parse("redis://localhost:6379?protocol=0"));
    }

    [Fact]
    public void TestWithMultipleEndpoints()
    {
        var configuration = Configuration.Parse("rediss://username:password@localhost:6379?endpoint=notSoLocalHost:6379&endpoint=reallyNotSoLocalHost:6379");
        Assert.True(configuration.Options.EndPoints.Any(x => x is DnsEndPoint endpoint && endpoint.Host == "notSoLocalHost" && endpoint.Port == 6379)!);
        Assert.True(configuration.Options.EndPoints.Any(x => x is DnsEndPoint endpoint && endpoint.Host == "reallyNotSoLocalHost" && endpoint.Port == 6379)!);
    }

    [Fact]
    public void TestEmptyUri()
    {
        var configuration = Configuration.Parse("");
        Assert.Equal("localhost", ((DnsEndPoint)configuration.Options.EndPoints.First()).Host);
        Assert.Equal(6379, ((DnsEndPoint)configuration.Options.EndPoints.First()).Port);
    }
    #endregion
}