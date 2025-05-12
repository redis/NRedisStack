using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using System.Text.Json;
using NRedisStack.Search;
using System.Diagnostics;
using Microsoft.Identity.Client;
using System.Net;

namespace NRedisStack.Tests;

public class ClientInfoTests : AbstractNRedisStackTest, IDisposable
{
    public ClientInfoTests(EndpointsFixture endpointsFixture) : base(endpointsFixture)
    {
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [InlineData] // No parameters passed, but still uses Theory
    public void TestMultiplexerInfoOnReconnect()
    {
        bool reconnected = false;
        bool hang = false;
        var db = GetCleanDatabase();
        db.Multiplexer.ConnectionRestored += (sender, e) => reconnected = true;
        Assert.Contains("lib-name=SE.Redis", db.Execute("CLIENT", "INFO").ToString());
        Auxiliary.ResetInfoDefaults();
        db.FT()._List();
        Assert.Contains("lib-name=NRedisStack", db.Execute("CLIENT", "INFO").ToString());

        Stopwatch sw = new Stopwatch();
        sw.Start();
        while ((!reconnected) && !hang)
        {
            try
            {
                RedisResult clientId = db.Execute("CLIENT", "ID");
                db.ExecuteAsync("CLIENT", "KILL", "ID", ((RedisValue)clientId), "SKIPME", "NO");
            }
            catch (Exception) { }
            hang = sw.Elapsed.TotalMilliseconds > 3000.0D;
        }
        Assert.True(reconnected, "Client was not reconnected");
        Assert.False(hang, "It took more than 3 seconds, likely it hanged");
        string clientInfo = null;
        while (sw.Elapsed.Milliseconds < 4000 && clientInfo == null)
        {
            try { clientInfo = db.Execute("CLIENT", "INFO").ToString(); }
            catch (Exception) { }
        }
        Assert.Contains("lib-name=SE.Redis", clientInfo);
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [InlineData] // No parameters passed, but still uses Theory
    public void TestRedisClientInfoOnReconnect()
    {
        bool reconnected = false;
        bool hang = false;
        RedisClient rc = RedisClient.Connect(GetEndpoint());
        IRedisDatabase db = rc.GetDatabase();
        db.Multiplexer.ConnectionRestored += (sender, e) => reconnected = true;

        Stopwatch sw = new Stopwatch();
        sw.Start();
        while ((!reconnected) && !hang)
        {
            try
            {
                RedisResult clientId = db.Execute("CLIENT", "ID");
                db.ExecuteAsync("CLIENT", "KILL", "ID", ((RedisValue)clientId), "SKIPME", "NO");
            }
            catch (Exception) { }
            hang = sw.Elapsed.TotalMilliseconds > 3000.0D;
        }

        Assert.True(reconnected, "Client was not reconnected");
        Assert.False(hang, "It took more than 3 seconds, likely it hanged");
        string clientInfo = null;
        while (sw.Elapsed.Milliseconds < 4000 && clientInfo == null)
        {
            try { clientInfo = db.Execute("CLIENT", "INFO").ToString(); }
            catch (Exception) { }
        }
        Assert.Contains("lib-name=NRedisStack", clientInfo);
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [InlineData] // No parameters passed, but still uses Theory
    public void TestConnectionMultiplexerConnect()
    {
        IDatabase db = ConnectionMultiplexer.Connect(GetEndpoint()).GetDatabase();
        Auxiliary.ResetInfoDefaults();
        db.StringSetAsync("key1", "something");
        var _ = db.FT()._List();
        Assert.Contains("lib-name=NRedisStack", db.Execute("CLIENT", "INFO").ToString());
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [InlineData] // No parameters passed, but still uses Theory
    public void TestRedisClientConnect()
    {
        RedisClient rc = RedisClient.Connect(GetEndpoint());
        IRedisDatabase db = rc.GetDatabase();
        db.StringSetAsync("key1", "something");
        Assert.Contains("lib-name=NRedisStack", db.Execute("CLIENT", "INFO").ToString());
    }

    [SkipIfRedis(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [InlineData] // No parameters passed, but still uses Theory
    public void TestRedisClientConnectWithConfigOptions()
    {
        ConfigurationOptions config = new ConfigurationOptions
        {
            EndPoints = { GetEndpoint() },
            LibraryName = "TestLib",
        };

        RedisClient rc = RedisClient.Connect(config);
        IRedisDatabase db = rc.GetDatabase();
        db.StringSetAsync("key1", "something");
        Assert.Contains("lib-name=TestLib", db.Execute("CLIENT", "INFO").ToString());
    }

    private string GetEndpoint()
    {
        var ep = GetCleanDatabase().Multiplexer.GetEndPoints()[0];
        String endpoint = null;
        if (ep is IPEndPoint iep)
        {
            endpoint = $"{iep.Address}:{iep.Port}";
        }
        else if (ep is DnsEndPoint dep)
        {
            endpoint = $"{dep.Host}:{dep.Port}";
        }
        return endpoint;
    }
}