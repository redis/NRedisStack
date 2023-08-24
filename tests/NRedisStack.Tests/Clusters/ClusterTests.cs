using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests;

public class ClusterTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "CLUSTER_TESTS";
    public ClusterTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().ExecuteBroadcast("FLUSHALL");
    }

    [SkipIfRedis(Is.Standalone)]
    public void ClusterConnect()
    {
        // first, run a cluster,
        ConfigurationOptions config = new ConfigurationOptions
        {
            // ports: 16379 -> 16384
            EndPoints =
            {
                { "127.0.0.1", 16379 },
                { "127.0.0.1", 16380 },
                { "127.0.0.1", 16381 },
                { "127.0.0.1", 16382 },
                { "127.0.0.1", 16383 },
                { "127.0.0.1", 16384 },
            },
        };
        var conn = ConnectionMultiplexer.Connect(config);
        IDatabase clusterDB = conn.GetDatabase();
        clusterDB.KeyDelete("foo");
        clusterDB.StringSet("foo", "bar");
        var value = clusterDB.StringGet("foo");
        Assert.Equal("bar", value);
        // var endpoints = conn.GetEndPoints();
        // Console.WriteLine("endpoints:" + endpoints.ToString());
    }

    [Fact]
    public void ClusterChecks()
    {
        var db = redisFixture.Redis.GetDatabase();
        var endpoints1 = db.Multiplexer.GetEndPoints();
        var endpoints2 = redisFixture.Redis.GetEndPoints();
        var isCtr = redisFixture.isCluster;
        var isCtr2 = "";
    }
}
