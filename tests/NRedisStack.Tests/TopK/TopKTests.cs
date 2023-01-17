using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;

namespace NRedisStack.Tests.TopK;

public class TopKTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "TOPK_TESTS";
    public TopKTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void CreateTopKFilter()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var topk = db.TOPK();

        topk.Reserve("aaa", 30, 2000, 7, 0.925);

        var res = topk.Add("aaa", "bb", "cc");
        Assert.True(res[0].IsNull && res[1].IsNull);

        Assert.Equal(topk.Query("aaa", "bb", "gg", "cc"), new bool[] { true, false, true });
        Assert.False(topk.Query("aaa", "notExists"));

        Assert.Equal(topk.Count("aaa", "bb", "gg", "cc"), new long[] { 1, 0, 1 });

        var res2 = topk.List("aaa");
        Assert.Equal(res2[0].ToString(), "bb");
        Assert.Equal(res2[1].ToString(), "cc");

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        var del = topk.IncrBy("aaa", tuple);
        Assert.True(topk.IncrBy("aaa", tuple)[0].IsNull);

        res2 = topk.List("aaa");
        Assert.Equal(res2[0].ToString(), "ff");
        Assert.Equal(res2[1].ToString(), "bb");
        Assert.Equal(res2[2].ToString(), "cc");

        var info = topk.Info("aaa");
        Assert.Equal(info.Decay, 0.925);
        Assert.Equal(info.Depth, 7);
        Assert.Equal(info.K, 30);
        Assert.Equal(info.Width, 2000);
    }

    [Fact]
    public async Task CreateTopKFilterAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var topk = db.TOPK();

        await topk.ReserveAsync("aaa", 30, 2000, 7, 0.925);

        var res = await topk.AddAsync("aaa", "bb", "cc");
        Assert.True(res[0].IsNull && res[1].IsNull);

        Assert.Equal(await topk.QueryAsync("aaa", "bb", "gg", "cc"), new bool[] { true, false, true });
        Assert.False(await topk.QueryAsync("aaa", "notExists"));

        Assert.Equal(await topk.CountAsync("aaa", "bb", "gg", "cc"), new long[] { 1, 0, 1 });

        var res2 = await topk.ListAsync("aaa");
        Assert.Equal(res2[0].ToString(), "bb");
        Assert.Equal(res2[1].ToString(), "cc");

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        Assert.True((await topk.IncrByAsync("aaa", tuple))[0].IsNull);

        res2 = await topk.ListAsync("aaa");
        Assert.Equal(res2[0].ToString(), "ff");
        Assert.Equal(res2[1].ToString(), "bb");
        Assert.Equal(res2[2].ToString(), "cc");

        var info = await topk.InfoAsync("aaa");
        Assert.Equal(info.Decay, 0.925);
        Assert.Equal(info.Depth, 7);
        Assert.Equal(info.K, 30);
        Assert.Equal(info.Width, 2000);
    }

    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var topk1 = db1.TOPK();
        var topk2 = db2.TOPK();

        Assert.NotEqual(topk1.GetHashCode(), topk2.GetHashCode());
    }

    [Fact]
    public void TestModulePrefixs1()
    {
        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var topk = db.TOPK();
            // ...
            conn.Dispose();
        }

        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var topk = db.TOPK();
            // ...
            conn.Dispose();
        }

    }
}