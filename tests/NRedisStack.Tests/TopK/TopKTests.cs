using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TopK;

public class TopKTests : AbstractNRedisStackTest, IDisposable
{
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

        //db.KeyDelete(key, CommandFlags.FireAndForget);
        topk.Reserve(key, 30, 2000, 7, 0.925);

        var res = topk.Add(key, "bb", "cc");
        Assert.True(res[0].IsNull && res[1].IsNull);

        Assert.Equal(topk.Query(key, "bb", "gg", "cc"), new bool[] { true, false, true });
        Assert.False(topk.Query(key, "notExists"));

        Assert.Equal(topk.Count(key, "bb", "gg", "cc"), new long[] { 1, 0, 1 });

        var res2 = topk.List(key);
        Assert.Equal(res2[0].ToString(), "bb");
        Assert.Equal(res2[1].ToString(), "cc");

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        var del = topk.IncrBy(key, tuple);
        Assert.True(topk.IncrBy(key, tuple)[0].IsNull);

        res2 = topk.List(key);
        Assert.Equal(res2[0].ToString(), "ff");
        Assert.Equal(res2[1].ToString(), "bb");
        Assert.Equal(res2[2].ToString(), "cc");

        var info = topk.Info(key);
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

        await topk.ReserveAsync(key, 30, 2000, 7, 0.925);

        var res = await topk.AddAsync(key, "bb", "cc");
        Assert.True(res[0].IsNull && res[1].IsNull);

        Assert.Equal(await topk.QueryAsync(key, "bb", "gg", "cc"), new bool[] { true, false, true });
        Assert.False(await topk.QueryAsync(key, "notExists"));

        Assert.Equal(await topk.CountAsync(key, "bb", "gg", "cc"), new long[] { 1, 0, 1 });

        var res2 = await topk.ListAsync(key);
        Assert.Equal(res2[0].ToString(), "bb");
        Assert.Equal(res2[1].ToString(), "cc");

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        Assert.True((await topk.IncrByAsync(key, tuple))[0].IsNull);

        res2 = await topk.ListAsync(key);
        Assert.Equal(res2[0].ToString(), "ff");
        Assert.Equal(res2[1].ToString(), "bb");
        Assert.Equal(res2[2].ToString(), "cc");

        var info = await topk.InfoAsync(key);
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
}