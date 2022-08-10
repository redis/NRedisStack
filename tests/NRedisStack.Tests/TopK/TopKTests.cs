using Xunit;
using StackExchange.Redis;
using NRedisStack.Core.RedisStackCommands;
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

        db.TOPK().Reserve("aaa", 30, 2000, 7, 0.925);

        var res = db.TOPK().Add("aaa", "bb", "cc");
        Assert.True(res[0].IsNull && res[1].IsNull);

        Assert.Equal(db.TOPK().Query("aaa", "bb", "gg", "cc"), new bool[] { true, false, true });

        Assert.Equal(db.TOPK().Count("aaa", "bb", "gg", "cc"), new long[] { 1, 0, 1 });

        var res2 = db.TOPK().List("aaa");
        Assert.Equal(res2[0].ToString(), "bb");
        Assert.Equal(res2[1].ToString(), "cc");

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        var del = db.TOPK().IncrBy("aaa", tuple);
        Assert.True(db.TOPK().IncrBy("aaa", tuple)[0].IsNull);

        res2 = db.TOPK().List("aaa");
        Assert.Equal(res2[0].ToString(), "ff");
        Assert.Equal(res2[1].ToString(), "bb");
        Assert.Equal(res2[2].ToString(), "cc");
    }

    [Fact]
    public async void CreateTopKFilterAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        await db.ExecuteAsync("FLUSHALL");

        db.TOPK().ReserveAsync("aaa", 30, 2000, 7, 0.925);

        var res = await db.TOPK().AddAsync("aaa", "bb", "cc");
        Assert.True(res[0].IsNull && res[1].IsNull);

        Assert.Equal(await db.TOPK().QueryAsync("aaa", "bb", "gg", "cc"), new bool[] { true, false, true });

        Assert.Equal(await db.TOPK().CountAsync("aaa", "bb", "gg", "cc"), new long[] { 1, 0, 1 });

        var res2 = await db.TOPK().ListAsync("aaa");
        Assert.Equal(res2[0].ToString(), "bb");
        Assert.Equal(res2[1].ToString(), "cc");

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        Assert.True((await db.TOPK().IncrByAsync("aaa", tuple))[0].IsNull);

        res2 = await db.TOPK().ListAsync("aaa");
        Assert.Equal(res2[0].ToString(), "ff");
        Assert.Equal(res2[1].ToString(), "bb");
        Assert.Equal(res2[2].ToString(), "cc");
    }
}