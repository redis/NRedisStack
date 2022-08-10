using Xunit;
using StackExchange.Redis;
using NRedisStack.Core.RedisStackCommands;
using Moq;

namespace NRedisStack.Tests.CuckooFilter;

public class CmsTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "CMS_TESTS";
    public CmsTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void TestInitByDim()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CMS().InitByDim(key, 16, 4);
        var info = db.CMS().Info(key);

        Assert.Equal(16, info.Width);
        Assert.Equal(4, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public async void TestInitByDimAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        await db.ExecuteAsync("FLUSHALL");

        await db.CMS().InitByDimAsync(key, 16, 4);
        var info = await db.CMS().InfoAsync(key);

        Assert.Equal(16, info.Width);
        Assert.Equal(4, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public void TestInitByProb()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CMS().InitByProb(key, 0.01, 0.01);
        var info = db.CMS().Info(key);

        Assert.Equal(200, info.Width);
        Assert.Equal(7, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public async void TestInitByProbAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        await db.ExecuteAsync("FLUSHALL");

        await db.CMS().InitByProbAsync(key, 0.01, 0.01);
        var info = await db.CMS().InfoAsync(key);

        Assert.Equal(200, info.Width);
        Assert.Equal(7, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public void TestKeyAlreadyExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CMS().InitByDim("dup", 16, 4);
        Assert.Throws<RedisServerException>(() => db.CMS().InitByDim("dup", 8, 6));
    }

    [Fact]
    public async void TestKeyAlreadyExistsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL"); //TODO: Check if I need Execute("FLUSHALL") to be Async in the async test

        await db.CMS().InitByDimAsync("dup", 16, 4);
        await Assert.ThrowsAsync<RedisServerException>(() => db.CMS().InitByDimAsync("dup", 8, 6));
    }

    [Fact]
    public void TestIncrBy()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CMS().InitByDim(key, 1000, 5);
        var resp = db.CMS().IncrBy(key, "foo", 5);
        Assert.Equal(5, resp);

        var info = db.CMS().Info(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(5, info.Count);

    }

    [Fact]
    public async void TestIncrByAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.CMS().InitByDimAsync(key, 1000, 5);
        var resp = await db.CMS().IncrByAsync(key, "foo", 5);
        Assert.Equal(5, resp);

        var info = await db.CMS().InfoAsync(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(5, info.Count);

    }

    [Fact]
    public void TestIncrByMultipleArgs()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CMS().InitByDim(key, 1000, 5);
        db.CMS().IncrBy(key, "foo", 5L);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 5);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        var resp = db.CMS().IncrBy(key, itemIncrements);
        Assert.Equal(new long[] { 10, 15 }, resp);

        var info = db.CMS().Info(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(25, info.Count);
    }

    [Fact]
    public async void TestIncrByMultipleArgsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.CMS().InitByDimAsync(key, 1000, 5);
        await db.CMS().IncrByAsync(key, "foo", 5L);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 5);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        var resp = await db.CMS().IncrByAsync(key, itemIncrements);
        Assert.Equal(new long[] { 10, 15 }, resp);

        var info = await db.CMS().InfoAsync(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(25, info.Count);
    }


    [Fact]
    public void TestQuery()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        db.CMS().InitByDim(key, 1000, 5);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 10);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        db.CMS().IncrBy(key, itemIncrements);

        var resp = db.CMS().Query(key, new RedisValue[] { "foo", "bar" });
        Assert.Equal(new long[] { 10, 15 }, resp);
    }

    [Fact]
    public async void TestQueryAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        await db.CMS().InitByDimAsync(key, 1000, 5);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 10);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        await db.CMS().IncrByAsync(key, itemIncrements);

        var resp = await db.CMS().QueryAsync(key, new RedisValue[] { "foo", "bar" });
        Assert.Equal(new long[] { 10, 15 }, resp);
    }

    [Fact]
    public void TestMerge()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CMS().InitByDim("A", 1000, 5);
        db.CMS().InitByDim("B", 1000, 5);
        db.CMS().InitByDim("C", 1000, 5);

        var aValues = new Tuple<RedisValue, long>[3];
        aValues[0] = new Tuple<RedisValue, long>("foo", 5);
        aValues[1] = new Tuple<RedisValue, long>("bar", 3);
        aValues[2] = new Tuple<RedisValue, long>("baz", 9);

        db.CMS().IncrBy("A", aValues);

        var bValues = new Tuple<RedisValue, long>[3];
        bValues[0] = new Tuple<RedisValue, long>("foo", 2);
        bValues[1] = new Tuple<RedisValue, long>("bar", 3);
        bValues[2] = new Tuple<RedisValue, long>("baz", 1);

        db.CMS().IncrBy("B", bValues);

        var q1 = db.CMS().Query("A", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 5L, 3L, 9L }, q1);

        var q2 = db.CMS().Query("B", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 2L, 3L, 1L }, q2);

        db.CMS().Merge("C", 2, new RedisValue[] { "A", "B" });

        var q3 = db.CMS().Query("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 7L, 6L, 10L }, q3);

        db.CMS().Merge("C", 2, new RedisValue[] { "A", "B" }, new long[] { 1, 2 });

        var q4 = db.CMS().Query("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 9L, 9L, 11L }, q4);

        db.CMS().Merge("C", 2, new RedisValue[] { "A", "B" }, new long[] { 2, 3 });


        var q5 = db.CMS().Query("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 16L, 15L, 21L }, q5);
    }


    [Fact]
    public async void TestMergeAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.CMS().InitByDimAsync("A", 1000, 5);
        await db.CMS().InitByDimAsync("B", 1000, 5);
        await db.CMS().InitByDimAsync("C", 1000, 5);

        var aValues = new Tuple<RedisValue, long>[3];
        aValues[0] = new Tuple<RedisValue, long>("foo", 5);
        aValues[1] = new Tuple<RedisValue, long>("bar", 3);
        aValues[2] = new Tuple<RedisValue, long>("baz", 9);

        await db.CMS().IncrByAsync("A", aValues);

        var bValues = new Tuple<RedisValue, long>[3];
        bValues[0] = new Tuple<RedisValue, long>("foo", 2);
        bValues[1] = new Tuple<RedisValue, long>("bar", 3);
        bValues[2] = new Tuple<RedisValue, long>("baz", 1);

        await db.CMS().IncrByAsync("B", bValues);

        var q1 = await db.CMS().QueryAsync("A", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 5L, 3L, 9L }, q1);

        var q2 = await db.CMS().QueryAsync("B", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 2L, 3L, 1L }, q2);

        await db.CMS().MergeAsync("C", 2, new RedisValue[] { "A", "B" });

        var q3 = await db.CMS().QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 7L, 6L, 10L }, q3);

        await db.CMS().MergeAsync("C", 2, new RedisValue[] { "A", "B" }, new long[] { 1, 2 });

        var q4 = await db.CMS().QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 9L, 9L, 11L }, q4);

        await db.CMS().MergeAsync("C", 2, new RedisValue[] { "A", "B" }, new long[] { 2, 3 });


        var q5 = await db.CMS().QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 16L, 15L, 21L }, q5);
    }
}

