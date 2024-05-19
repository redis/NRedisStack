using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.CuckooFilter;

public class CmsTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "CMS_TESTS";
    public CmsTests(RedisFixture redisFixture) : base(redisFixture) { }


    [Fact]
    public void TestInitByDim()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        cms.InitByDim(key, 16, 4);
        var info = cms.Info(key);

        Assert.Equal(16, info.Width);
        Assert.Equal(4, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public async Task TestInitByDimAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        await cms.InitByDimAsync(key, 16, 4);
        var info = await cms.InfoAsync(key);

        Assert.Equal(16, info.Width);
        Assert.Equal(4, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public void TestInitByProb()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        cms.InitByProb(key, 0.01, 0.01);
        var info = cms.Info(key);

        Assert.Equal(200, info.Width);
        Assert.Equal(7, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public async Task TestInitByProbAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        await cms.InitByProbAsync(key, 0.01, 0.01);
        var info = await cms.InfoAsync(key);

        Assert.Equal(200, info.Width);
        Assert.Equal(7, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Fact]
    public void TestKeyAlreadyExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        cms.InitByDim("dup", 16, 4);
        Assert.Throws<RedisServerException>(() => cms.InitByDim("dup", 8, 6));
    }

    [Fact]
    public async Task TestKeyAlreadyExistsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        await cms.InitByDimAsync("dup", 16, 4);
        await Assert.ThrowsAsync<RedisServerException>(() => cms.InitByDimAsync("dup", 8, 6));
    }

    [Fact]
    public void TestIncrBy()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        cms.InitByDim(key, 1000, 5);
        var resp = cms.IncrBy(key, "foo", 5);
        Assert.Equal(5, resp);

        var info = cms.Info(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(5, info.Count);

    }

    [Fact]
    public async Task TestIncrByAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        await cms.InitByDimAsync(key, 1000, 5);
        var resp = await cms.IncrByAsync(key, "foo", 5);
        Assert.Equal(5, resp);

        var info = await cms.InfoAsync(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(5, info.Count);

    }

    [Fact]
    public void TestIncrByMultipleArgs()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        cms.InitByDim(key, 1000, 5);
        cms.IncrBy(key, "foo", 5L);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 5);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        var resp = cms.IncrBy(key, itemIncrements);
        Assert.Equal(new long[] { 10, 15 }, resp);

        var info = cms.Info(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(25, info.Count);
    }

    [Fact]
    public async Task TestIncrByMultipleArgsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        await cms.InitByDimAsync(key, 1000, 5);
        await cms.IncrByAsync(key, "foo", 5L);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 5);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        var resp = await cms.IncrByAsync(key, itemIncrements);
        Assert.Equal(new long[] { 10, 15 }, resp);

        var info = await cms.InfoAsync(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(25, info.Count);
    }


    [Fact]
    public void TestQuery()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();
        cms.InitByDim(key, 1000, 5);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 10);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        cms.IncrBy(key, itemIncrements);

        var resp = cms.Query(key, "foo", "bar");
        Assert.Equal(new long[] { 10, 15 }, resp);
    }

    [Fact]
    public async Task TestQueryAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();
        await cms.InitByDimAsync(key, 1000, 5);

        var itemIncrements = new Tuple<RedisValue, long>[2];
        itemIncrements[0] = new Tuple<RedisValue, long>("foo", 10);
        itemIncrements[1] = new Tuple<RedisValue, long>("bar", 15);

        await cms.IncrByAsync(key, itemIncrements);

        var resp = await cms.QueryAsync(key, new RedisValue[] { "foo", "bar" });
        Assert.Equal(new long[] { 10, 15 }, resp);
    }

    [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise, Is.EnterpriseOssCluster)]
    public void TestMerge()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        cms.InitByDim("A", 1000, 5);
        cms.InitByDim("B", 1000, 5);
        cms.InitByDim("C", 1000, 5);

        var aValues = new Tuple<RedisValue, long>[3];
        aValues[0] = new Tuple<RedisValue, long>("foo", 5);
        aValues[1] = new Tuple<RedisValue, long>("bar", 3);
        aValues[2] = new Tuple<RedisValue, long>("baz", 9);

        cms.IncrBy("A", aValues);

        var bValues = new Tuple<RedisValue, long>[3];
        bValues[0] = new Tuple<RedisValue, long>("foo", 2);
        bValues[1] = new Tuple<RedisValue, long>("bar", 3);
        bValues[2] = new Tuple<RedisValue, long>("baz", 1);

        cms.IncrBy("B", bValues);

        var q1 = cms.Query("A", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 5L, 3L, 9L }, q1);

        var q2 = cms.Query("B", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 2L, 3L, 1L }, q2);

        cms.Merge("C", 2, new RedisValue[] { "A", "B" });

        var q3 = cms.Query("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 7L, 6L, 10L }, q3);

        cms.Merge("C", 2, new RedisValue[] { "A", "B" }, new long[] { 1, 2 });

        var q4 = cms.Query("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 9L, 9L, 11L }, q4);

        cms.Merge("C", 2, new RedisValue[] { "A", "B" }, new long[] { 2, 3 });


        var q5 = cms.Query("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 16L, 15L, 21L }, q5);
    }


    [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise, Is.EnterpriseOssCluster)]
    public async Task TestMergeAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.FlushAll();
        var cms = db.CMS();

        await cms.InitByDimAsync("A", 1000, 5);
        await cms.InitByDimAsync("B", 1000, 5);
        await cms.InitByDimAsync("C", 1000, 5);

        var aValues = new Tuple<RedisValue, long>[3];
        aValues[0] = new Tuple<RedisValue, long>("foo", 5);
        aValues[1] = new Tuple<RedisValue, long>("bar", 3);
        aValues[2] = new Tuple<RedisValue, long>("baz", 9);

        await cms.IncrByAsync("A", aValues);

        var bValues = new Tuple<RedisValue, long>[3];
        bValues[0] = new Tuple<RedisValue, long>("foo", 2);
        bValues[1] = new Tuple<RedisValue, long>("bar", 3);
        bValues[2] = new Tuple<RedisValue, long>("baz", 1);

        await cms.IncrByAsync("B", bValues);

        var q1 = await cms.QueryAsync("A", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 5L, 3L, 9L }, q1);

        var q2 = await cms.QueryAsync("B", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 2L, 3L, 1L }, q2);

        await cms.MergeAsync("C", 2, new RedisValue[] { "A", "B" });

        var q3 = await cms.QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 7L, 6L, 10L }, q3);

        await cms.MergeAsync("C", 2, new RedisValue[] { "A", "B" }, new long[] { 1, 2 });

        var q4 = await cms.QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 9L, 9L, 11L }, q4);

        await cms.MergeAsync("C", 2, new RedisValue[] { "A", "B" }, new long[] { 2, 3 });


        var q5 = await cms.QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
        Assert.Equal(new long[] { 16L, 15L, 21L }, q5);
    }


    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var cms1 = db1.CMS();
        var cms2 = db2.CMS();

        Assert.NotEqual(cms1.GetHashCode(), cms2.GetHashCode());
    }
}

