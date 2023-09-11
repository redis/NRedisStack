using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.CuckooFilter;

public class CuckooTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "CUCKOO_TESTS";
    public CuckooTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().ExecuteBroadcast("FLUSHALL");
    }

    [Fact]
    public void TestReserveBasic()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();
        Assert.True(cf.Reserve(key, 100L, maxIterations: 20, expansion: 1));
        Assert.Throws<RedisServerException>(() => cf.Reserve(key, 100L));

        Assert.True((cf.Add(key, "item1")));
        Assert.True(cf.Exists(key, "item1"));
        Assert.False(cf.Exists(key, "item2"));
    }

    [Fact]
    public async Task TestReserveBasicAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();
        Assert.True(await cf.ReserveAsync(key, 100L, maxIterations: 20, expansion: 1));
        _ = Assert.ThrowsAsync<RedisServerException>(async () => await cf.ReserveAsync(key, 100L));

        Assert.True(await (cf.AddAsync(key, "item1")));
        Assert.True(await cf.ExistsAsync(key, "item1"));
        Assert.False(await cf.ExistsAsync(key, "item2"));
    }

    [Fact]
    public void TestAddExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        Assert.True(cf.Add(key, "item1"));
        Assert.True(cf.Exists(key, "item1"));
    }

    [Fact]
    public async Task TestAddExistsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        Assert.True(await cf.AddAsync(key, "item1"));
        Assert.True(await cf.ExistsAsync(key, "item1"));
    }

    [Fact]
    public void TestAddNX()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        Assert.True(cf.AddNX(key, "item1"));
        Assert.False(cf.AddNX(key, "item1"));
        Assert.True(cf.Exists(key, "item1"));
    }

    [Fact]
    public async Task TestAddNXAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        Assert.True(await cf.AddNXAsync(key, "item1"));
        Assert.False(await cf.AddNXAsync(key, "item1"));
        Assert.True(await cf.ExistsAsync(key, "item1"));
    }

    [Fact]
    public void TestCountFilterDoesNotExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        Assert.Equal(0, cf.Count("notExistFilter", "notExistItem"));
    }

    [Fact]
    public async Task TestCountFilterDoesNotExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        Assert.Equal(0, await cf.CountAsync("notExistFilter", "notExistItem"));
    }

    [Fact]
    public void TestCountFilterExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        cf.Insert(key, new RedisValue[] { "foo" });
        Assert.Equal(0, cf.Count(key, "notExistItem"));
    }

    [Fact]
    public async Task TestCountFilterExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        await cf.InsertAsync(key, new RedisValue[] { "foo" });
        Assert.Equal(0, await cf.CountAsync(key, "notExistItem"));
    }

    [Fact]
    public void TestCountItemExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        cf.Insert(key, new RedisValue[] { "foo" });
        Assert.Equal(1, cf.Count(key, "foo"));
    }

    [Fact]
    public async Task TestCountItemExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        await cf.InsertAsync(key, new RedisValue[] { "foo" });
        Assert.Equal(1, await cf.CountAsync(key, "foo"));
    }

    [Fact]
    public void TestDelete()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        cf.Add(key, "item");
        Assert.False(cf.Del(key, "notExistsItem"));
        Assert.True(cf.Del(key, "item"));

        Assert.Throws<RedisServerException>(() => cf.Del("notExistKey", "item"));
    }

    [Fact]
    public async Task TestDeleteAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        await cf.AddAsync(key, "item");
        Assert.False(await cf.DelAsync(key, "notExistsItem"));
        Assert.True(await cf.DelAsync(key, "item"));

        await Assert.ThrowsAsync<RedisServerException>(() => cf.DelAsync("notExistKey", "item"));
    }

    [Fact]
    public void TestInfo()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        cf.Add(key, "item");
        var info = cf.Info(key);

        Assert.NotNull(info);
        Assert.Equal(info.BucketSize, (long)2);
        Assert.Equal(info.ExpansionRate, (long)1);
        Assert.Equal(info.MaxIterations, (long)20);
        Assert.Equal(info.NumberOfBuckets, (long)512);
        Assert.Equal(info.NumberOfFilters, (long)1);
        Assert.Equal(info.NumberOfItemsDeleted, (long)0);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);
        Assert.Equal(info.Size, (long)1080);

        Assert.Throws<RedisServerException>(() => cf.Info("notExistKey"));
    }

    [Fact]
    public async Task TestInfoAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        await cf.AddAsync(key, "item");
        var info = await cf.InfoAsync(key);

        Assert.NotNull(info);
        Assert.Equal(info.BucketSize, (long)2);
        Assert.Equal(info.ExpansionRate, (long)1);
        Assert.Equal(info.MaxIterations, (long)20);
        Assert.Equal(info.NumberOfBuckets, (long)512);
        Assert.Equal(info.NumberOfFilters, (long)1);
        Assert.Equal(info.NumberOfItemsDeleted, (long)0);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);
        Assert.Equal(info.Size, (long)1080);



        await Assert.ThrowsAsync<RedisServerException>(() => cf.InfoAsync("notExistKey"));
    }

    [Fact]
    public void TestInsert()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        cf.Insert("key", items);

        Assert.True(cf.Exists("key", "item1"));
        Assert.True(cf.Exists("key", "item2"));
        Assert.True(cf.Exists("key", "item3"));
    }

    [Fact]
    public async Task TestInsertAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        await cf.InsertAsync("key", items);

        Assert.True(await cf.ExistsAsync("key", "item1"));
        Assert.True(await cf.ExistsAsync("key", "item2"));
        Assert.True(await cf.ExistsAsync("key", "item3"));
    }

    [Fact]
    public void TestInsertNX()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        Assert.Throws<RedisServerException>(() => cf.InsertNX(key, items, 1024, true));
        var result = cf.InsertNX(key, items, 1024);
        cf.InsertNX(key, items, 10245, true);
        var trues = new bool[] { true, true, true };
        Assert.Equal(result, trues);

        Assert.True(cf.Exists(key, "item1"));
        Assert.True(cf.Exists(key, "item2"));
        Assert.True(cf.Exists(key, "item3"));

        Assert.Equal(cf.MExists(key, items), trues);

        result = cf.InsertNX(key, items);
        Assert.Equal(result, new bool[] { false, false, false });

        // test empty items:
        Assert.Throws<ArgumentOutOfRangeException>(() => cf.InsertNX(key, new RedisValue[] { }));
    }

    [Fact]
    public async Task TestInsertNXAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        _ = Assert.ThrowsAsync<RedisServerException>(async () => await cf.InsertNXAsync(key, items, 1024, true));
        var result = await cf.InsertNXAsync(key, items, 1024);
        await cf.InsertNXAsync(key, items, 10245, true);
        var trues = new bool[] { true, true, true };
        Assert.Equal(result, trues);

        Assert.True(await cf.ExistsAsync(key, "item1"));
        Assert.True(await cf.ExistsAsync(key, "item2"));
        Assert.True(await cf.ExistsAsync(key, "item3"));

        Assert.Equal(await cf.MExistsAsync(key, items), trues);

        result = await cf.InsertNXAsync(key, items);
        Assert.Equal(result, new bool[] { false, false, false });

        // test empty items:
        _ = Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await cf.InsertNXAsync(key, new RedisValue[] { }));
    }

    [Fact]
    public void TestExistsNonExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        RedisValue item = new RedisValue("item");
        Assert.False(cf.Exists("NonExistKey", item));
    }

    [Fact]
    public async Task TestExistsNonExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        RedisValue item = new RedisValue("item");
        Assert.False(await cf.ExistsAsync("NonExistKey", item));
    }

    [Fact]
    public void TestScanDumpAndLoadChunk()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        cf.Reserve("cuckoo", 100, 50);
        cf.Add("cuckoo-dump", "a");

        long iterator = 0;
        while (true)
        {
            var chunkData = cf.ScanDump("cuckoo-dump", iterator);
            iterator = chunkData.Item1;
            if (iterator == 0) break;
            Assert.True(cf.LoadChunk("cuckoo-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal(cf.Info("cuckoo-dump").NumberOfItemsInserted, cf.Info("cuckoo-load").NumberOfItemsInserted);
        // check for existing items
        Assert.True(cf.Exists("cuckoo-load", "a"));
    }

    [Fact]
    public async Task TestScanDumpAndLoadChunkAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var cf = db.CF();

        await cf.ReserveAsync("cuckoo", 100, 50);
        await cf.AddAsync("cuckoo-dump", "a");

        long iterator = 0;
        while (true)
        {
            var chunkData = await cf.ScanDumpAsync("cuckoo-dump", iterator);
            iterator = chunkData.Item1;
            if (iterator == 0) break;
            Assert.True(await cf.LoadChunkAsync("cuckoo-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal((await cf.InfoAsync("cuckoo-dump")).NumberOfItemsInserted, (await cf.InfoAsync("cuckoo-load")).NumberOfItemsInserted);
        // check for existing items
        Assert.True(await cf.ExistsAsync("cuckoo-load", "a"));
    }


    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var cf1 = db1.CF();
        var cf2 = db2.CF();

        Assert.NotEqual(cf1.GetHashCode(), cf2.GetHashCode());
    }
}