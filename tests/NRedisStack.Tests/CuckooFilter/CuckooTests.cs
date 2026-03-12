using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.CuckooFilter;

// 

public class CuckooTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "CUCKOO_TESTS";

    public CuckooTests(EndpointsFixture endpointsFixture) : base(endpointsFixture)
    {
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestReserveBasic(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);

        var cf = db.CF();
        Assert.True(cf.Reserve(key, 100L, maxIterations: 20, expansion: 1));
        Assert.Throws<RedisServerException>(() => cf.Reserve(key, 100L));

        Assert.True((cf.Add(key, "item1")));
        Assert.True(cf.Exists(key, "item1"));
        Assert.False(cf.Exists(key, "item2"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestReserveBasicAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();
        Assert.True(await cf.ReserveAsync(key, 100L, maxIterations: 20, expansion: 1));
        await Assert.ThrowsAsync<RedisServerException>(async () => await cf.ReserveAsync(key, 100L));

        Assert.True(await (cf.AddAsync(key, "item1")));
        Assert.True(await cf.ExistsAsync(key, "item1"));
        Assert.False(await cf.ExistsAsync(key, "item2"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAddExists(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        Assert.True(cf.Add(key, "item1"));
        Assert.True(cf.Exists(key, "item1"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAddExistsAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        Assert.True(await cf.AddAsync(key, "item1"));
        Assert.True(await cf.ExistsAsync(key, "item1"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAddNX(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        Assert.True(cf.AddNX(key, "item1"));
        Assert.False(cf.AddNX(key, "item1"));
        Assert.True(cf.Exists(key, "item1"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAddNXAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        Assert.True(await cf.AddNXAsync(key, "item1"));
        Assert.False(await cf.AddNXAsync(key, "item1"));
        Assert.True(await cf.ExistsAsync(key, "item1"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCountFilterDoesNotExist(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        Assert.Equal(0, cf.Count("notExistFilter", "notExistItem"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCountFilterDoesNotExistAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        Assert.Equal(0, await cf.CountAsync("notExistFilter", "notExistItem"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCountFilterExist(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        cf.Insert(key, ["foo"]);
        Assert.Equal(0, cf.Count(key, "notExistItem"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCountFilterExistAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        await cf.InsertAsync(key, ["foo"]);
        Assert.Equal(0, await cf.CountAsync(key, "notExistItem"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCountItemExist(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        cf.Insert(key, ["foo"]);
        Assert.Equal(1, cf.Count(key, "foo"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCountItemExistAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        await cf.InsertAsync(key, ["foo"]);
        Assert.Equal(1, await cf.CountAsync(key, "foo"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestDelete(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        cf.Add(key, "item");
        Assert.False(cf.Del(key, "notExistsItem"));
        Assert.True(cf.Del(key, "item"));

        Assert.Throws<RedisServerException>(() => cf.Del("notExistKey", "item"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestDeleteAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        await cf.AddAsync(key, "item");
        Assert.False(await cf.DelAsync(key, "notExistsItem"));
        Assert.True(await cf.DelAsync(key, "item"));

        await Assert.ThrowsAsync<RedisServerException>(() => cf.DelAsync("notExistKey", "item"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestInfo(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        cf.Add(key, "item");
        var info = cf.Info(key);

        Assert.NotNull(info);
        Assert.Equal((long)2, info.BucketSize);
        Assert.Equal((long)1, info.ExpansionRate);
        Assert.Equal((long)20, info.MaxIterations);
        Assert.Equal((long)512, info.NumberOfBuckets);
        Assert.Equal((long)1, info.NumberOfFilters);
        Assert.Equal((long)0, info.NumberOfItemsDeleted);
        Assert.Equal((long)1, info.NumberOfItemsInserted);
        Assert.Equal((long)1080, info.Size);

        Assert.Throws<RedisServerException>(() => cf.Info("notExistKey"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestInfoAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        await cf.AddAsync(key, "item");
        var info = await cf.InfoAsync(key);

        Assert.NotNull(info);
        Assert.Equal((long)2, info.BucketSize);
        Assert.Equal((long)1, info.ExpansionRate);
        Assert.Equal((long)20, info.MaxIterations);
        Assert.Equal((long)512, info.NumberOfBuckets);
        Assert.Equal((long)1, info.NumberOfFilters);
        Assert.Equal((long)0, info.NumberOfItemsDeleted);
        Assert.Equal((long)1, info.NumberOfItemsInserted);
        Assert.Equal((long)1080, info.Size);



        await Assert.ThrowsAsync<RedisServerException>(() => cf.InfoAsync("notExistKey"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestInsert(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        RedisValue[] items = ["item1", "item2", "item3"];

        cf.Insert("key", items);

        Assert.True(cf.Exists("key", "item1"));
        Assert.True(cf.Exists("key", "item2"));
        Assert.True(cf.Exists("key", "item3"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestInsertAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        RedisValue[] items = ["item1", "item2", "item3"];

        await cf.InsertAsync("key", items);

        Assert.True(await cf.ExistsAsync("key", "item1"));
        Assert.True(await cf.ExistsAsync("key", "item2"));
        Assert.True(await cf.ExistsAsync("key", "item3"));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestInsertNX(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        RedisValue[] items = ["item1", "item2", "item3"];

        Assert.Throws<RedisServerException>(() => cf.InsertNX(key, items, 1024, true));
        var result = cf.InsertNX(key, items, 1024);
        cf.InsertNX(key, items, 10245, true);
        var trues = new[] { true, true, true };
        Assert.Equal(result, trues);

        Assert.True(cf.Exists(key, "item1"));
        Assert.True(cf.Exists(key, "item2"));
        Assert.True(cf.Exists(key, "item3"));

        Assert.Equal(cf.MExists(key, items), trues);

        result = cf.InsertNX(key, items);
        Assert.Equal(result, new[] { false, false, false });

        // test empty items:
        Assert.Throws<ArgumentOutOfRangeException>(() => cf.InsertNX(key, []));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestInsertNXAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        RedisValue[] items = ["item1", "item2", "item3"];

        await Assert.ThrowsAsync<RedisServerException>(async () => await cf.InsertNXAsync(key, items, 1024, true));
        var result = await cf.InsertNXAsync(key, items, 1024);
        await cf.InsertNXAsync(key, items, 10245, true);
        var trues = new[] { true, true, true };
        Assert.Equal(result, trues);

        Assert.True(await cf.ExistsAsync(key, "item1"));
        Assert.True(await cf.ExistsAsync(key, "item2"));
        Assert.True(await cf.ExistsAsync(key, "item3"));

        Assert.Equal(await cf.MExistsAsync(key, items), trues);

        result = await cf.InsertNXAsync(key, items);
        Assert.Equal(result, new[] { false, false, false });

        // test empty items:
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await cf.InsertNXAsync(key, []));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestExistsNonExist(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        RedisValue item = new("item");
        Assert.False(cf.Exists("NonExistKey", item));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestExistsNonExistAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var cf = db.CF();

        RedisValue item = new("item");
        Assert.False(await cf.ExistsAsync("NonExistKey", item));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestScanDumpAndLoadChunk(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
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

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestScanDumpAndLoadChunkAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
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


    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestModulePrefixs(string endpointId)
    {
        var redis = GetConnection(endpointId);

        IDatabase db1 = redis.GetDatabase();
        IDatabase db2 = redis.GetDatabase();

        var cf1 = db1.CF();
        var cf2 = db2.CF();

        Assert.NotEqual(cf1.GetHashCode(), cf2.GetHashCode());
    }
}