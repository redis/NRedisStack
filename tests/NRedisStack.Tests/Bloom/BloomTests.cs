using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;

namespace NRedisStack.Tests.Bloom;

public class BloomTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "BLOOM_TESTS";
    public BloomTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void TestReserveBasic()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");


        db.BF().Reserve(key, 0.001, 100L);

        Assert.True((db.BF().Add(key, "item1")));
        Assert.True(db.BF().Exists(key, "item1"));
        Assert.False(db.BF().Exists(key, "item2"));
    }

    [Fact]
    public async Task TestReserveBasicAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");


        await db.BF().ReserveAsync(key, 0.001, 100L);

        Assert.True(await (db.BF().AddAsync(key, "item1")));
        Assert.True(await db.BF().ExistsAsync(key, "item1"));
        Assert.False(await db.BF().ExistsAsync(key, "item2"));
    }

    [Fact]
    public void TestAddWhenExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");


        Assert.True((db.BF().Add(key, "item1"))); // first time
        Assert.False(db.BF().Add(key, "item1")); // second time
    }

    [Fact]
    public async Task TestAddWhenExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");


        Assert.True(await db.BF().AddAsync(key, "item1")); // first time
        Assert.False(await db.BF().AddAsync(key, "item1")); // second time
    }

    [Fact]
    public void TestAddExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");


        db.BF().Add(key, "item1");
        Assert.True(db.BF().Exists(key, "item1"));
    }

    [Fact]
    public async Task TestAddExistsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");


        await db.BF().AddAsync(key, "item1");
        Assert.True(await db.BF().ExistsAsync(key, "item1"));
    }

    [Fact]
    public void TestAddExistsMulti()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var items = new RedisValue[] { "foo", "bar", "baz" };
        var items2 = new RedisValue[] { "newElement", "bar", "baz" };

        var result = db.BF().MAdd(key, items);
        Assert.Equal(new bool[] { true, true, true }, result);

        result = db.BF().MAdd(key, items2);
        Assert.Equal(new bool[] { true, false, false }, result);
    }

    [Fact]
    public async Task TestAddExistsMultiAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var items = new RedisValue[] { "foo", "bar", "baz" };
        var items2 = new RedisValue[] { "newElement", "bar", "baz" };

        var result = await db.BF().MAddAsync(key, items);
        Assert.Equal(new bool[] { true, true, true }, result);

        result = await db.BF().MAddAsync(key, items2);
        Assert.Equal(new bool[] { true, false, false }, result);
    }

    [Fact]
    public void TestExample()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        // Simple bloom filter using default module settings
        db.BF().Add("simpleBloom", "Mark");
        // Does "Mark" now exist?
        db.BF().Exists("simpleBloom", "Mark"); // true
        db.BF().Exists("simpleBloom", "Farnsworth"); // False

        // If you have a long list of items to check/add, you can use the
        // "multi" methods
        var items = new RedisValue[] { "foo", "bar", "baz", "bat", "bag" };
        db.BF().MAdd("simpleBloom", items);

        // Check if they exist:
        var allItems = new RedisValue[] { "foo", "bar", "baz", "bat", "Mark", "nonexist" };
        var rv = db.BF().MExists("simpleBloom", allItems);
        // All items except the last one will be 'true'
        Assert.Equal(new bool[] { true, true, true, true, true, false }, rv);

        // Reserve a "customized" bloom filter
        db.BF().Reserve("specialBloom", 0.0001, 10000);
        db.BF().Add("specialBloom", "foo");
    }

    [Fact]
    public async Task TestExampleAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        // Simple bloom filter using default module settings
        await db.BF().AddAsync("simpleBloom", "Mark");
        // Does "Mark" now exist?
        await db.BF().ExistsAsync("simpleBloom", "Mark"); // true
        await db.BF().ExistsAsync("simpleBloom", "Farnsworth"); // False

        // If you have a long list of items to check/add, you can use the
        // "multi" methods
        var items = new RedisValue[] { "foo", "bar", "baz", "bat", "bag" };
        await db.BF().MAddAsync("simpleBloom", items);

        // Check if they exist:
        var allItems = new RedisValue[] { "foo", "bar", "baz", "bat", "Mark", "nonexist" };
        var rv = await db.BF().MExistsAsync("simpleBloom", allItems);
        // All items except the last one will be 'true'
        Assert.Equal(new bool[] { true, true, true, true, true, false }, rv);

        // Reserve a "customized" bloom filter
        await db.BF().ReserveAsync("specialBloom", 0.0001, 10000);
        await db.BF().AddAsync("specialBloom", "foo");
    }

    [Fact]
    public void TestInsert()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        db.BF().Insert("key", items);

        Assert.True(db.BF().Exists("key", "item1"));
        Assert.True(db.BF().Exists("key", "item2"));
        Assert.True(db.BF().Exists("key", "item3"));
    }

    [Fact]
    public async Task TestInsertAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        await db.BF().InsertAsync("key", items);

        Assert.True(await db.BF().ExistsAsync("key", "item1"));
        Assert.True(await db.BF().ExistsAsync("key", "item2"));
        Assert.True(await db.BF().ExistsAsync("key", "item3"));
    }

    [Fact]
    public void TestExistsNonExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        RedisValue item = new RedisValue("item");
        Assert.False(db.BF().Exists("NonExistKey", item));
    }

    [Fact]
    public async Task TestExistsNonExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        RedisValue item = new RedisValue("item");
        Assert.False(await db.BF().ExistsAsync("NonExistKey", item));
    }

    [Fact]
    public void TestInfo()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.BF().Add(key, "item");
        var info = db.BF().Info(key);

        Assert.NotNull(info);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);

        Assert.Throws<RedisServerException>(() => db.BF().Info("notExistKey"));
    }

    [Fact]
    public async Task TestInfoAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.BF().AddAsync(key, "item");
        var info = await db.BF().InfoAsync(key);

        Assert.NotNull(info);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);

        await Assert.ThrowsAsync<RedisServerException>(() => db.BF().InfoAsync("notExistKey"));
    }

    [Fact]
    public void TestScanDumpAndLoadChunk()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.BF().Reserve("bloom-dump", 0.1, 10);
        db.BF().Add("bloom-dump", "a");

        long iterator = 0;
        while (true)
        {
            var chunkData = db.BF().ScanDump("bloom-dump", iterator);
            iterator = chunkData.Item1;
            if (iterator == 0) break;
            Assert.True(db.BF().LoadChunk("bloom-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal(db.BF().Info("bloom-dump").NumberOfItemsInserted, db.BF().Info("bloom-load").NumberOfItemsInserted);
        // check for existing items
        Assert.True(db.BF().Exists("bloom-load", "a"));
    }

    [Fact]
    public async Task TestScanDumpAndLoadChunkAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.BF().ReserveAsync("bloom-dump", 0.1, 10);
        await db.BF().AddAsync("bloom-dump", "a");

        long iterator = 0;
        while (true)
        {
            var chunkData = await db.BF().ScanDumpAsync("bloom-dump", iterator);
            iterator = chunkData.Item1;
            if (iterator == 0) break;
            Assert.True(await db.BF().LoadChunkAsync("bloom-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal((await db.BF().InfoAsync("bloom-dump")).NumberOfItemsInserted, (await db.BF().InfoAsync("bloom-load")).NumberOfItemsInserted);
        // check for existing items
        Assert.True(await db.BF().ExistsAsync("bloom-load", "a"));
    }
}