using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.Bloom;

public class BloomTests : AbstractNRedisStackTest, IDisposable
{
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
        var bf = db.BF();


        bf.Reserve(key, 0.001, 100L);

        Assert.True((bf.Add(key, "item1")));
        Assert.True(bf.Exists(key, "item1"));
        Assert.False(bf.Exists(key, "item2"));
    }

    [Fact]
    public async Task TestReserveBasicAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        await bf.ReserveAsync(key, 0.001, 100L);

        Assert.True(await (bf.AddAsync(key, "item1")));
        Assert.True(await bf.ExistsAsync(key, "item1"));
        Assert.False(await bf.ExistsAsync(key, "item2"));
    }

    [Fact]
    public void TestAddWhenExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        Assert.True((bf.Add(key, "item1"))); // first time
        Assert.False(bf.Add(key, "item1")); // second time
    }

    [Fact]
    public async Task TestAddWhenExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();


        Assert.True(await bf.AddAsync(key, "item1")); // first time
        Assert.False(await bf.AddAsync(key, "item1")); // second time
    }

    [Fact]
    public void TestAddExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();


        bf.Add(key, "item1");
        Assert.True(bf.Exists(key, "item1"));
    }

    [Fact]
    public async Task TestAddExistsAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();


        await bf.AddAsync(key, "item1");
        Assert.True(await bf.ExistsAsync(key, "item1"));
    }

    [Fact]
    public void TestAddExistsMulti()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();
        var items = new RedisValue[] { "foo", "bar", "baz" };
        var items2 = new RedisValue[] { "newElement", "bar", "baz" };

        var result = bf.MAdd(key, items);
        Assert.Equal(new bool[] { true, true, true }, result);

        result = bf.MAdd(key, items2);
        Assert.Equal(new bool[] { true, false, false }, result);
    }

    [Fact]
    public async Task TestAddExistsMultiAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();
        var items = new RedisValue[] { "foo", "bar", "baz" };
        var items2 = new RedisValue[] { "newElement", "bar", "baz" };

        var result = await bf.MAddAsync(key, items);
        Assert.Equal(new bool[] { true, true, true }, result);

        result = await bf.MAddAsync(key, items2);
        Assert.Equal(new bool[] { true, false, false }, result);
    }

    [Fact]
    public void TestExample()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        // Simple bloom filter using default module settings
        bf.Add("simpleBloom", "Mark");
        // Does "Mark" now exist?
        bf.Exists("simpleBloom", "Mark"); // true
        bf.Exists("simpleBloom", "Farnsworth"); // False

        // If you have a long list of items to check/add, you can use the
        // "multi" methods
        var items = new RedisValue[] { "foo", "bar", "baz", "bat", "bag" };
        bf.MAdd("simpleBloom", items);

        // Check if they exist:
        var allItems = new RedisValue[] { "foo", "bar", "baz", "bat", "Mark", "nonexist" };
        var rv = bf.MExists("simpleBloom", allItems);
        // All items except the last one will be 'true'
        Assert.Equal(new bool[] { true, true, true, true, true, false }, rv);

        // Reserve a "customized" bloom filter
        bf.Reserve("specialBloom", 0.0001, 10000);
        bf.Add("specialBloom", "foo");
    }

    [Fact]
    public async Task TestExampleAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        // Simple bloom filter using default module settings
        await bf.AddAsync("simpleBloom", "Mark");
        // Does "Mark" now exist?
        await bf.ExistsAsync("simpleBloom", "Mark"); // true
        await bf.ExistsAsync("simpleBloom", "Farnsworth"); // False

        // If you have a long list of items to check/add, you can use the
        // "multi" methods
        var items = new RedisValue[] { "foo", "bar", "baz", "bat", "bag" };
        await bf.MAddAsync("simpleBloom", items);

        // Check if they exist:
        var allItems = new RedisValue[] { "foo", "bar", "baz", "bat", "Mark", "nonexist" };
        var rv = await bf.MExistsAsync("simpleBloom", allItems);
        // All items except the last one will be 'true'
        Assert.Equal(new bool[] { true, true, true, true, true, false }, rv);

        // Reserve a "customized" bloom filter
        await bf.ReserveAsync("specialBloom", 0.0001, 10000);
        await bf.AddAsync("specialBloom", "foo");
    }

    [Fact]
    public void TestInsert()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        bf.Insert("key", items);

        Assert.True(bf.Exists("key", "item1"));
        Assert.True(bf.Exists("key", "item2"));
        Assert.True(bf.Exists("key", "item3"));
    }

    [Fact]
    public async Task TestInsertAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        await bf.InsertAsync("key", items);

        Assert.True(await bf.ExistsAsync("key", "item1"));
        Assert.True(await bf.ExistsAsync("key", "item2"));
        Assert.True(await bf.ExistsAsync("key", "item3"));
    }

    [Fact]
    public void TestExistsNonExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        RedisValue item = new RedisValue("item");
        Assert.False(bf.Exists("NonExistKey", item));
    }

    [Fact]
    public async Task TestExistsNonExistAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        RedisValue item = new RedisValue("item");
        Assert.False(await bf.ExistsAsync("NonExistKey", item));
    }

    [Fact]
    public void TestInfo()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        bf.Add(key, "item");
        var info = bf.Info(key);

        Assert.NotNull(info);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);

        Assert.Throws<RedisServerException>(() => bf.Info("notExistKey"));
    }

    [Fact]
    public async Task TestInfoAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        await bf.AddAsync(key, "item");
        var info = await bf.InfoAsync(key);

        Assert.NotNull(info);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);

        await Assert.ThrowsAsync<RedisServerException>(() => bf.InfoAsync("notExistKey"));
    }

    [Fact]
    public void TestScanDumpAndLoadChunk()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        bf.Reserve("bloom-dump", 0.1, 10);
        bf.Add("bloom-dump", "a");

        long iterator = 0;
        while (true)
        {
            var chunkData = bf.ScanDump("bloom-dump", iterator);
            iterator = chunkData.Item1;
            if (iterator == 0) break;
            Assert.True(bf.LoadChunk("bloom-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal(bf.Info("bloom-dump").NumberOfItemsInserted, bf.Info("bloom-load").NumberOfItemsInserted);
        // check for existing items
        Assert.True(bf.Exists("bloom-load", "a"));
    }

    [Fact]
    public async Task TestScanDumpAndLoadChunkAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        await bf.ReserveAsync("bloom-dump", 0.1, 10);
        await bf.AddAsync("bloom-dump", "a");

        long iterator = 0;
        while (true)
        {
            var chunkData = await bf.ScanDumpAsync("bloom-dump", iterator);
            iterator = chunkData.Item1;
            if (iterator == 0) break;
            Assert.True(await bf.LoadChunkAsync("bloom-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal((await bf.InfoAsync("bloom-dump")).NumberOfItemsInserted, (await bf.InfoAsync("bloom-load")).NumberOfItemsInserted);
        // check for existing items
        Assert.True(await bf.ExistsAsync("bloom-load", "a"));
    }


    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var bf1 = db1.FT();
        var bf2 = db2.FT();

        Assert.NotEqual(bf1.GetHashCode(), bf2.GetHashCode());
    }

    [Fact]
    public void TestCard()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        // return 0 if the key does not exist:
        Assert.Equal(0, bf.Card("notExist"));

        // Store a filter:
        Assert.True(bf.Add("bf1", "item_foo"));
        Assert.Equal(1, bf.Card("bf1"));

        // Error when key is of a type other than Bloom filter:
        db.StringSet("setKey", "value");
        Assert.Throws<RedisServerException>(() => bf.Card("setKey"));
    }

    [Fact]
    public async Task TestCardAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        // return 0 if the key does not exist:
        Assert.Equal(0, await bf.CardAsync("notExist"));

        // Store a filter:
        Assert.True(await bf.AddAsync("bf1", "item_foo"));
        Assert.Equal(1, await bf.CardAsync("bf1"));

        // Error when key is of a type other than Bloom filter:
        db.StringSet("setKey", "value");
        await Assert.ThrowsAsync<RedisServerException>(() => bf.CardAsync("setKey"));
    }

    [Fact]
    public void TestInsertArgsError()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var bf = db.BF();

        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };
        // supose to throw exception:
        Assert.Throws<RedisServerException>(() => bf.Insert("key3", items, 100, 0.01, 2, nocreate: true, nonscaling: true));
    }
}