using Xunit;
using StackExchange.Redis;
using NRedisStack.Core.RedisStackCommands;
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

        db.BF().Reserve(key, 0.001, 100L);

        Assert.True((db.BF().Add(key, "item1")));
        Assert.True(db.BF().Exists(key, "item1"));
        Assert.False(db.BF().Exists(key, "item2"));
    }

    [Fact]
    public void TestAddWhenExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        Assert.True((db.BF().Add(key, "item1"))); // first time
        Assert.False(db.BF().Add(key, "item1")); // second time
    }

    [Fact]
    public void TestAddExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        db.BF().Add(key, "item1");
        Assert.True(db.BF().Exists(key, "item1"));
    }

    [Fact]
    public void TestAddExistsMulti()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        var items = new RedisValue[]{"foo", "bar", "baz"};
        var items2 = new RedisValue[]{"newElement", "bar", "baz"};

        var result = db.BF().MAdd(key, items);
        Assert.Equal(new bool[] {true, true, true}, result);

        result = db.BF().MAdd(key, items2);
        Assert.Equal(new bool[] {true, false, false}, result);
    }

    [Fact]
    public void TestExample() {
                IDatabase db = redisFixture.Redis.GetDatabase();

    // Simple bloom filter using default module settings
     db.BF().Add("simpleBloom", "Mark");
    // Does "Mark" now exist?
     db.BF().Exists("simpleBloom", "Mark"); // true
     db.BF().Exists("simpleBloom", "Farnsworth"); // False

    // If you have a long list of items to check/add, you can use the
    // "multi" methods
    var items = new RedisValue[]{"foo", "bar", "baz", "bat", "bag"};
     db.BF().MAdd("simpleBloom", items);

    // Check if they exist:
    var allItems = new RedisValue[]{"foo", "bar", "baz", "bat", "Mark", "nonexist"};
    var rv =  db.BF().MExists("simpleBloom", allItems);
    // All items except the last one will be 'true'
    Assert.Equal(new bool[] {true, true, true, true, true, false}, rv);

    // Reserve a "customized" bloom filter
     db.BF().Reserve("specialBloom", 0.0001, 10000);
     db.BF().Add("specialBloom", "foo");
  }

    [Fact]
    public void TestInsert()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        db.BF().Insert("key", items);

        Assert.True(db.BF().Exists("key", "item1"));
        Assert.True(db.BF().Exists("key", "item2"));
        Assert.True(db.BF().Exists("key", "item3"));
    }

    [Fact]
    public void TestExistsNonExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        RedisValue item = new RedisValue("item");
        Assert.False(db.BF().Exists("NonExistKey", item));
    }

    [Fact]
    public void TestInfo() //TODO: think again about the returned value of BF.INFO, maybe creating a new returned type
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.BF().Add(key, "item");
        var info = db.BF().Info(key);

        Assert.NotNull(info);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);

        Assert.Throws<RedisServerException>( () => db.BF().Info("notExistKey"));
    }

    [Fact]
    public void TestScanDumpAndLoadChunk() //TODO: Fininsh this Test
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.BF().Reserve("bloom-dump",0.1, 10);
        db.BF().Add("bloom-dump", "a");

        long iterator = 0;
        while(true)
        {
            var chunkData = db.BF().ScanDump("bloom-dump", iterator);
            iterator = chunkData.Item1;
            if(iterator == 0) break;
            Assert.True(db.BF().LoadChunk("bloom-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal(db.BF().Info("bloom-dump").NumberOfItemsInserted, db.BF().Info("bloom-load").NumberOfItemsInserted);
        // check for existing items
        Assert.True(db.BF().Exists("bloom-load", "a"));
    }
}