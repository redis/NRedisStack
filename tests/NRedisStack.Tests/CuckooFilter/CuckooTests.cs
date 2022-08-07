using Xunit;
using StackExchange.Redis;
using NRedisStack.Core.RedisStackCommands;
using Moq;

namespace NRedisStack.Tests.CuckooFilter;

public class CuckooTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "CUCKOO_TESTS";
    public CuckooTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void TestReserveBasic()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        Assert.True(db.CF().Reserve(key, 100L));
        Assert.Throws<RedisServerException>(() => db.CF().Reserve(key, 100L));

        Assert.True((db.CF().Add(key, "item1")));
        Assert.True(db.CF().Exists(key, "item1"));
        Assert.False(db.CF().Exists(key, "item2"));
    }

    [Fact]
    public void TestAddExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(db.CF().Add(key, "item1"));
        Assert.True(db.CF().Exists(key, "item1"));
    }

    [Fact]
    public void TestAddNX()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(db.CF().AddNX(key, "item1"));
        Assert.False(db.CF().AddNX(key, "item1"));
        Assert.True(db.CF().Exists(key, "item1"));
    }

    [Fact]
    public void TestCountFilterDoesNotExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        Assert.Equal(db.CF().Count("notExistFilter", "notExistItem"), 0);
    }

    [Fact]
    public void TestCountFilterExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        db.CF().Insert(key, new RedisValue[]{"foo"});
        Assert.Equal(db.CF().Count(key, "notExistItem"), 0);
    }

    [Fact]
    public void TestCountItemExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CF().Insert(key, new RedisValue[]{"foo"});
        Assert.Equal(db.CF().Count(key, "foo"), 1);
    }

    [Fact]
    public void TestDelete()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CF().Add(key, "item");
        Assert.False(db.CF().Del(key, "notExistsItem"));
        Assert.True(db.CF().Del(key, "item"));

        Assert.Throws<RedisServerException>( () => db.CF().Del("notExistKey", "item"));
    }

    [Fact]
    public void TestInfo() //TODO: think again about the returned value of CF.INFO, maybe creating a new returned type
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        db.CF().Add(key, "item");
        var info = db.CF().Info(key);

        Assert.NotNull(info);
        Assert.Equal(info.NumberOfItemsInserted, (long)1);

        Assert.Throws<RedisServerException>( () => db.CF().Info("notExistKey"));
    }

    [Fact]
    public void TestInsert()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };

        db.CF().Insert("key", items);

        Assert.True(db.CF().Exists("key", "item1"));
        Assert.True(db.CF().Exists("key", "item2"));
        Assert.True(db.CF().Exists("key", "item3"));
    }

    [Fact]
    public void TestInsertNX()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        RedisValue[] items = new RedisValue[] { "item1", "item2", "item3" };
        db.Execute("FLUSHALL");

        var result = db.CF().InsertNX(key, items);
        var trues = new bool[] {true, true, true};
        Assert.Equal(result, trues);

        Assert.True(db.CF().Exists(key, "item1"));
        Assert.True(db.CF().Exists(key, "item2"));
        Assert.True(db.CF().Exists(key, "item3"));

        Assert.Equal(db.CF().MExists(key, items), trues);

        result = db.CF().InsertNX(key, items);
        Assert.Equal(result, new bool[] {false, false, false});
    }

    [Fact]
    public void TestExistsNonExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        RedisValue item = new RedisValue("item");
        Assert.False(db.CF().Exists("NonExistKey", item));
    }



    [Fact]
    public void TestScanDumpAndLoadChunk()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.CF().Reserve("cuckoo",100, 50);
        db.CF().Add("cuckoo-dump", "a");

        long iterator = 0;
        while(true)
        {
            var chunkData = db.CF().ScanDump("cuckoo-dump", iterator);
            iterator = chunkData.Item1;
            if(iterator == 0) break;
            Assert.True(db.CF().LoadChunk("cuckoo-load", iterator, chunkData.Item2));
        }

        // check for properties
        Assert.Equal(db.CF().Info("cuckoo-dump").NumberOfItemsInserted, db.CF().Info("cuckoo-load").NumberOfItemsInserted);
        // check for existing items
        Assert.True(db.CF().Exists("cuckoo-load", "a"));
    }
}