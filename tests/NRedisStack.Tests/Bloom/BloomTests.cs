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
    public void TestInfo() //TODO: think again about the returned value of BF.INFO
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.BF().Add(key, "item");
        var info = db.BF().Info(key);
        var dictionary = info.ToDictionary();
        Assert.Equal(dictionary["Number of items inserted"].ToString(), "1");

        // TODO: Check fail when doing db.BF().Info("notExistKey");
    }
}