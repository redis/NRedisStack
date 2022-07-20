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
    public void TestBfAddWhenExist()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        Assert.True((db.BF().Add(key, "item1")).ToString() == "1"); // first time
        Assert.True(db.BF().Add(key, "item1").ToString() == "0"); // second time
    }

    [Fact]
    public void TestBfAddExists()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();

        db.BF().Add(key, "item1");
        Assert.True(db.BF().Exists(key, "item1"));
    }

    [Fact]
    public void TestBfInsert()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        RedisValue[] items = new RedisValue[] { "item1" , "item2", "item3"};

        db.BF().Insert("key", items);

        Assert.True(db.BF().Exists("key", "item1"));
        Assert.True(db.BF().Exists("key", "item2"));
        Assert.True(db.BF().Exists("key", "item3"));
    }
}