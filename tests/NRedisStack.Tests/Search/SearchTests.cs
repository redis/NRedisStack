using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;


namespace NRedisStack.Tests.Search;

public class SearchTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "SEARCH_TESTS";
    public SearchTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }


    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var ft1 = db1.FT();
        var ft2 = db2.FT();

        Assert.NotEqual(ft1.GetHashCode(), ft2.GetHashCode());
    }

    [Fact]
    public void TestModulePrefixs1()
    {
        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var ft = db.FT();
            // ...
            conn.Dispose();
        }

        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var ft = db.FT();
            // ...
            conn.Dispose();
        }

    }

}