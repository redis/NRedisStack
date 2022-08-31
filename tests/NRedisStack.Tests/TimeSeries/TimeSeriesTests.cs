using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;


namespace NRedisStack.Tests.TimeSeries;

public class TimeSeriesTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "TIME_SERIES_TESTS";
    public TimeSeriesTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    // [Fact]
    // public void TestCreateOK()
    // {
    //     IDatabase db = redisFixture.Redis.GetDatabase();
    //     var result =  ts.Create(key);
    //     Assert.True(result);
    //     //TimeSeriesInformation info =  ts.Info(key);
    // }


    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var ts1 = db1.TS();
        var ts2 = db2.TS();

        Assert.NotEqual(ts1.GetHashCode(), ts2.GetHashCode());
    }

    [Fact]
    public void TestModulePrefixs1()
    {
        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var ts = db.TS();
            // ...
            conn.Dispose();
        }

        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var ts = db.TS();
            // ...
            conn.Dispose();
        }

    }

}