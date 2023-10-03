using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;


namespace NRedisStack.Tests.TimeSeries;

public class TimeSeriesTests : AbstractNRedisStackTest, IDisposable
{
    // private readonly string key = "TIME_SERIES_TESTS";
    public TimeSeriesTests(RedisFixture redisFixture) : base(redisFixture) { }

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
}