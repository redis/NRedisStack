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
    //     var result = db.TS().Create(key);
    //     Assert.True(result);
    //     //TimeSeriesInformation info = db.TS().Info(key);
    // }

}