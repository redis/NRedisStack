using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;


namespace NRedisStack.Tests.TimeSeries;

public class TimeSeriesTests(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
{
    [Fact]
    public void TestModulePrefixs()
    {
        var redis = GetConnection();
        IDatabase db1 = redis.GetDatabase();
        IDatabase db2 = redis.GetDatabase();

        var ts1 = db1.TS();
        var ts2 = db2.TS();

        Assert.NotEqual(ts1.GetHashCode(), ts2.GetHashCode());
    }
}