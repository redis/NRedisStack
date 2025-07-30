#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestGetAsync(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    [Fact]
    public async Task TestGetNotExists()
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase();
        var ts = db.TS();
        var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.GetAsync(key));
        Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
    }

    [Fact]
    public async Task TestEmptyGet()
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase();
        var ts = db.TS();
        await ts.CreateAsync(key);
        Assert.Null(await ts.GetAsync(key));
    }

    [Fact]
    public async Task TestAddAndGet()
    {
        var key = CreateKeyName();
        var now = DateTime.UtcNow;
        var expected = new TimeSeriesTuple(now, 1.1);
        var db = GetCleanDatabase();
        var ts = db.TS();
        await ts.CreateAsync(key);
        await ts.AddAsync(key, now, 1.1);
        var actual = await ts.GetAsync(key);
        Assert.Equal(expected, actual);
    }
}