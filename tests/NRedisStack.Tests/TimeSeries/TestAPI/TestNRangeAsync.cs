#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestNRangeAsync(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
{
    private static long Ts(TimeStamp t) => (long)t.Value;

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestNRangeOuterJoinAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(3);
        await ts.AddAsync(keys[0], 10000, 10); await ts.AddAsync(keys[0], 20000, 12);
        await ts.AddAsync(keys[1], 10000, 13);
        await ts.AddAsync(keys[2], 20000, 99);

        var rows = await ts.NRangeAsync(keys, "-", "+");

        Assert.Equal(2, rows.Count);
        Assert.Equal(10000, Ts(rows[0].Timestamp));
        Assert.Equal(3, rows[0].Values.Count);
        Assert.Equal(10, rows[0].Values[0]);
        Assert.Equal(13, rows[0].Values[1]);
        Assert.True(double.IsNaN(rows[0].Values[2]));
        Assert.Equal(20000, Ts(rows[1].Timestamp));
        Assert.True(double.IsNaN(rows[1].Values[1]));
        Assert.Equal(99, rows[1].Values[2]);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestNRevRangeAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(2);
        await ts.AddAsync(keys[0], 10000, 1); await ts.AddAsync(keys[0], 20000, 2);
        await ts.AddAsync(keys[1], 10000, 3); await ts.AddAsync(keys[1], 20000, 4);

        var rows = await ts.NRevRangeAsync(keys, "-", "+");
        Assert.Equal(2, rows.Count);
        Assert.Equal(20000, Ts(rows[0].Timestamp));
        Assert.Equal(10000, Ts(rows[1].Timestamp));

        var one = await ts.NRevRangeAsync(keys, "-", "+", count: 1);
        Assert.Single(one);
        Assert.Equal(20000, Ts(one[0].Timestamp));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestNRangeAggregationPerKeyAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(3);
        await ts.AddAsync(keys[0], 1000, 10); await ts.AddAsync(keys[0], 2000, 20);
        await ts.AddAsync(keys[1], 1000, 4); await ts.AddAsync(keys[1], 2000, 6);
        await ts.AddAsync(keys[2], 1000, 100); await ts.AddAsync(keys[2], 2000, 50);

        var rows = await ts.NRangeAsync(keys, "-", "+",
            aggregations: [TsAggregation.Avg, TsAggregation.Sum, TsAggregation.Max],
            timeBucket: 100000);

        Assert.Single(rows);
        Assert.Equal(15, rows[0].Values[0]);
        Assert.Equal(10, rows[0].Values[1]);
        Assert.Equal(100, rows[0].Values[2]);
    }
}
