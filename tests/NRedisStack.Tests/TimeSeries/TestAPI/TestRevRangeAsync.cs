#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestRevRangeAsync(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    private async Task<List<TimeSeriesTuple>> CreateData(TimeSeriesCommands ts, string key, int timeBucket, bool addSecondPointPerBucket = false)
    {
        var tuples = new List<TimeSeriesTuple>();
        for (var i = 0; i < 10; i++)
        {
            var timeStamp = await ts.AddAsync(key, i * timeBucket, i);
            tuples.Add(new(timeStamp, i));
            if (addSecondPointPerBucket)
            {
                await ts.AddAsync(key, (i * timeBucket) + 1, 2 * i);
            }
        }
        return tuples;
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestSimpleRevRange(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        Assert.Equal(ReverseData(tuples), await ts.RevRangeAsync(key, "-", "+"));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRevRangeCount(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        Assert.Equal(ReverseData(tuples).GetRange(0, 5), await ts.RevRangeAsync(key, "-", "+", count: 5));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRevRangeAggregation(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        Assert.Equal(ReverseData(tuples), await ts.RevRangeAsync(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.8.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRevRangeMultiAggregation(string endpointId)
    {
        var key = $"{CreateKeyName()}:{Guid.NewGuid():N}";
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = ReverseData(await CreateData(ts, key, 50));
        var res = await ts.RevRangeAsync(key, "-", "+", aggregation: new TsAggregations(TsAggregation.Min, TsAggregation.Avg, TsAggregation.Max, TsAggregation.Count), timeBucket: 50);

        Assert.Equal(tuples.Count, res.Count);
        for (int i = 0; i < res.Count; i++)
        {
            Assert.Equal(tuples[i].Time, res[i].Time);
            Assert.Equal(tuples[i].Val, res[i][0]);
            Assert.Equal(tuples[i].Val, res[i][1]);
            Assert.Equal(tuples[i].Val, res[i][2]);
            Assert.Equal(1, res[i][3]);
        }
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.8.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRevRangeMultiAggregationWithMultiplePointsPerBucket(string endpointId)
    {
        var key = $"{CreateKeyName()}:{Guid.NewGuid():N}";
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = ReverseData(await CreateData(ts, key, 50, addSecondPointPerBucket: true));
        var res = await ts.RevRangeAsync(key, "-", "+", aggregation: new TsAggregations(TsAggregation.Min, TsAggregation.Avg, TsAggregation.Max, TsAggregation.Count), timeBucket: 50);

        Assert.Equal(tuples.Count, res.Count);
        for (int i = 0; i < res.Count; i++)
        {
            var expected = tuples[i].Val;
            Assert.Equal(tuples[i].Time, res[i].Time);
            Assert.Equal(expected, res[i][0]);
            Assert.Equal(expected * 1.5, res[i][1]);
            Assert.Equal(expected * 2, res[i][2]);
            Assert.Equal(2, res[i][3]);
        }
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRevRangeAlign(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = new List<TimeSeriesTuple>()
        {
            new(1, 10),
            new(3, 5),
            new(11, 10),
            new(21, 11)
        };

        foreach (var tuple in tuples)
        {
            await ts.AddAsync(key, tuple.Time, tuple.Val);
        }

        // Aligh start
        var resStart = new List<TimeSeriesTuple>()
        {
            new(21, 1),
            new(11, 1),
            new(1, 2)
        };
        Assert.Equal(resStart, await ts.RevRangeAsync(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

        // Aligh end
        var resEnd = new List<TimeSeriesTuple>()
        {
            new(20, 1),
            new(10, 1),
            new(0, 2)
        };
        Assert.Equal(resEnd, await ts.RevRangeAsync(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

        // Align 1
        Assert.Equal(resStart, await ts.RevRangeAsync(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestMissingTimeBucket(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await ts.RevRangeAsync(key, "-", "+", aggregation: TsAggregation.Avg));
        Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestFilterBy(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);

        var res = await ts.RevRangeAsync(key, "-", "+", filterByValue: (0, 2));
        Assert.Equal(3, res.Count);
        Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

        var filterTs = new List<TimeStamp> { 0, 50, 100 };
        res = await ts.RevRangeAsync(key, "-", "+", filterByTs: filterTs);
        Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

        res = await ts.RevRangeAsync(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
        Assert.Equal(tuples.GetRange(2, 1), res);
    }
}
