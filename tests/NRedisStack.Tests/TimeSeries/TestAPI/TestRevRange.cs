#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestRevRange(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    private List<TimeSeriesTuple> CreateData(ITimeSeriesCommands ts, string key, int timeBucket)
    {
        var tuples = new List<TimeSeriesTuple>();
        for (var i = 0; i < 10; i++)
        {
            var timeStamp = ts.Add(key, i * timeBucket, i);
            tuples.Add(new(timeStamp, i));
        }
        return tuples;
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestSimpleRevRange(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = CreateData(ts, key, 50);
        Assert.Equal(ReverseData(tuples), ts.RevRange(key, "-", "+"));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestRevRangeCount(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = CreateData(ts, key, 50);
        Assert.Equal(ReverseData(tuples).GetRange(0, 5), ts.RevRange(key, "-", "+", count: 5));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestRevRangeAggregation(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = CreateData(ts, key, 50);
        Assert.Equal(ReverseData(tuples), ts.RevRange(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestRevRangeAlign(string endpointId)
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
            ts.Add(key, tuple.Time, tuple.Val);
        }

        // Aligh start
        var resStart = new List<TimeSeriesTuple>()
        {
            new(21, 1),
            new(11, 1),
            new(1, 2)
        };
        Assert.Equal(resStart, ts.RevRange(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

        // Aligh end
        var resEnd = new List<TimeSeriesTuple>()
        {
            new(20, 1),
            new(10, 1),
            new(0, 2)
        };
        Assert.Equal(resEnd, ts.RevRange(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

        // Align 1
        Assert.Equal(resStart, ts.RevRange(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestMissingTimeBucket(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = CreateData(ts, key, 50);
        var ex = Assert.Throws<ArgumentException>(() => ts.RevRange(key, "-", "+", aggregation: TsAggregation.Avg));
        Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestFilterBy(string endpointId)
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var tuples = CreateData(ts, key, 50);

        var res = ts.RevRange(key, "-", "+", filterByValue: (0, 2));
        Assert.Equal(3, res.Count);
        Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

        var filterTs = new List<TimeStamp> { 0, 50, 100 };
        res = ts.RevRange(key, "-", "+", filterByTs: filterTs);
        Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

        res = ts.RevRange(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
        Assert.Equal(tuples.GetRange(2, 1), res);
    }
}