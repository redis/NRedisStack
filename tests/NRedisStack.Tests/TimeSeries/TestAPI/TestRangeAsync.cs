#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestRangeAsync(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    private async Task<List<TimeSeriesTuple>> CreateData(TimeSeriesCommands ts, string key, int timeBucket)
    {
        var tuples = new List<TimeSeriesTuple>();
        for (var i = 0; i < 10; i++)
        {
            var timeStamp = await ts.AddAsync(key, i * timeBucket, i);
            tuples.Add(new(timeStamp, i));
        }
        return tuples;
    }

    [Fact]
    public async Task TestSimpleRange()
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase();
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        Assert.Equal(tuples, await ts.RangeAsync(key, "-", "+"));
    }

    [Fact]
    public async Task TestRangeCount()
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase();
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        Assert.Equal(tuples.GetRange(0, 5), await ts.RangeAsync(key, "-", "+", count: 5));
    }

    [Fact]
    public async Task TestRangeAggregation()
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase();
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        Assert.Equal(tuples, await ts.RangeAsync(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
    }

    [Fact]
    public async Task TestRangeAlign()
    {
        var key = CreateKeyName();
        IDatabase db = GetCleanDatabase();
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
            new(1, 2),
            new(11, 1),
            new(21, 1)
        };
        Assert.Equal(resStart, await ts.RangeAsync(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

        // Aligh end
        var resEnd = new List<TimeSeriesTuple>()
        {
            new(0, 2),
            new(10, 1),
            new(20, 1)
        };
        Assert.Equal(resEnd, await ts.RangeAsync(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

        // Align 1
        Assert.Equal(resStart, await ts.RangeAsync(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
    }

    [Fact]
    public async Task TestMissingTimeBucket()
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase();
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);
        var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await ts.RangeAsync(key, "-", "+", aggregation: TsAggregation.Avg));
        Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
    }

    [Fact]
    public async Task TestFilterBy()
    {
        var key = CreateKeyName();
        var db = GetCleanDatabase();
        var ts = db.TS();
        var tuples = await CreateData(ts, key, 50);

        var res = await ts.RangeAsync(key, "-", "+", filterByValue: (0, 2)); // The first 3 tuples
        Assert.Equal(3, res.Count);
        Assert.Equal(tuples.GetRange(0, 3), res);

        var filterTs = new List<TimeStamp> { 0, 50, 100 }; // Also the first 3 tuples
        res = await ts.RangeAsync(key, "-", "+", filterByTs: filterTs);
        Assert.Equal(tuples.GetRange(0, 3), res);

        res = await ts.RangeAsync(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5)); // The third tuple
        Assert.Equal(tuples.GetRange(2, 1), res);
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestLatestAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        await ts.CreateAsync("ts1");
        await ts.CreateAsync("ts2");
        await ts.CreateRuleAsync("ts1", new("ts2", 10, TsAggregation.Sum));
        await ts.AddAsync("ts1", 1, 1);
        await ts.AddAsync("ts1", 2, 3);
        await ts.AddAsync("ts1", 11, 7);
        await ts.AddAsync("ts1", 13, 1);
        var range = await ts.RangeAsync("ts1", 0, 20);
        Assert.Equal(4, range.Count);

        var compact = new TimeSeriesTuple(0, 4);
        var latest = new TimeSeriesTuple(10, 8);

        // get
        Assert.Equal(compact, await ts.GetAsync("ts2"));

        Assert.Equal(latest, await ts.GetAsync("ts2", true));

        // range
        Assert.Equal(new List<TimeSeriesTuple>() { compact }, await ts.RangeAsync("ts2", 0, 10));

        Assert.Equal(new List<TimeSeriesTuple>() { compact, latest }, await ts.RangeAsync("ts2", 0, 10, true));

        // revrange
        Assert.Equal(new List<TimeSeriesTuple>() { compact }, await ts.RevRangeAsync("ts2", 0, 10));

        Assert.Equal(new List<TimeSeriesTuple>() { latest, compact }, await ts.RevRangeAsync("ts2", 0, 10, true));
    }

    [SkipIfRedisTheory(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAlignTimestampAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        ts.Create("ts1");
        ts.Create("ts2");
        ts.Create("ts3");
        ts.CreateRule("ts1", new("ts2", 10, TsAggregation.Count), 0);
        ts.CreateRule("ts1", new("ts3", 10, TsAggregation.Count), 1);
        ts.Add("ts1", 1, 1);
        ts.Add("ts1", 10, 3);
        ts.Add("ts1", 21, 7);
        Assert.Equal(2, (await ts.RangeAsync("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
        Assert.Single((await ts.RangeAsync("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)));
    }

    [Fact]
    public async Task TestBucketTimestampAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();

        ts.Create("t1");

        ts.Add("t1", 15, 1);
        ts.Add("t1", 17, 4);
        ts.Add("t1", 51, 3);
        ts.Add("t1", 73, 5);
        ts.Add("t1", 75, 3);

        var rangeHigh = await ts.RangeAsync("t1", 0, 100,
            align: 0,
            aggregation: TsAggregation.Max,
            timeBucket: 10);

        var expected = new List<TimeSeriesTuple>();
        expected.Add(new(10, 4.0));
        expected.Add(new(50, 3.0));
        expected.Add(new(70, 5.0));
        Assert.Equal(rangeHigh, expected);

        rangeHigh = await ts.RangeAsync("t1", 0, 100,
            align: 0,
            aggregation: TsAggregation.Max,
            timeBucket: 10,
            bt: TsBucketTimestamps.high);

        expected.Clear();
        expected.Add(new(20, 4.0));
        expected.Add(new(60, 3.0));
        expected.Add(new(80, 5.0));
        Assert.Equal(rangeHigh, expected);

        var rangeLow = await ts.RangeAsync("t1", 0, 100,
            align: 0,
            aggregation: TsAggregation.Max,
            timeBucket: 10,
            bt: TsBucketTimestamps.low);
        expected.Clear();
        expected.Add(new(10, 4.0));
        expected.Add(new(50, 3.0));
        expected.Add(new(70, 5.0));
        Assert.Equal(rangeLow, expected);

        var rangeMid = await ts.RangeAsync("t1", 0, 100,
            align: 0,
            aggregation: TsAggregation.Max,
            timeBucket: 10,
            bt: TsBucketTimestamps.mid);
        expected.Clear();
        expected.Add(new(15, 4.0));
        expected.Add(new(55, 3.0));
        expected.Add(new(75, 5.0));
        Assert.Equal(rangeMid, expected);

    }

    [Fact]
    public async Task TestEmptyAsync()
    {
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();

        ts.Create("t1");

        ts.Add("t1", 15, 1);
        ts.Add("t1", 17, 4);
        ts.Add("t1", 51, 3);
        ts.Add("t1", 73, 5);
        ts.Add("t1", 75, 3);

        var range = await ts.RangeAsync("t1", 0, 100,
            align: 0,
            aggregation: TsAggregation.Max,
            timeBucket: 10);

        var expected = new List<TimeSeriesTuple>();
        expected.Add(new(10, 4.0));
        expected.Add(new(50, 3.0));
        expected.Add(new(70, 5.0));
        Assert.Equal(range, expected);

        range = await ts.RangeAsync("t1", 0, 100,
            align: 0,
            aggregation: TsAggregation.Max,
            timeBucket: 10,
            empty: true);

        expected.Clear();

        expected.Add(new(10, 4.0));
        expected.Add(new(20, double.NaN));
        expected.Add(new(30, double.NaN));
        expected.Add(new(40, double.NaN));
        expected.Add(new(50, 3.0));
        expected.Add(new(60, double.NaN));
        expected.Add(new(70, 5.0));

        for (int i = 0; i < range.Count(); i++)
        {
            Assert.Equal(range[i].Time.Value, expected[i].Time.Value);
            Assert.Equal(range[i].Val, expected[i].Val);
        }
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.5.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRangeCountNanAggregationAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var key = CreateKeyName();

        // Create a time series and add data including NaN values
        await ts.CreateAsync(key);
        await ts.AddAsync(key, 10, 1.0);
        await ts.AddAsync(key, 20, double.NaN);
        await ts.AddAsync(key, 30, 3.0);
        await ts.AddAsync(key, 40, double.NaN);
        await ts.AddAsync(key, 50, 5.0);

        // Test CountNan aggregation - should count NaN values
        var range = await ts.RangeAsync(key, 0, 100, aggregation: TsAggregation.CountNan, timeBucket: 100);
        Assert.Single(range);
        Assert.Equal(2, range[0].Val); // 2 NaN values
    }

    [SkipIfRedisTheory(Comparison.LessThan, "8.5.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRangeCountAllAggregationAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var key = CreateKeyName();

        // Create a time series and add data including NaN values
        await ts.CreateAsync(key);
        await ts.AddAsync(key, 10, 1.0);
        await ts.AddAsync(key, 20, double.NaN);
        await ts.AddAsync(key, 30, 3.0);
        await ts.AddAsync(key, 40, double.NaN);
        await ts.AddAsync(key, 50, 5.0);

        // Test CountAll aggregation - should count all values including NaN
        var range = await ts.RangeAsync(key, 0, 100, aggregation: TsAggregation.CountAll, timeBucket: 100);
        Assert.Single(range);
        Assert.Equal(5, range[0].Val); // 5 total values (3 regular + 2 NaN)
    }
}