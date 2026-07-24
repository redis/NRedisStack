#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestNRange(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
{
    private static long Ts(TimeStamp t) => (long)t.Value;

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNRangeOuterJoin(string endpointId)
    {
        SkipClusterPre8(endpointId);
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(3);
        // a and c overlap only partially with b => NaN holes in the pivot.
        ts.Add(keys[0], 10000, 10); ts.Add(keys[0], 20000, 12);
        ts.Add(keys[1], 10000, 13);                              // no 20000
        ts.Add(keys[2], 20000, 99);                              // no 10000

        var rows = ts.NRange(keys, "-", "+");

        Assert.Equal(2, rows.Count);
        // row order is increasing timestamp for NRANGE
        Assert.Equal(10000, Ts(rows[0].Timestamp));
        Assert.Equal(3, rows[0].Values.Count);                  // values.length == numkeys (R.11)
        Assert.Equal(10, rows[0].Values[0]);
        Assert.Equal(13, rows[0].Values[1]);
        Assert.True(double.IsNaN(rows[0].Values[2]));           // missing sample surfaced as NaN (R.12)

        Assert.Equal(20000, Ts(rows[1].Timestamp));
        Assert.Equal(12, rows[1].Values[0]);
        Assert.True(double.IsNaN(rows[1].Values[1]));
        Assert.Equal(99, rows[1].Values[2]);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNRevRangeOrderAndCount(string endpointId)
    {
        SkipClusterPre8(endpointId);
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(2);
        ts.Add(keys[0], 10000, 1); ts.Add(keys[0], 20000, 2);
        ts.Add(keys[1], 10000, 3); ts.Add(keys[1], 20000, 4);

        var rows = ts.NRevRange(keys, "-", "+");
        Assert.Equal(2, rows.Count);
        Assert.Equal(20000, Ts(rows[0].Timestamp));             // reverse (decreasing) order
        Assert.Equal(10000, Ts(rows[1].Timestamp));

        // COUNT limits rows after merge, in decreasing order for NREVRANGE (R.8)
        var one = ts.NRevRange(keys, "-", "+", count: 1);
        Assert.Single(one);
        Assert.Equal(20000, Ts(one[0].Timestamp));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNRangeEmpty(string endpointId)
    {
        SkipClusterPre8(endpointId);
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(2);
        ts.Add(keys[0], 10000, 1);
        ts.Add(keys[1], 10000, 2);

        // a range that matches no samples is a valid, empty success.
        Assert.Empty(ts.NRange(keys, 30000, 40000));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNRangeAggregationPerKey(string endpointId)
    {
        SkipClusterPre8(endpointId);
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(3);
        // all in one 100000ms bucket
        ts.Add(keys[0], 1000, 10); ts.Add(keys[0], 2000, 20);   // avg => 15
        ts.Add(keys[1], 1000, 4); ts.Add(keys[1], 2000, 6);     // sum => 10
        ts.Add(keys[2], 1000, 100); ts.Add(keys[2], 2000, 50);  // max => 100

        // exactly one aggregator per key, separate tokens (R.9)
        var rows = ts.NRange(keys, "-", "+",
            aggregations: [TsAggregation.Avg, TsAggregation.Sum, TsAggregation.Max],
            timeBucket: 100000);

        Assert.Single(rows);
        Assert.Equal(3, rows[0].Values.Count);
        Assert.Equal(15, rows[0].Values[0]);
        Assert.Equal(10, rows[0].Values[1]);
        Assert.Equal(100, rows[0].Values[2]);
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNRangeMultipleAggregatorsPerKey(string endpointId)
    {
        SkipClusterPre8(endpointId);
        // A key may request several aggregators (comma-joined server-side), contributing multiple value
        // columns; total columns == sum of aggregators across keys, not numkeys.
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(2);
        ts.Add(keys[0], 1000, 10); ts.Add(keys[0], 2000, 20);   // avg => 15, sum => 30
        ts.Add(keys[1], 1000, 4); ts.Add(keys[1], 2000, 6);     // max => 6

        // key[0] gets two aggregators, key[1] gets one (implicitly widened from TsAggregation).
        var rows = ts.NRange(keys, "-", "+",
            aggregations: [new TsAggregations(TsAggregation.Avg, TsAggregation.Sum), TsAggregation.Max],
            timeBucket: 100000);

        Assert.Single(rows);
        Assert.Equal(3, rows[0].Values.Count);                  // 2 (key[0]) + 1 (key[1]), not numkeys
        Assert.Equal(15, rows[0].Values[0]);                    // key[0] avg
        Assert.Equal(30, rows[0].Values[1]);                    // key[0] sum
        Assert.Equal(6, rows[0].Values[2]);                     // key[1] max
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNRangeAggregatorCountMismatchIsServerError(string endpointId)
    {
        SkipClusterPre8(endpointId);
        // The client does not validate aggregator-vs-key count; the server rejects the mismatch (R.9).
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(3);
        foreach (var k in keys) ts.Add(k, 1000, 1);

        Assert.Throws<StackExchange.Redis.RedisServerException>(() => ts.NRange(keys, "-", "+",
            aggregations: [TsAggregation.Avg], timeBucket: 100000));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestNRangeAggregationOptionsRequireAggregators(string endpointId)
    {
        SkipClusterPre8(endpointId);
        // aggregation-clause options have no wire representation without an aggregator; supplying them without
        // one is local misuse and throws (mirrors TS.RANGE) rather than being silently dropped.
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(2);

        Assert.Throws<ArgumentException>(() => ts.NRange(keys, "-", "+", timeBucket: 1000));
        Assert.Throws<ArgumentException>(() => ts.NRange(keys, "-", "+", flags: TimeSeriesRangeFlags.Empty));
        Assert.Throws<ArgumentException>(() => ts.NRange(keys, "-", "+", align: 0));
    }
}
