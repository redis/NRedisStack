#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using System.Linq;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestRead(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
{
    private static (long ts, double val)[] Pairs(IEnumerable<TimeSeriesTuple> tuples) =>
        tuples.Select(t => ((long)t.Time, t.Val)).ToArray();

    private static string Seed(ITimeSeriesCommands ts, string key)
    {
        for (long t = 1000; t <= 5000; t += 1000) ts.Add(key, t, t / 1000);
        return key;
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestReadBatch(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var key = Seed(ts, CreateKeyName());

        // from 0: everything, ascending
        Assert.Equal([(1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)], Pairs(ts.Read(key, 0)));
        // inclusive lower bound (>=)
        Assert.Equal([(3000, 3), (4000, 4), (5000, 5)], Pairs(ts.Read(key, 3000)));
        // MAX_COUNT caps the batch
        Assert.Equal([(1000, 1), (2000, 2)], Pairs(ts.Read(key, 0, maxCount: 2)));
        // '+' returns the latest existing sample
        Assert.Equal([(5000, 5)], Pairs(ts.Read(key, "+")));
        // nothing at/after the bound is a valid empty reply
        Assert.Empty(ts.Read(key, 99999));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestReadEnumerablePagesThrough(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var key = Seed(ts, CreateKeyName());

        // batchSize 2 => pages of 2,2,1; cursor advances past the last sample each time, no dupes/gaps.
        var all = Pairs(ts.ReadEnumerable(key, 0, batchSize: 2));
        Assert.Equal([(1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)], all);

        // no batchSize: single underlying call, same full sequence
        Assert.Equal(all, Pairs(ts.ReadEnumerable(key, 0)));

        // starting mid-series
        Assert.Equal([(4000, 4), (5000, 5)], Pairs(ts.ReadEnumerable(key, 4000, batchSize: 2)));
    }
}
