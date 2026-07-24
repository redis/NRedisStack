#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestReadAsync(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
{
    private static (long ts, double val)[] Pairs(IEnumerable<TimeSeriesTuple> tuples) =>
        tuples.Select(t => ((long)t.Time, t.Val)).ToArray();

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestReadBatchAsync(string endpointId)
    {
        SkipClusterPre8(endpointId);
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var key = CreateKeyName();
        for (long t = 1000; t <= 5000; t += 1000) await ts.AddAsync(key, t, t / 1000);

        var result = await ts.ReadAsync(key, 3000);
        Assert.Equal([(3000, 3), (4000, 4), (5000, 5)], Pairs(result));

        result = await ts.ReadAsync(key, 0, maxCount: 2);
        Assert.Equal([(1000, 1), (2000, 2)], Pairs(result));
        Assert.Empty(await ts.ReadAsync(key, 99999));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestReadAsyncEnumerablePagesThrough(string endpointId)
    {
        SkipClusterPre8(endpointId);
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var key = CreateKeyName();
        for (long t = 1000; t <= 5000; t += 1000) await ts.AddAsync(key, t, t / 1000);

        var collected = new List<TimeSeriesTuple>();
        await foreach (var tuple in ts.ReadAsyncEnumerable(key, 0, batchSize: 2))
        {
            collected.Add(tuple);
        }
        Assert.Equal([(1000, 1), (2000, 2), (3000, 3), (4000, 4), (5000, 5)], Pairs(collected));
    }
}
