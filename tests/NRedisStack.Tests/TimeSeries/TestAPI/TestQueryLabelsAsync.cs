#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestQueryLabelsAsync(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
{
    // TS.QUERYLABELS has no ordering contract (R.3), so compare order-independently.
    private static List<string> Sorted(IEnumerable<string> items)
    {
        var list = new List<string>(items);
        list.Sort(StringComparer.Ordinal);
        return list;
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestQueryLabelNamesAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(3);
        await ts.CreateAsync(keys[0], labels: [new TimeSeriesLabel("region", "us"), new TimeSeriesLabel("team", "cto"), new TimeSeriesLabel("sensor", "temp")]);
        await ts.CreateAsync(keys[1], labels: [new TimeSeriesLabel("region", "us"), new TimeSeriesLabel("team", "aut"), new TimeSeriesLabel("sensor", "hum")]);
        await ts.CreateAsync(keys[2], labels: [new TimeSeriesLabel("region", "eu"), new TimeSeriesLabel("team", "cto"), new TimeSeriesLabel("sensor", "temp")]);

        // No filter: union of label names across all indexed series.
        Assert.Equal(["region", "sensor", "team"], Sorted(await ts.QueryLabelNamesAsync()));

        // With a filter: union of label names across the matching series only.
        Assert.Equal(["region", "sensor", "team"], Sorted(await ts.QueryLabelNamesAsync(["region=us"])));

        // A filter matching no series is a valid, empty success (not an error).
        Assert.Empty(await ts.QueryLabelNamesAsync(["region=antarctica"]));
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "8.10.0")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestQueryLabelValuesAsync(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        var keys = CreateKeyNames(3);
        await ts.CreateAsync(keys[0], labels: [new TimeSeriesLabel("region", "us"), new TimeSeriesLabel("team", "cto")]);
        await ts.CreateAsync(keys[1], labels: [new TimeSeriesLabel("region", "us"), new TimeSeriesLabel("team", "aut")]);
        await ts.CreateAsync(keys[2], labels: [new TimeSeriesLabel("region", "eu"), new TimeSeriesLabel("team", "cto")]);

        // No filter: all distinct values of the chosen label.
        Assert.Equal(["eu", "us"], Sorted(await ts.QueryLabelValuesAsync("region")));

        // With a filter: values across the matching series only.
        Assert.Equal(["aut", "cto"], Sorted(await ts.QueryLabelValuesAsync("team", ["region=us"])));

        // An unknown label yields a valid, empty success (not an error).
        Assert.Empty(await ts.QueryLabelValuesAsync("no_such_label"));
    }
}
