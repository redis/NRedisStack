using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI;

public class TestAlter(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
{
    private readonly string key = "ALTER_TESTS";


    [Fact]
    [Obsolete]
    public void TestAlterRetentionTime()
    {
        long retentionTime = 5000;
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();
        ts.Create(key);
        Assert.True(ts.Alter(key, retentionTime: retentionTime));
        TimeSeriesInformation info = ts.Info(key);
        Assert.Equal(retentionTime, info.RetentionTime);
    }

    [Fact]
    [Obsolete]
    public void TestAlterLabels()
    {
        TimeSeriesLabel label = new("key", "value");
        var labels = new List<TimeSeriesLabel> { label };
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();
        ts.Create(key);
        Assert.True(ts.Alter(key, labels: labels));
        TimeSeriesInformation info = ts.Info(key);
        Assert.Equal(labels, info.Labels);
        labels.Clear();
        Assert.True(ts.Alter(key, labels: labels));
        info = ts.Info(key);
        Assert.Equal(labels, info.Labels);
    }

    [Fact]
    [Obsolete]
    public void TestAlterPolicyAndChunk()
    {
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();
        ts.Create(key);
        Assert.True(ts.Alter(key, chunkSizeBytes: 128, duplicatePolicy: TsDuplicatePolicy.MIN));
        TimeSeriesInformation info = ts.Info(key);
        Assert.Equal(128, info.ChunkSize);
        Assert.Equal(TsDuplicatePolicy.MIN, info.DuplicatePolicy);
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.4.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAlterAndIgnoreValues(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var ts = db.TS();
        ts.Create(key, new TsCreateParamsBuilder().build());
        var parameters = new TsAlterParamsBuilder().AddIgnoreValues(13, 14).build();
        Assert.True(ts.Alter(key, parameters));

        int j = -1, k = -1;
        RedisResult info = TimeSeriesHelper.getInfo(db, key, out j, out k);
        Assert.NotNull(info);
        Assert.True(info.Length > 0);
        Assert.NotEqual(-1, j);
        Assert.NotEqual(-1, k);
        Assert.Equal(13, (long)info[j + 1]);
        Assert.Equal(14, (long)info[k + 1]);
    }
}