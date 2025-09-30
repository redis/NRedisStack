#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using Xunit;
using NRedisStack.RedisStackCommands;
using NRedisStack.Tests;


namespace NRedisTimeSeries.Test.TestDataTypes;

public class TestInformation(EndpointsFixture endpointsFixture)
    : AbstractNRedisStackTest(endpointsFixture)
{
    [SkipIfRedisFact(Comparison.GreaterThanOrEqual, "7.9.240")]
    public void TestInformationSync()
    {
        string key = CreateKeyName();
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();
        ts.Add(key, "*", 1.1);
        ts.Add(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

        TimeSeriesInformation info = ts.Info(key);
        TimeSeriesInformation infoDebug = ts.Info(key, debug: true);

        Assert.Equal(0, info.RetentionTime);
        Assert.Equal(1, info.ChunkCount);
        Assert.Null(info.DuplicatePolicy);
        Assert.Null(info.KeySelfName);
        Assert.Null(info.Chunks);

        Assert.Equal(0, infoDebug.RetentionTime);
        Assert.Equal(1, infoDebug.ChunkCount);
        Assert.Null(infoDebug.DuplicatePolicy);
        Assert.Equal(infoDebug.KeySelfName, key);
        Assert.Single(infoDebug.Chunks!);
    }

    [SkipIfRedisFact(Comparison.GreaterThanOrEqual, "7.9.240")]
    public async Task TestInformationAsync()
    {
        string key = CreateKeyName();
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();
        await ts.AddAsync(key, "*", 1.1);
        await ts.AddAsync(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

        TimeSeriesInformation info = await ts.InfoAsync(key);
        TimeSeriesInformation infoDebug = await ts.InfoAsync(key, debug: true);

        Assert.Equal(0, info.RetentionTime);
        Assert.Equal(1, info.ChunkCount);
        Assert.Null(info.DuplicatePolicy);
        Assert.Null(info.KeySelfName);
        Assert.Null(info.Chunks);

        Assert.Equal(0, infoDebug.RetentionTime);
        Assert.Equal(1, infoDebug.ChunkCount);
        Assert.Null(infoDebug.DuplicatePolicy);
        Assert.Equal(infoDebug.KeySelfName, key);
        Assert.Single(infoDebug.Chunks!);
    }

    [SkipIfRedisFact(Comparison.LessThan, "7.9.240")]
    public void TestInformationSync_CE80()
    {
        string key = CreateKeyName();
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();
        ts.Add(key, "*", 1.1);
        ts.Add(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

        TimeSeriesInformation info = ts.Info(key);
        TimeSeriesInformation infoDebug = ts.Info(key, debug: true);

        Assert.Equal(0, info.RetentionTime);
        Assert.Equal(1, info.ChunkCount);
        Assert.Equal(TsDuplicatePolicy.BLOCK, info.DuplicatePolicy);
        Assert.Null(info.KeySelfName);
        Assert.Null(info.Chunks);

        Assert.Equal(0, infoDebug.RetentionTime);
        Assert.Equal(1, infoDebug.ChunkCount);
        Assert.Equal(TsDuplicatePolicy.BLOCK, infoDebug.DuplicatePolicy);
        Assert.Equal(infoDebug.KeySelfName, key);
        Assert.Single(infoDebug.Chunks!);
    }

    [SkipIfRedisFact(Comparison.LessThan, "7.9.240")]
    public async Task TestInformationAsync_CE80()
    {
        string key = CreateKeyName();
        IDatabase db = GetCleanDatabase();
        var ts = db.TS();
        await ts.AddAsync(key, "*", 1.1);
        await ts.AddAsync(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

        TimeSeriesInformation info = await ts.InfoAsync(key);
        TimeSeriesInformation infoDebug = await ts.InfoAsync(key, debug: true);

        Assert.Equal(0, info.RetentionTime);
        Assert.Equal(1, info.ChunkCount);
        Assert.Equal(TsDuplicatePolicy.BLOCK, info.DuplicatePolicy);
        Assert.Null(info.KeySelfName);
        Assert.Null(info.Chunks);

        Assert.Equal(0, infoDebug.RetentionTime);
        Assert.Equal(1, infoDebug.ChunkCount);
        Assert.Equal(TsDuplicatePolicy.BLOCK, infoDebug.DuplicatePolicy);
        Assert.Equal(infoDebug.KeySelfName, key);
        Assert.Single(infoDebug.Chunks!);
    }
}