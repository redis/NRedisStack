using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.Tdigest;

public class TdigestTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "TDIGEST_TESTS";

    public TdigestTests(EndpointsFixture endpointsFixture) : base(endpointsFixture)
    {
    }

    private void AssertMergedUnmergedNodes(ITdigestCommands tdigest, string key, int mergedNodes, int unmergedNodes)
    {
        var info = tdigest.Info(key);
        Assert.Equal((long)mergedNodes, info.MergedNodes);
        Assert.Equal((long)unmergedNodes, info.UnmergedNodes);
    }

    private void AssertTotalWeight(ITdigestCommands tdigest, string key, double totalWeight)
    {
        var info = tdigest.Info(key);
        Assert.Equal(totalWeight, info.MergedWeight + info.UnmergedWeight);
        //Assert.Equal(totalWeight, 0.01);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreateSimple(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        Assert.True(tdigest.Create(key));

        var info = tdigest.Info(key);
        Assert.Equal(610, info.Capacity);
        Assert.Equal(100, info.Compression);
        Assert.Equal(9768, info.MemoryUsage);
        Assert.Equal(0, info.MergedNodes);
        Assert.Equal(0, info.MergedWeight);
        Assert.Equal(0, info.Observations);
        Assert.Equal(0, info.TotalCompressions);
        Assert.Equal(0, info.UnmergedWeight);
        Assert.Equal(0, info.UnmergedNodes);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCreateSimpleAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        Assert.True(await tdigest.CreateAsync(key));

        var info = await tdigest.InfoAsync(key);
        Assert.Equal(610, info.Capacity);
        Assert.Equal(100, info.Compression);
        Assert.Equal(9768, info.MemoryUsage);
        Assert.Equal(0, info.MergedNodes);
        Assert.Equal(0, info.MergedWeight);
        Assert.Equal(0, info.Observations);
        Assert.Equal(0, info.TotalCompressions);
        Assert.Equal(0, info.UnmergedWeight);
        Assert.Equal(0, info.UnmergedNodes);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCreateAndInfo(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        for (int i = 100; i < 1000; i += 100)
        {
            string myKey = "td-" + i;
            Assert.True(tdigest.Create(myKey, i));

            var info = tdigest.Info(myKey);
            Assert.Equal(i, info.Compression);
        }
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCreateAndInfoAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        for (int i = 100; i < 1000; i += 100)
        {
            string myKey = "td-" + i;
            Assert.True(await tdigest.CreateAsync(myKey, i));

            var info = await tdigest.InfoAsync(myKey);
            Assert.Equal(i, info.Compression);
        }
    }


    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestRankCommands(string endpointId)
    {
        //final String key = "ranks";
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();
        tdigest.Create(key);
        tdigest.Add(key, 2d, 3d, 5d);
        Assert.Equal(new long[] { 0, 2 }, tdigest.Rank(key, 2, 4));
        Assert.Equal(new long[] { 0, 1 }, tdigest.RevRank(key, 5, 4));
        Assert.Equal(new double[] { 2, 3 }, tdigest.ByRank(key, 0, 1));
        Assert.Equal(new double[] { 5, 3 }, tdigest.ByRevRank(key, 0, 1));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestRankCommandsAsync(string endpointId)
    {
        //final String key = "ranks";
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();
        tdigest.Create(key);
        tdigest.Add(key, 2d, 3d, 5d);
        Assert.Equal(new long[] { 0, 2 }, await tdigest.RankAsync(key, 2, 4));
        Assert.Equal(new long[] { 0, 1 }, await tdigest.RevRankAsync(key, 5, 4));
        Assert.Equal(new double[] { 2, 3 }, await tdigest.ByRankAsync(key, 0, 1));
        Assert.Equal(new double[] { 5, 3 }, await tdigest.ByRevRankAsync(key, 0, 1));
    }


    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestReset(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create("reset", 100);
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        // on empty
        Assert.True(tdigest.Reset("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        // tdigest.Add("reset", RandomValue(), RandomValue(), RandomValue());
        tdigest.Add("reset", RandomValue(), RandomValue(), RandomValue());
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 3);

        Assert.True(tdigest.Reset("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestResetAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("reset", 100);
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        // on empty
        Assert.True(await tdigest.ResetAsync("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        //await tdigest.AddAsync("reset", RandomValue(), RandomValue(), RandomValue());
        tdigest.Add("reset", RandomValue(), RandomValue(), RandomValue());

        AssertMergedUnmergedNodes(tdigest, "reset", 0, 3);

        Assert.True(await tdigest.ResetAsync("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestAdd(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create("tdadd", 100);

        Assert.True(tdigest.Add("tdadd", RandomValue()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 1);

        Assert.True(tdigest.Add("tdadd", RandomValue(), RandomValue(), RandomValue(), RandomValue()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 5);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestAddAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("tdadd", 100);

        Assert.True(await tdigest.AddAsync("tdadd", RandomValue()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 1);

        Assert.True(await tdigest.AddAsync("tdadd", RandomValue(), RandomValue(), RandomValue(), RandomValue()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 5);
    }

    [SkipIfRedis(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestMerge(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create("td2", 100);
        tdigest.Create("td4m", 100);

        Assert.True(tdigest.Merge("td2", sourceKeys: "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 0, 0);

        // tdigest.Add("td2", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        // tdigest.Add("td4m", DefinedValueWeight(1, 100), DefinedValueWeight(1, 100));
        tdigest.Add("td2", 1, 1, 1);
        tdigest.Add("td4m", 1, 1);

        Assert.True(tdigest.Merge("td2", sourceKeys: "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 3, 2);
    }


    [SkipIfRedis(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestMergeAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("td2", 100);
        await tdigest.CreateAsync("td4m", 100);

        Assert.True(await tdigest.MergeAsync("td2", sourceKeys: "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 0, 0);

        // await tdigest.AddAsync("td2", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        // await tdigest.AddAsync("td4m", DefinedValueWeight(1, 100), DefinedValueWeight(1, 100));

        await tdigest.AddAsync("td2", 1, 1, 1);
        await tdigest.AddAsync("td4m", 1, 1);

        Assert.True(await tdigest.MergeAsync("td2", sourceKeys: "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 3, 2);
    }

    [SkipIfRedis(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void MergeMultiAndParams(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();
        tdigest.Create("from1", 100);
        tdigest.Create("from2", 200);

        // tdigest.Add("from1", 1d, 1);
        // tdigest.Add("from2", 1d, 10);
        tdigest.Add("from1", 1d);
        tdigest.Add("from2", WeightedValue(1d, 10));

        Assert.True(tdigest.Merge("to", 2, sourceKeys: new RedisKey[] { "from1", "from2" }));
        AssertTotalWeight(tdigest, "to", 11d);

        Assert.True(tdigest.Merge("to", 50, true, "from1", "from2"));
        Assert.Equal(50, tdigest.Info("to").Compression);
    }

    [SkipIfRedis(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task MergeMultiAndParamsAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();
        tdigest.Create("from1", 100);
        tdigest.Create("from2", 200);

        // tdigest.Add("from1", 1d, 1);
        // tdigest.Add("from2", 1d, 10);
        tdigest.Add("from1", 1d);
        tdigest.Add("from2", WeightedValue(1d, 10));

        Assert.True(await tdigest.MergeAsync("to", 2, sourceKeys: new RedisKey[] { "from1", "from2" }));
        AssertTotalWeight(tdigest, "to", 11d);

        Assert.True(await tdigest.MergeAsync("to", 50, true, "from1", "from2"));
        Assert.Equal(50, tdigest.Info("to").Compression);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestCDF(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create("tdcdf", 100);
        foreach (var item in tdigest.CDF("tdcdf", 50))
        {
            Assert.Equal(double.NaN, item);
        }

        // tdigest.Add("tdcdf", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        // tdigest.Add("tdcdf", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));

        tdigest.Add("tdcdf", 1, 1, 1);
        tdigest.Add("tdcdf", 100, 100);
        Assert.Equal(new double[] { 0.6 }, tdigest.CDF("tdcdf", 50));
        tdigest.CDF("tdcdf", 25, 50, 75); // TODO: Why needed?
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestCDFAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("tdcdf", 100);
        foreach (var item in (await tdigest.CDFAsync("tdcdf", 50)))
        {
            Assert.Equal(double.NaN, item);
        }

        // await tdigest.AddAsync("tdcdf", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        // await tdigest.AddAsync("tdcdf", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        tdigest.Add("tdcdf", 1, 1, 1);
        tdigest.Add("tdcdf", 100, 100);

        Assert.Equal(new double[] { 0.6 }, await tdigest.CDFAsync("tdcdf", 50));
        await tdigest.CDFAsync("tdcdf", 25, 50, 75); // TODO: Why needed?

    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestQuantile(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create("tdqnt", 100);
        var resDelete = tdigest.Quantile("tdqnt", 0.5);
        Assert.Equal(new double[] { double.NaN }, tdigest.Quantile("tdqnt", 0.5));

        // tdigest.Add("tdqnt", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        // tdigest.Add("tdqnt", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        tdigest.Add("tdqnt", 1, 1, 1);
        tdigest.Add("tdqnt", 100, 100);
        Assert.Equal(new double[] { 1 }, tdigest.Quantile("tdqnt", 0.5));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestQuantileAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create("tdqnt", 100);
        var resDelete = await tdigest.QuantileAsync("tdqnt", 0.5);
        Assert.Equal(new double[] { double.NaN }, await tdigest.QuantileAsync("tdqnt", 0.5));

        // await tdigest.AddAsync("tdqnt", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        // await tdigest.AddAsync("tdqnt", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        tdigest.Add("tdqnt", 1, 1, 1);
        tdigest.Add("tdqnt", 100, 100);
        Assert.Equal(new double[] { 1 }, await tdigest.QuantileAsync("tdqnt", 0.5));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestMinAndMax(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create(key, 100);
        Assert.Equal(double.NaN, tdigest.Min(key));
        Assert.Equal(double.NaN, tdigest.Max(key));

        // tdigest.Add(key, DefinedValueWeight(2, 1));
        // tdigest.Add(key, DefinedValueWeight(5, 1));
        tdigest.Add(key, 2);
        tdigest.Add(key, 5);
        Assert.Equal(2d, tdigest.Min(key));
        Assert.Equal(5d, tdigest.Max(key));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestMinAndMaxAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync(key, 100);
        Assert.Equal(double.NaN, await tdigest.MinAsync(key));
        Assert.Equal(double.NaN, await tdigest.MaxAsync(key));

        // await tdigest.AddAsync(key, DefinedValueWeight(2, 1));
        // await tdigest.AddAsync(key, DefinedValueWeight(5, 1));
        tdigest.Add(key, 2);
        tdigest.Add(key, 5);
        Assert.Equal(2d, await tdigest.MinAsync(key));
        Assert.Equal(5d, await tdigest.MaxAsync(key));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestTrimmedMean(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        tdigest.Create(key, 500);

        for (int i = 0; i < 20; i++)
        {
            //tdigest.Add(key, new Tuple<double, long>(i, 1));
            tdigest.Add(key, i);
        }

        Assert.Equal(9.5, tdigest.TrimmedMean(key, 0.1, 0.9));
        Assert.Equal(9.5, tdigest.TrimmedMean(key, 0.0, 1.0));
        Assert.Equal(4.5, tdigest.TrimmedMean(key, 0.0, 0.5));
        Assert.Equal(14.5, tdigest.TrimmedMean(key, 0.5, 1.0));
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task TestTrimmedMeanAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync(key, 500);

        for (int i = 0; i < 20; i++)
        {
            // await tdigest.AddAsync(key, new Tuple<double, long>(i, 1));
            tdigest.Add(key, i);
        }

        Assert.Equal(9.5, await tdigest.TrimmedMeanAsync(key, 0.1, 0.9));
        Assert.Equal(9.5, await tdigest.TrimmedMeanAsync(key, 0.0, 1.0));
        Assert.Equal(4.5, await tdigest.TrimmedMeanAsync(key, 0.0, 0.5));
        Assert.Equal(14.5, await tdigest.TrimmedMeanAsync(key, 0.5, 1.0));
    }


    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestModulePrefixs(string endpointId)
    {
        var redis = GetConnection(endpointId);
        IDatabase db1 = redis.GetDatabase();
        IDatabase db2 = redis.GetDatabase();

        var tdigest1 = db1.TDIGEST();
        var tdigest2 = db2.TDIGEST();

        Assert.NotEqual(tdigest1.GetHashCode(), tdigest2.GetHashCode());
    }

    private static double RandomValue()
    {
        Random random = new Random();
        return random.NextDouble() * 10000;
    }

    static Tuple<double, long> RandomValueWeight()
    {
        Random random = new Random();

        return new Tuple<double, long>(random.NextDouble() * 10000, random.Next() + 1);
    }

    static Tuple<double, long>[] RandomValueWeightArray(int count)
    {
        var arr = new Tuple<double, long>[count];
        for (int i = 0; i < count; i++)
        {
            arr[i] = RandomValueWeight();
        }
        return arr;
    }

    static Tuple<double, long> DefinedValueWeight(double value, long weight)
    {
        return new Tuple<double, long>(value, weight);
    }

    private static double[] WeightedValue(double value, int weight)
    {
        double[] values = new double[weight];
        for (var i = 0; i < values.Length; i++)
        {
            values[i] = value;
        }

        return values;
    }
}
