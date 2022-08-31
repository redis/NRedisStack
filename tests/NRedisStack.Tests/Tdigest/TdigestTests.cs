using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;

namespace NRedisStack.Tests.Tdigest;

public class TdigestTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "TDIGEST_TESTS";
    public TdigestTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    private void AssertMergedUnmergedNodes(TdigestCommands tdigest, string key, int mergedNodes, int unmergedNodes)
    {
        var info = tdigest.Info(key);
        Assert.Equal((long)mergedNodes, info.MergedNodes);
        Assert.Equal((long)unmergedNodes, info.UnmergedNodes);
    }

    private void AssertTotalWeight(TdigestCommands tdigest, string key, double totalWeight)
    {
        var info = tdigest.Info(key);
        Assert.Equal(totalWeight, info.MergedWeight + info.UnmergedWeight);
        //Assert.Equal(totalWeight, 0.01);
    }

    [Fact]
    public void TestCreateSimple()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        Assert.True(tdigest.Create(key));

        var info = tdigest.Info(key);
        Assert.Equal(100, info.Compression);
    }

    [Fact]
    public async Task TestCreateSimpleAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        Assert.True(await tdigest.CreateAsync(key));

        var info = await tdigest.InfoAsync(key);
        Assert.Equal(100, info.Compression);
    }

    [Fact]
    public void TestCreateAndInfo()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        for (int i = 100; i < 1000; i += 100)
        {
            string myKey = "td-" + i;
            Assert.True(tdigest.Create(myKey, i));

            var info = tdigest.Info(myKey);
            Assert.Equal(i, info.Compression);
        }
    }

    [Fact]
    public async Task TestCreateAndInfoAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        for (int i = 100; i < 1000; i += 100)
        {
            string myKey = "td-" + i;
            Assert.True(await tdigest.CreateAsync(myKey, i));

            var info = await tdigest.InfoAsync(myKey);
            Assert.Equal(i, info.Compression);
        }
    }

    [Fact]
    public void TestReset()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create("reset", 100);
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        // on empty
        Assert.True(tdigest.Reset("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        tdigest.Add("reset", RandomValueWeight(), RandomValueWeight(), RandomValueWeight());
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 3);

        Assert.True(tdigest.Reset("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);
    }

    [Fact]
    public async Task TestResetAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("reset", 100);
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        // on empty
        Assert.True(await tdigest.ResetAsync("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);

        await tdigest.AddAsync("reset", RandomValueWeight(), RandomValueWeight(), RandomValueWeight());
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 3);

        Assert.True(await tdigest.ResetAsync("reset"));
        AssertMergedUnmergedNodes(tdigest, "reset", 0, 0);
    }

    [Fact]
    public void TestAdd()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create("tdadd", 100);

        Assert.True(tdigest.Add("tdadd", RandomValueWeight()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 1);

        Assert.True(tdigest.Add("tdadd", RandomValueWeight(), RandomValueWeight(), RandomValueWeight(), RandomValueWeight()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 5);
    }

    [Fact]
    public async Task TestAddAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("tdadd", 100);

        Assert.True(await tdigest.AddAsync("tdadd", RandomValueWeight()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 1);

        Assert.True(await tdigest.AddAsync("tdadd", RandomValueWeight(), RandomValueWeight(), RandomValueWeight(), RandomValueWeight()));
        AssertMergedUnmergedNodes(tdigest, "tdadd", 0, 5);
    }


    [Fact]
    public void TestMerge()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create("td2", 100);
        tdigest.Create("td4m", 100);

        Assert.True(tdigest.Merge("td2", "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 0, 0);

        tdigest.Add("td2", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        tdigest.Add("td4m", DefinedValueWeight(1, 100), DefinedValueWeight(1, 100));

        Assert.True(tdigest.Merge("td2", "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 3, 2);
    }


    [Fact]
    public async Task TestMergeAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("td2", 100);
        await tdigest.CreateAsync("td4m", 100);

        Assert.True(await tdigest.MergeAsync("td2", "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 0, 0);

        await tdigest.AddAsync("td2", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        await tdigest.AddAsync("td4m", DefinedValueWeight(1, 100), DefinedValueWeight(1, 100));

        Assert.True(await tdigest.MergeAsync("td2", "td4m"));
        AssertMergedUnmergedNodes(tdigest, "td2", 3, 2);
    }

    [Fact]
    public void TestMergeStore()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create("from1", 100);
        tdigest.Create("from2", 200);

        tdigest.Add("from1", 1, 1);
        tdigest.Add("from2", 1, 10);

        Assert.True(tdigest.MergeStore("to", 2, 100, "from1", "from2"));
        AssertTotalWeight(tdigest, "to", 11d);

        Assert.True(tdigest.MergeStore("to50", 2, 50, "from1", "from2"));
        Assert.Equal(50, tdigest.Info("to50").Compression);
    }

    [Fact]
    public async Task TestMergeStoreAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("from1", 100);
        await tdigest.CreateAsync("from2", 200);

        await tdigest.AddAsync("from1", 1, 1);
        await tdigest.AddAsync("from2", 1, 10);

        Assert.True(await tdigest.MergeStoreAsync("to", 2, 100, "from1", "from2"));
        AssertTotalWeight(tdigest, "to", 11d);

        Assert.True(await tdigest.MergeStoreAsync("to50", 2, 50, "from1", "from2"));
        Assert.Equal(50, (await tdigest.InfoAsync("to50")).Compression);
    }

    [Fact]
    public void TestCDF()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create("tdcdf", 100);
        Assert.Equal(double.NaN, tdigest.CDF("tdcdf", 50));

        tdigest.Add("tdcdf", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        tdigest.Add("tdcdf", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(0.6, tdigest.CDF("tdcdf", 50));
    }

    [Fact]
    public async Task TestCDFAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync("tdcdf", 100);
        Assert.Equal(double.NaN, await tdigest.CDFAsync("tdcdf", 50));

        await tdigest.AddAsync("tdcdf", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        await tdigest.AddAsync("tdcdf", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(0.6, await tdigest.CDFAsync("tdcdf", 50));
    }

    [Fact]
    public void TestQuantile()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create("tdqnt", 100);
        var resDelete = tdigest.Quantile("tdqnt", 0.5);
        Assert.Equal(new double[] { double.NaN }, tdigest.Quantile("tdqnt", 0.5));

        tdigest.Add("tdqnt", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        tdigest.Add("tdqnt", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(new double[] { 1 }, tdigest.Quantile("tdqnt", 0.5));
    }

    [Fact]
    public async Task TestQuantileAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create("tdqnt", 100);
        var resDelete = await tdigest.QuantileAsync("tdqnt", 0.5);
        Assert.Equal(new double[] { double.NaN }, await tdigest.QuantileAsync("tdqnt", 0.5));

        await tdigest.AddAsync("tdqnt", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        await tdigest.AddAsync("tdqnt", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(new double[] { 1 }, await tdigest.QuantileAsync("tdqnt", 0.5));
    }

    [Fact]
    public void TestMinAndMax()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create(key, 100);
        Assert.Equal(double.MaxValue, tdigest.Min(key));
        Assert.Equal(-double.MaxValue, tdigest.Max(key));

        tdigest.Add(key, DefinedValueWeight(2, 1));
        tdigest.Add(key, DefinedValueWeight(5, 1));
        Assert.Equal(2d, tdigest.Min(key));
        Assert.Equal(5d, tdigest.Max(key));
    }

    [Fact]
    public async Task TestMinAndMaxAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync(key, 100);
        Assert.Equal(double.MaxValue, await tdigest.MinAsync(key));
        Assert.Equal(-double.MaxValue, await tdigest.MaxAsync(key));

        await tdigest.AddAsync(key, DefinedValueWeight(2, 1));
        await tdigest.AddAsync(key, DefinedValueWeight(5, 1));
        Assert.Equal(2d, await tdigest.MinAsync(key));
        Assert.Equal(5d, await tdigest.MaxAsync(key));
    }

    [Fact]
    public void TestTrimmedMean()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        tdigest.Create(key, 500);

        for (int i = 0; i < 20; i++)
        {
            tdigest.Add(key, new Tuple<double, double>(i, 1));
        }

        Assert.Equal(9.5, tdigest.TrimmedMean(key, 0.1, 0.9));
        Assert.Equal(9.5, tdigest.TrimmedMean(key, 0.0, 1.0));
        Assert.Equal(4.5, tdigest.TrimmedMean(key, 0.0, 0.5));
        Assert.Equal(14.5, tdigest.TrimmedMean(key, 0.5, 1.0));
    }

    [Fact]
    public async Task TestTrimmedMeanAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var tdigest = db.TDIGEST();

        await tdigest.CreateAsync(key, 500);

        for (int i = 0; i < 20; i++)
        {
            await tdigest.AddAsync(key, new Tuple<double, double>(i, 1));
        }

        Assert.Equal(9.5, await tdigest.TrimmedMeanAsync(key, 0.1, 0.9));
        Assert.Equal(9.5, await tdigest.TrimmedMeanAsync(key, 0.0, 1.0));
        Assert.Equal(4.5, await tdigest.TrimmedMeanAsync(key, 0.0, 0.5));
        Assert.Equal(14.5, await tdigest.TrimmedMeanAsync(key, 0.5, 1.0));
    }


    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var tdigest1 = db1.TDIGEST();
        var tdigest2 = db2.TDIGEST();

        Assert.NotEqual(tdigest1.GetHashCode(), tdigest2.GetHashCode());
    }

    [Fact]
    public void TestModulePrefixs1()
    {
        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var tdigest = db.TDIGEST();
            // ...
            conn.Dispose();
        }

        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var tdigest = db.TDIGEST();
            // ...
            conn.Dispose();
        }

    }

    static Tuple<double, double> RandomValueWeight()
    {
        Random random = new Random();

        return new Tuple<double, double>(random.NextDouble() * 10000, random.NextDouble() * 500 + 1);
    }

    static Tuple<double, double> DefinedValueWeight(double value, double weight)
    {
        return new Tuple<double, double>(value, weight);
    }
}
