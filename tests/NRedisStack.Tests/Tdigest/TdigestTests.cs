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

    private void AssertMergedUnmergedNodes(IDatabase db, string key, int mergedNodes, int unmergedNodes)
    {
        var info = db.TDIGEST().Info(key);
        Assert.Equal((long)mergedNodes, info.MergedNodes);
        Assert.Equal((long)unmergedNodes, info.UnmergedNodes);
    }

    private void AssertTotalWeight(IDatabase db, string key, double totalWeight)
    {
        var info = db.TDIGEST().Info(key);
        Assert.Equal(totalWeight, info.MergedWeight + info.UnmergedWeight);
        //Assert.Equal(totalWeight, 0.01);
    }

    [Fact]
    public void TestCreateSimple()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(db.TDIGEST().Create(key));

        var info = db.TDIGEST().Info(key);
        Assert.Equal(100, info.Compression);
    }

    [Fact]
    public async Task TestCreateSimpleAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(await db.TDIGEST().CreateAsync(key));

        var info = await db.TDIGEST().InfoAsync(key);
        Assert.Equal(100, info.Compression);
    }

    [Fact]
    public void TestCreateAndInfo()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        for (int i = 100; i < 1000; i += 100)
        {
            string myKey = "td-" + i;
            Assert.True(db.TDIGEST().Create(myKey, i));

            var info = db.TDIGEST().Info(myKey);
            Assert.Equal(i, info.Compression);
        }
    }

    [Fact]
    public async Task TestCreateAndInfoAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        for (int i = 100; i < 1000; i += 100)
        {
            string myKey = "td-" + i;
            Assert.True(await db.TDIGEST().CreateAsync(myKey, i));

            var info = await db.TDIGEST().InfoAsync(myKey);
            Assert.Equal(i, info.Compression);
        }
    }

    //TODO: start async methods from here:
    [Fact]
    public void TestReset()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create("reset", 100);
        AssertMergedUnmergedNodes(db, "reset", 0, 0);

        // on empty
        Assert.True(db.TDIGEST().Reset("reset"));
        AssertMergedUnmergedNodes(db, "reset", 0, 0);

        db.TDIGEST().Add("reset", RandomValueWeight(), RandomValueWeight(), RandomValueWeight());
        AssertMergedUnmergedNodes(db, "reset", 0, 3);

        Assert.True(db.TDIGEST().Reset("reset"));
        AssertMergedUnmergedNodes(db, "reset", 0, 0);
    }

    [Fact]
    public async Task TestResetAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.TDIGEST().CreateAsync("reset", 100);
        AssertMergedUnmergedNodes(db, "reset", 0, 0);

        // on empty
        Assert.True(await db.TDIGEST().ResetAsync("reset"));
        AssertMergedUnmergedNodes(db, "reset", 0, 0);

        await db.TDIGEST().AddAsync("reset", RandomValueWeight(), RandomValueWeight(), RandomValueWeight());
        AssertMergedUnmergedNodes(db, "reset", 0, 3);

        Assert.True(await db.TDIGEST().ResetAsync("reset"));
        AssertMergedUnmergedNodes(db, "reset", 0, 0);
    }

    [Fact]
    public void TestAdd()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create("tdadd", 100);

        Assert.True(db.TDIGEST().Add("tdadd", RandomValueWeight()));
        AssertMergedUnmergedNodes(db, "tdadd", 0, 1);

        Assert.True(db.TDIGEST().Add("tdadd", RandomValueWeight(), RandomValueWeight(), RandomValueWeight(), RandomValueWeight()));
        AssertMergedUnmergedNodes(db, "tdadd", 0, 5);
    }

    [Fact]
    public async Task TestAddAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.TDIGEST().CreateAsync("tdadd", 100);

        Assert.True(await db.TDIGEST().AddAsync("tdadd", RandomValueWeight()));
        AssertMergedUnmergedNodes(db, "tdadd", 0, 1);

        Assert.True(await db.TDIGEST().AddAsync("tdadd", RandomValueWeight(), RandomValueWeight(), RandomValueWeight(), RandomValueWeight()));
        AssertMergedUnmergedNodes(db, "tdadd", 0, 5);
    }


    [Fact]
    public void TestMerge()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create("td2", 100);
        db.TDIGEST().Create("td4m", 100);

        Assert.True(db.TDIGEST().Merge("td2", "td4m"));
        AssertMergedUnmergedNodes(db, "td2", 0, 0);

        db.TDIGEST().Add("td2", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        db.TDIGEST().Add("td4m", DefinedValueWeight(1, 100), DefinedValueWeight(1, 100));

        Assert.True(db.TDIGEST().Merge("td2", "td4m"));
        AssertMergedUnmergedNodes(db, "td2", 3, 2);
    }


    [Fact]
    public async Task TestMergeAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.TDIGEST().CreateAsync("td2", 100);
        await db.TDIGEST().CreateAsync("td4m", 100);

        Assert.True(await db.TDIGEST().MergeAsync("td2", "td4m"));
        AssertMergedUnmergedNodes(db, "td2", 0, 0);

        await db.TDIGEST().AddAsync("td2", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        await db.TDIGEST().AddAsync("td4m", DefinedValueWeight(1, 100), DefinedValueWeight(1, 100));

        Assert.True(await db.TDIGEST().MergeAsync("td2", "td4m"));
        AssertMergedUnmergedNodes(db, "td2", 3, 2);
    }

    [Fact]
    public void TestMergeStore()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create("from1", 100);
        db.TDIGEST().Create("from2", 200);

        db.TDIGEST().Add("from1", 1, 1);
        db.TDIGEST().Add("from2", 1, 10);

        Assert.True(db.TDIGEST().MergeStore("to", 2, 100, "from1", "from2"));
        AssertTotalWeight(db, "to", 11d);

        Assert.True(db.TDIGEST().MergeStore("to50", 2, 50, "from1", "from2"));
        Assert.Equal(50, db.TDIGEST().Info("to50").Compression);
    }

    [Fact]
    public async Task TestMergeStoreAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.TDIGEST().CreateAsync("from1", 100);
        await db.TDIGEST().CreateAsync("from2", 200);

        await db.TDIGEST().AddAsync("from1", 1, 1);
        await db.TDIGEST().AddAsync("from2", 1, 10);

        Assert.True(await db.TDIGEST().MergeStoreAsync("to", 2, 100, "from1", "from2"));
        AssertTotalWeight(db, "to", 11d);

        Assert.True(await db.TDIGEST().MergeStoreAsync("to50", 2, 50, "from1", "from2"));
        Assert.Equal(50, (await db.TDIGEST().InfoAsync("to50")).Compression);
    }

    [Fact]
    public void TestCDF()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create("tdcdf", 100);
        Assert.Equal(double.NaN, db.TDIGEST().CDF("tdcdf", 50));

        db.TDIGEST().Add("tdcdf", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        db.TDIGEST().Add("tdcdf", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(0.6, db.TDIGEST().CDF("tdcdf", 50));
    }

    [Fact]
    public async Task TestCDFAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.TDIGEST().CreateAsync("tdcdf", 100);
        Assert.Equal(double.NaN, await db.TDIGEST().CDFAsync("tdcdf", 50));

        await db.TDIGEST().AddAsync("tdcdf", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        await db.TDIGEST().AddAsync("tdcdf", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(0.6, await db.TDIGEST().CDFAsync("tdcdf", 50));
    }

    [Fact]
    public void TestQuantile()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create("tdqnt", 100);
        var resDelete = db.TDIGEST().Quantile("tdqnt", 0.5);
        Assert.Equal(new double[] { double.NaN }, db.TDIGEST().Quantile("tdqnt", 0.5));

        db.TDIGEST().Add("tdqnt", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        db.TDIGEST().Add("tdqnt", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(new double[] { 1 }, db.TDIGEST().Quantile("tdqnt", 0.5));
    }

    [Fact]
    public async Task TestQuantileAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create("tdqnt", 100);
        var resDelete = await db.TDIGEST().QuantileAsync("tdqnt", 0.5);
        Assert.Equal(new double[] { double.NaN }, await db.TDIGEST().QuantileAsync("tdqnt", 0.5));

        await db.TDIGEST().AddAsync("tdqnt", DefinedValueWeight(1, 1), DefinedValueWeight(1, 1), DefinedValueWeight(1, 1));
        await db.TDIGEST().AddAsync("tdqnt", DefinedValueWeight(100, 1), DefinedValueWeight(100, 1));
        Assert.Equal(new double[] { 1 }, await db.TDIGEST().QuantileAsync("tdqnt", 0.5));
    }

    [Fact]
    public void TestMinAndMax()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create(key, 100);
        Assert.Equal(double.MaxValue, db.TDIGEST().Min(key));
        Assert.Equal(-double.MaxValue, db.TDIGEST().Max(key));

        db.TDIGEST().Add(key, DefinedValueWeight(2, 1));
        db.TDIGEST().Add(key, DefinedValueWeight(5, 1));
        Assert.Equal(2d, db.TDIGEST().Min(key));
        Assert.Equal(5d, db.TDIGEST().Max(key));
    }

    [Fact]
    public async Task TestMinAndMaxAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.TDIGEST().CreateAsync(key, 100);
        Assert.Equal(double.MaxValue, await db.TDIGEST().MinAsync(key));
        Assert.Equal(-double.MaxValue, await db.TDIGEST().MaxAsync(key));

        await db.TDIGEST().AddAsync(key, DefinedValueWeight(2, 1));
        await db.TDIGEST().AddAsync(key, DefinedValueWeight(5, 1));
        Assert.Equal(2d, await db.TDIGEST().MinAsync(key));
        Assert.Equal(5d, await db.TDIGEST().MaxAsync(key));
    }

    [Fact]
    public void TestTrimmedMean()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.TDIGEST().Create(key, 500);

        for (int i = 0; i < 20; i++)
        {
            db.TDIGEST().Add(key, new Tuple<double, double>(i, 1));
        }

        Assert.Equal(9.5, db.TDIGEST().TrimmedMean(key, 0.1, 0.9));
        Assert.Equal(9.5, db.TDIGEST().TrimmedMean(key, 0.0, 1.0));
        Assert.Equal(4.5, db.TDIGEST().TrimmedMean(key, 0.0, 0.5));
        Assert.Equal(14.5, db.TDIGEST().TrimmedMean(key, 0.5, 1.0));
    }

    [Fact]
    public async Task TestTrimmedMeanAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.TDIGEST().CreateAsync(key, 500);

        for (int i = 0; i < 20; i++)
        {
            await db.TDIGEST().AddAsync(key, new Tuple<double, double>(i, 1));
        }

        Assert.Equal(9.5, await db.TDIGEST().TrimmedMeanAsync(key, 0.1, 0.9));
        Assert.Equal(9.5, await db.TDIGEST().TrimmedMeanAsync(key, 0.0, 1.0));
        Assert.Equal(4.5, await db.TDIGEST().TrimmedMeanAsync(key, 0.0, 0.5));
        Assert.Equal(14.5, await db.TDIGEST().TrimmedMeanAsync(key, 0.5, 1.0));
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
