using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TopK;

public class TopKTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "TOPK_TESTS";

    public TopKTests(EndpointsFixture endpointsFixture) : base(endpointsFixture)
    {
    }


    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void CreateTopKFilter(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var topk = db.TOPK();

        //db.KeyDelete(key, CommandFlags.FireAndForget);
        topk.Reserve(key, 30, 2000, 7, 0.925);

        var res = topk.Add(key, "bb", "cc");
        Assert.True(res![0].IsNull && res[1].IsNull);
        // ReSharper disable once UseCollectionExpression - need to avoid span overload due to TFMs
        Assert.Equal(topk.Query(key, "bb", "gg", "cc"), new[] { true, false, true });
        Assert.False(topk.Query(key, "notExists"));
        // ReSharper disable once UseCollectionExpression - need to avoid span overload due to TFMs
        Assert.Equal(topk.Count(key, "bb", "gg", "cc"), new[] {1L, 0L, 1L});

        var res2 = topk.List(key);
        Assert.Equal("bb", res2[0].ToString());
        Assert.Equal("cc", res2[1].ToString());

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        var del = topk.IncrBy(key, tuple);
        Assert.True(topk.IncrBy(key, tuple)[0].IsNull);

        res2 = topk.List(key);
        Assert.Equal("ff", res2[0].ToString());
        Assert.Equal("bb", res2[1].ToString());
        Assert.Equal("cc", res2[2].ToString());

        var info = topk.Info(key);
        Assert.Equal(0.925, info.Decay);
        Assert.Equal(7, info.Depth);
        Assert.Equal(30, info.K);
        Assert.Equal(2000, info.Width);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task CreateTopKFilterAsync(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var topk = db.TOPK();

        await topk.ReserveAsync(key, 30, 2000, 7, 0.925);

        var res = await topk.AddAsync(key, "bb", "cc");
        Assert.True(res![0].IsNull && res[1].IsNull);

        // ReSharper disable once UseCollectionExpression - need to avoid span overload due to TFMs
        Assert.Equal(await topk.QueryAsync(key, "bb", "gg", "cc"), new[] { true, false, true });
        Assert.False(await topk.QueryAsync(key, "notExists"));
        // ReSharper disable once UseCollectionExpression - need to avoid span overload due to TFMs
        Assert.Equal(await topk.CountAsync(key, "bb", "gg", "cc"), new[] {1L, 0L, 1L});

        var res2 = await topk.ListAsync(key);
        Assert.Equal("bb", res2[0].ToString());
        Assert.Equal("cc", res2[1].ToString());

        var tuple = new Tuple<RedisValue, long>("ff", 10);
        Assert.True((await topk.IncrByAsync(key, tuple))[0].IsNull);

        res2 = await topk.ListAsync(key);
        Assert.Equal("ff", res2[0].ToString());
        Assert.Equal("bb", res2[1].ToString());
        Assert.Equal("cc", res2[2].ToString());

        var info = await topk.InfoAsync(key);
        Assert.Equal(0.925, info.Decay);
        Assert.Equal(7, info.Depth);
        Assert.Equal(30, info.K);
        Assert.Equal(2000, info.Width);
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestModulePrefixs(string endpointId)
    {
        var redis = GetConnection(endpointId);
        IDatabase db1 = redis.GetDatabase();
        IDatabase db2 = redis.GetDatabase();

        var topk1 = db1.TOPK();
        var topk2 = db2.TOPK();

        Assert.NotEqual(topk1.GetHashCode(), topk2.GetHashCode());
    }
}