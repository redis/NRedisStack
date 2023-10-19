using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using System.Text.Json;
using NRedisStack.Search;

namespace NRedisStack.Tests;

public class PipelineTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "PIPELINE_TESTS";
    public PipelineTests(RedisFixture redisFixture) : base(redisFixture) { }

    [SkipIfRedis(Is.OSSCluster, Comparison.GreaterThanOrEqual, "7.1.242")]
    [Obsolete]
    public void TestModulsPipeline()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var pipeline = new Pipeline(db);

        _ = pipeline.Bf.ReserveAsync("bf-key", 0.001, 100);
        _ = pipeline.Bf.AddAsync("bf-key", "1");
        _ = pipeline.Cms.InitByDimAsync("cms-key", 100, 5);
        _ = pipeline.Cf.ReserveAsync("cf-key", 100);
        _ = pipeline.Graph.QueryAsync("graph-key", "CREATE ({name:'shachar',age:23})");
        _ = pipeline.Json.SetAsync("json-key", "$", "{}");
        _ = pipeline.Ft.CreateAsync("ft-key", new FTCreateParams(), new Schema().AddTextField("txt"));
        _ = pipeline.Tdigest.CreateAsync("tdigest-key", 100);
        _ = pipeline.Ts.CreateAsync("ts-key", 100);
        _ = pipeline.TopK.ReserveAsync("topk-key", 100, 100, 100);

        Assert.False(db.KeyExists("bf-key"));
        Assert.False(db.KeyExists("cms-key"));
        Assert.False(db.KeyExists("cf-key"));
        Assert.False(db.KeyExists("graph-key"));
        Assert.False(db.KeyExists("json-key"));
        Assert.Empty(db.FT()._List());
        Assert.False(db.KeyExists("tdigest-key"));
        Assert.False(db.KeyExists("ts-key"));
        Assert.False(db.KeyExists("topk-key"));

        pipeline.Execute();

        Assert.True(db.KeyExists("bf-key"));
        Assert.True(db.KeyExists("cms-key"));
        Assert.True(db.KeyExists("cf-key"));
        Assert.True(db.KeyExists("graph-key"));
        Assert.True(db.KeyExists("json-key"));
        Assert.Single(db.FT()._List());
        Assert.True(db.KeyExists("tdigest-key"));
        Assert.True(db.KeyExists("ts-key"));
        Assert.True(db.KeyExists("topk-key"));

        Assert.True(db.BF().Exists("bf-key", "1"));
        Assert.Equal(100, db.CMS().Info("cms-key").Width);
        Assert.True(db.CF().Info("cf-key").Size > 0);
        Assert.True(db.GRAPH().List().Count > 0);
        Assert.False(db.JSON().Get("json-key").IsNull);
        Assert.NotNull(db.FT().Info("ft-key"));
        Assert.NotNull(db.TDIGEST().Info("tdigest-key"));
        Assert.NotNull(db.TS().Info("ts-key"));
        Assert.NotNull(db.TOPK().Info("topk-key"));
    }

    [SkipIfRedis(Is.OSSCluster)]
    [Obsolete]
    public void TestModulsPipelineWithotGraph()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var pipeline = new Pipeline(db);

        _ = pipeline.Bf.ReserveAsync("bf-key", 0.001, 100);
        _ = pipeline.Bf.AddAsync("bf-key", "1");
        _ = pipeline.Cms.InitByDimAsync("cms-key", 100, 5);
        _ = pipeline.Cf.ReserveAsync("cf-key", 100);
        _ = pipeline.Json.SetAsync("json-key", "$", "{}");
        _ = pipeline.Ft.CreateAsync("ft-key", new FTCreateParams(), new Schema().AddTextField("txt"));
        _ = pipeline.Tdigest.CreateAsync("tdigest-key", 100);
        _ = pipeline.Ts.CreateAsync("ts-key", 100);
        _ = pipeline.TopK.ReserveAsync("topk-key", 100, 100, 100);

        Assert.False(db.KeyExists("bf-key"));
        Assert.False(db.KeyExists("cms-key"));
        Assert.False(db.KeyExists("cf-key"));
        Assert.False(db.KeyExists("json-key"));
        Assert.Empty(db.FT()._List());
        Assert.False(db.KeyExists("tdigest-key"));
        Assert.False(db.KeyExists("ts-key"));
        Assert.False(db.KeyExists("topk-key"));

        pipeline.Execute();

        Assert.True(db.KeyExists("bf-key"));
        Assert.True(db.KeyExists("cms-key"));
        Assert.True(db.KeyExists("cf-key"));
        Assert.True(db.KeyExists("json-key"));
        Assert.Single(db.FT()._List());
        Assert.True(db.KeyExists("tdigest-key"));
        Assert.True(db.KeyExists("ts-key"));
        Assert.True(db.KeyExists("topk-key"));

        Assert.True(db.BF().Exists("bf-key", "1"));
        Assert.Equal(100, db.CMS().Info("cms-key").Width);
        Assert.True(db.CF().Info("cf-key").Size > 0);
        Assert.False(db.JSON().Get("json-key").IsNull);
        Assert.NotNull(db.FT().Info("ft-key"));
        Assert.NotNull(db.TDIGEST().Info("tdigest-key"));
        Assert.NotNull(db.TS().Info("ts-key"));
        Assert.NotNull(db.TOPK().Info("topk-key"));
    }

    [SkipIfRedis(Is.OSSCluster)]
    public void TestBloomPipeline()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var pipeline = new Pipeline(db);

        _ = pipeline.Bf.ReserveAsync(key, 0.001, 100);
        for (int i = 0; i < 1000; i++)
        {
            _ = pipeline.Bf.AddAsync(key, i.ToString());
        }

        for (int i = 0; i < 100; i++)
        {
            Assert.False(db.BF().Exists(key, i.ToString()));
        }

        pipeline.Execute();

        for (int i = 0; i < 1000; i++)
        {
            Assert.True(db.BF().Exists(key, i.ToString()));
        }
    }

    [Fact]
    public void TestJsonPipeline()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        var pipeline = new Pipeline(db);
        pipeline.Db.ExecuteAsync("FLUSHALL");

        string jsonPerson = JsonSerializer.Serialize(new Person { Name = "Shachar", Age = 23 });
        _ = pipeline.Json.SetAsync("key", "$", jsonPerson);
        var setResponse = pipeline.Json.SetAsync("key", "$", jsonPerson);
        var getResponse = pipeline.Json.GetAsync("key");

        pipeline.Execute();

        setResponse.Wait();
        getResponse.Wait();

        Assert.True(setResponse.Result);
        Assert.Equal("{\"Name\":\"Shachar\",\"Age\":23}", getResponse.Result.ToString());
    }
}