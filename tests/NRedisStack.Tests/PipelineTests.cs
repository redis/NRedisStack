using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using System.Text.Json;
using NRedisStack.Search;

namespace NRedisStack.Tests;

public class PipelineTests : AbstractNRedisStackTest, IDisposable
{
    public PipelineTests(EndpointsFixture endpointsFixture) : base(endpointsFixture)
    {
    }

    private const string key = "PIPELINE_TESTS";

    [SkipIfRedisTheory(Comparison.GreaterThanOrEqual, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    [Obsolete]
    public void TestModulesPipeline(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var pipeline = new Pipeline(db);

        _ = pipeline.Bf.ReserveAsync("bf-key", 0.001, 100);
        _ = pipeline.Bf.AddAsync("bf-key", "1");
        _ = pipeline.Cms.InitByDimAsync("cms-key", 100, 5);
        _ = pipeline.Cf.ReserveAsync("cf-key", 100);
        _ = pipeline.Json.SetAsync("json-key", "$", "{}");
        _ = pipeline.Ft.CreateAsync("ft-key", new(), new Schema().AddTextField("txt"));
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

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    [Obsolete]
    public void TestModulesPipelineWithoutGraph(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var pipeline = new Pipeline(db);

        _ = pipeline.Bf.ReserveAsync("bf-key", 0.001, 100);
        _ = pipeline.Bf.AddAsync("bf-key", "1");
        _ = pipeline.Cms.InitByDimAsync("cms-key", 100, 5);
        _ = pipeline.Cf.ReserveAsync("cf-key", 100);
        _ = pipeline.Json.SetAsync("json-key", "$", "{}");
        _ = pipeline.Ft.CreateAsync("ft-key", new(), new Schema().AddTextField("txt"));
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

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void TestBloomPipeline(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
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

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestJsonPipeline(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var pipeline = new Pipeline(db);

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

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    [Obsolete]
    public async Task Issue401_TestPipelineAsInitialCommand(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);

        Auxiliary.ResetInfoDefaults(); // demonstrate first connection
        var pipeline = new Pipeline(db);

        var setTask = pipeline.Json.SetAsync("json-key", "$", "{}");
        _ = pipeline.Db.KeyExpireAsync(key, TimeSpan.FromSeconds(10));

        pipeline.Execute();

        Assert.True(await setTask);
    }
}