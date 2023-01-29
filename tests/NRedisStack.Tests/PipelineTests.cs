using Xunit;
using StackExchange.Redis;
using NRedisStack.RedisStackCommands;
using Moq;
using System.Text.Json;
using NRedisStack.Search.FT.CREATE;
using NRedisStack.Search;

namespace NRedisStack.Tests.Bloom;

public class PipelineTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "PIPELINE_TESTS";
    public PipelineTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    // [Fact]
    // public async Task TestPipeline()
    // {
    //     IDatabase db = redisFixture.Redis.GetDatabase();
    //     db.Execute("FLUSHALL");
    //     var pipeline = new Pipeline(db);

    //     pipeline.Db.StringSetAsync("a", "a1");
    //     pipeline.Db.StringGetAsync("a");
    //     pipeline.Db.SortedSetAddAsync("z", new SortedSetEntry[] { new SortedSetEntry("z1", 1) });
    //     pipeline.Db.SortedSetAddAsync("z", new SortedSetEntry[] { new SortedSetEntry("z2", 4) });
    //     pipeline.Db.SortedSetIncrementAsync()
    // }


    [Fact]
    public async Task TestModulsPipeline()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var pipeline = new Pipeline(db);

        pipeline.Bf.ReserveAsync("bf-key", 0.001, 100);
        pipeline.Bf.AddAsync("bf-key", "1");
        pipeline.Cms.InitByDimAsync("cms-key", 100, 5);
        pipeline.Cf.ReserveAsync("cf-key", 100);
        pipeline.Graph.QueryAsync("graph-key", "CREATE ({name:'shachar',age:23})");
        pipeline.Json.SetAsync("json-key", "$", "{}");
        pipeline.Ft.CreateAsync("ft-key", new FTCreateParams(), new Schema().AddTextField("txt"));
        pipeline.Tdigest.CreateAsync("tdigest-key", 100);
        pipeline.Ts.CreateAsync("ts-key", 100);
        pipeline.TopK.ReserveAsync("topk-key", 100, 100, 100);

        Assert.False(db.KeyExists("bf-key"));
        Assert.False(db.KeyExists("cms-key"));
        Assert.False(db.KeyExists("cf-key"));
        Assert.False(db.KeyExists("graph-key"));
        Assert.False(db.KeyExists("json-key"));
        Assert.Equal(0, db.FT()._List().Length);
        Assert.False(db.KeyExists("tdigest-key"));
        Assert.False(db.KeyExists("ts-key"));
        Assert.False(db.KeyExists("topk-key"));

        pipeline.Execute();

        Assert.True(db.KeyExists("bf-key"));
        Assert.True(db.KeyExists("cms-key"));
        Assert.True(db.KeyExists("cf-key"));
        Assert.True(db.KeyExists("graph-key"));
        Assert.True(db.KeyExists("json-key"));
        Assert.True(db.FT()._List().Length == 1);
        Assert.True(db.KeyExists("tdigest-key"));
        Assert.True(db.KeyExists("ts-key"));
        Assert.True(db.KeyExists("topk-key"));

        Assert.True(db.BF().Exists("bf-key", "1"));
        Assert.True(db.CMS().Info("cms-key").Width == 100);
        Assert.True(db.CF().Info("cf-key").Size > 0);
        Assert.True(db.GRAPH().List().Count > 0);
        Assert.False(db.JSON().Get("json-key").IsNull);
        Assert.NotNull(db.FT().Info("ft-key"));
        Assert.NotNull(db.TDIGEST().Info("tdigest-key"));
        Assert.NotNull(db.TS().Info("ts-key"));
        Assert.NotNull(db.TOPK().Info("topk-key"));
    }

    [Fact]
    public async Task TestBloomPipeline()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");
        var pipeline = new Pipeline(db);

        pipeline.Bf.ReserveAsync(key, 0.001, 100);
        for(int i = 0; i < 1000; i++)
        {
            pipeline.Bf.AddAsync(key, i.ToString());
        }

        for(int i = 0; i < 100; i++)
        {
            Assert.False(db.BF().Exists(key, i.ToString()));
        }

        pipeline.Execute();

        for(int i = 0; i < 1000; i++)
        {
            Assert.True(db.BF().Exists(key, i.ToString()));
        }
    }

    [Fact]
    public async Task TestJsonPipeline()
    {
        var pipeline = new Pipeline(ConnectionMultiplexer.Connect("localhost"));
        pipeline.Db.ExecuteAsync("FLUSHALL");

        string jsonPerson = JsonSerializer.Serialize(new Person { Name = "Shachar", Age = 23 });
        var setResponse = pipeline.Json.SetAsync("key", "$", jsonPerson);
        var getResponse = pipeline.Json.GetAsync("key");

        pipeline.Execute();

        Assert.Equal("True", setResponse.Result.ToString());
        Assert.Equal("{\"Name\":\"Shachar\",\"Age\":23}", getResponse.Result.ToString());
    }
}