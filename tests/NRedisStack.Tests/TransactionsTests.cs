using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using StackExchange.Redis;
using System.Text.Json;
using Xunit;

namespace NRedisStack.Tests;

public class TransactionTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "TRX_TESTS";

    public TransactionTests(EndpointsFixture endpointsFixture) : base(endpointsFixture)
    {
    }

    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void TestJsonTransaction(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var transaction = new Transaction(db);
        string jsonPerson = JsonSerializer.Serialize(new Person { Name = "Shachar", Age = 23 });
        var setResponse = transaction.Json.SetAsync(key, "$", jsonPerson);
        var getResponse = transaction.Json.GetAsync(key);

        transaction.Execute();

        setResponse.Wait();
        getResponse.Wait();

        Assert.True(setResponse.Result);
        Assert.Equal("{\"Name\":\"Shachar\",\"Age\":23}", getResponse.Result.ToString());
    }

    [SkipIfRedis(Comparison.GreaterThanOrEqual, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    [Obsolete]
    public void TestModulesTransaction(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tran = new Transaction(db);

        _ = tran.Bf.ReserveAsync("bf-key", 0.001, 100);
        _ = tran.Bf.AddAsync("bf-key", "1");
        _ = tran.Cms.InitByDimAsync("cms-key", 100, 5);
        _ = tran.Cf.ReserveAsync("cf-key", 100);
        _ = tran.Graph.QueryAsync("graph-key", "CREATE ({name:'shachar',age:23})");
        _ = tran.Json.SetAsync("json-key", "$", "{}");
        _ = tran.Ft.CreateAsync("ft-key", new FTCreateParams(), new Schema().AddTextField("txt"));
        _ = tran.Tdigest.CreateAsync("tdigest-key", 100);
        _ = tran.Ts.CreateAsync("ts-key", 100);
        _ = tran.TopK.ReserveAsync("topk-key", 100, 100, 100);

        Assert.False(db.KeyExists("bf-key"));
        Assert.False(db.KeyExists("cms-key"));
        Assert.False(db.KeyExists("cf-key"));
        Assert.False(db.KeyExists("graph-key"));
        Assert.False(db.KeyExists("json-key"));
        Assert.Empty(db.FT()._List());
        Assert.False(db.KeyExists("tdigest-key"));
        Assert.False(db.KeyExists("ts-key"));
        Assert.False(db.KeyExists("topk-key"));

        tran.Execute();

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

    [SkipIfRedis(Is.Enterprise)]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    [Obsolete]
    public void TestModulesTransactionWithoutGraph(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        var tran = new Transaction(db);

        _ = tran.Bf.ReserveAsync("bf-key", 0.001, 100);
        _ = tran.Bf.AddAsync("bf-key", "1");
        _ = tran.Cms.InitByDimAsync("cms-key", 100, 5);
        _ = tran.Cf.ReserveAsync("cf-key", 100);
        _ = tran.Json.SetAsync("json-key", "$", "{}");
        _ = tran.Ft.CreateAsync("ft-key", new FTCreateParams(), new Schema().AddTextField("txt"));
        _ = tran.Tdigest.CreateAsync("tdigest-key", 100);
        _ = tran.Ts.CreateAsync("ts-key", 100);
        _ = tran.TopK.ReserveAsync("topk-key", 100, 100, 100);

        Assert.False(db.KeyExists("bf-key"));
        Assert.False(db.KeyExists("cms-key"));
        Assert.False(db.KeyExists("cf-key"));
        Assert.False(db.KeyExists("json-key"));
        Assert.Empty(db.FT()._List());
        Assert.False(db.KeyExists("tdigest-key"));
        Assert.False(db.KeyExists("ts-key"));
        Assert.False(db.KeyExists("topk-key"));

        tran.Execute();

        Assert.True(db.KeyExists("bf-key"));
        Assert.True(db.KeyExists("cms-key"));
        Assert.True(db.KeyExists("cf-key"));
        Assert.True(db.KeyExists("json-key"));
        Assert.True(db.FT()._List().Length == 1);
        Assert.True(db.KeyExists("tdigest-key"));
        Assert.True(db.KeyExists("ts-key"));
        Assert.True(db.KeyExists("topk-key"));

        Assert.True(db.BF().Exists("bf-key", "1"));
        Assert.True(db.CMS().Info("cms-key").Width == 100);
        Assert.True(db.CF().Info("cf-key").Size > 0);
        Assert.False(db.JSON().Get("json-key").IsNull);
        Assert.NotNull(db.FT().Info("ft-key"));
        Assert.NotNull(db.TDIGEST().Info("tdigest-key"));
        Assert.NotNull(db.TS().Info("ts-key"));
        Assert.NotNull(db.TOPK().Info("topk-key"));
    }
}