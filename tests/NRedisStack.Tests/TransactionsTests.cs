using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using StackExchange.Redis;
using System.Text.Json;
using Xunit;

namespace NRedisStack.Tests
{
    public class TransactionTests : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "TRX_TESTS";
        public TransactionTests(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestJsonTransaction()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var transaction = new Transaction(db);
            string jsonPerson = JsonSerializer.Serialize(new Person { Name = "Shachar", Age = 23 });
            var setResponse = transaction.Json.SetAsync(key, "$", jsonPerson);
            var getResponse = transaction.Json.GetAsync(key);

            transaction.Execute();

            Assert.Equal("True", setResponse.Result.ToString());
            Assert.Equal("{\"Name\":\"Shachar\",\"Age\":23}", getResponse.Result.ToString());
        }

        [SkipIfRedis(Comparison.GreaterThanOrEqual, "7.1.242")]
        public async Task TestModulsTransaction()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tran = new Transaction(db);

            tran.Bf.ReserveAsync("bf-key", 0.001, 100);
            tran.Bf.AddAsync("bf-key", "1");
            tran.Cms.InitByDimAsync("cms-key", 100, 5);
            tran.Cf.ReserveAsync("cf-key", 100);
            tran.Graph.QueryAsync("graph-key", "CREATE ({name:'shachar',age:23})");
            tran.Json.SetAsync("json-key", "$", "{}");
            tran.Ft.CreateAsync("ft-key", new FTCreateParams(), new Schema().AddTextField("txt"));
            tran.Tdigest.CreateAsync("tdigest-key", 100);
            tran.Ts.CreateAsync("ts-key", 100);
            tran.TopK.ReserveAsync("topk-key", 100, 100, 100);

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

        [SkipIfRedis(Is.OSSCluster)]
        public async Task TestModulsTransactionWithoutGraph()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tran = new Transaction(db);

            tran.Bf.ReserveAsync("bf-key", 0.001, 100);
            tran.Bf.AddAsync("bf-key", "1");
            tran.Cms.InitByDimAsync("cms-key", 100, 5);
            tran.Cf.ReserveAsync("cf-key", 100);
            tran.Json.SetAsync("json-key", "$", "{}");
            tran.Ft.CreateAsync("ft-key", new FTCreateParams(), new Schema().AddTextField("txt"));
            tran.Tdigest.CreateAsync("tdigest-key", 100);
            tran.Ts.CreateAsync("ts-key", 100);
            tran.TopK.ReserveAsync("topk-key", 100, 100, 100);

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
}
