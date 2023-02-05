using Moq;
using StackExchange.Redis;
using System.Text.Json;
using Xunit;

namespace NRedisStack.Tests
{
    public class TransactionsTests : AbstractNRedisStackTest, IDisposable
    {
        Mock<IDatabase> _mock = new Mock<IDatabase>();
        private readonly string key = "TRX_TESTS";
        public TransactionsTests(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public async Task TestJsonTransactions()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var transaction = new Transactions(db);
            string jsonPerson = JsonSerializer.Serialize(new Person { Name = "Shachar", Age = 23 });
            var setResponse = transaction.Json.SetAsync(key, "$", jsonPerson);
            var getResponse = transaction.Json.GetAsync(key);

            transaction.Execute();

            Assert.Equal("True", setResponse.Result.ToString());
            Assert.Equal("{\"Name\":\"Shachar\",\"Age\":23}", getResponse.Result.ToString());
        }

        /*        [Fact]
                public async Task TestCountMinSketchWithTopKInTransaction()
                {
                    IDatabase db = redisFixture.Redis.GetDatabase();
                    db.Execute("FLUSHALL");
                    var transaction = new Transactions(db);

                    *//*transaction.TopK.ReserveAsync("aaa", 30, 2000, 7, 0.925);
                    transaction.TopK.AddAsync("aaa", "foo", "bar");
                    var results = transaction.TopK.ListAsync("aaa", true);


                    transaction.Cms.InitByDimAsync("A", 1000, 5);
                    var aValues = new Tuple<RedisValue, long>[3];
                    aValues[0] = new Tuple<RedisValue, long>("foo", 5);
                    aValues[1] = new Tuple<RedisValue, long>("bar", 3);
                    aValues[2] = new Tuple<RedisValue, long>("baz", 9);
                    transaction.Cms.IncrByAsync("A", aValues);
                    transaction.Cms.QueryAsync("A", new RedisValue[] { "foo", "bar", "baz" });*//*


                    transaction.Cms.InitByDimAsync("A", 1000, 5);
                    transaction.Cms.InitByDimAsync("B", 1000, 5);
                    transaction.Cms.InitByDimAsync("C", 1000, 5);

                    var aValues = new Tuple<RedisValue, long>[3];
                    aValues[0] = new Tuple<RedisValue, long>("foo", 5);
                    aValues[1] = new Tuple<RedisValue, long>("bar", 3);
                    aValues[2] = new Tuple<RedisValue, long>("baz", 9);

                    transaction.Cms.IncrByAsync("A", aValues);

                    var bValues = new Tuple<RedisValue, long>[3];
                    bValues[0] = new Tuple<RedisValue, long>("foo", 2);
                    bValues[1] = new Tuple<RedisValue, long>("bar", 3);
                    bValues[2] = new Tuple<RedisValue, long>("baz", 1);

                    transaction.Cms.IncrByAsync("B", bValues);

                    var q1 = transaction.Cms.QueryAsync("A", new RedisValue[] { "foo", "bar", "baz" });
                    Assert.Equal(new long[] { 5L, 3L, 9L }, q1);

                    var q2 = transaction.Cms.QueryAsync("B", new RedisValue[] { "foo", "bar", "baz" });
                    Assert.Equal(new long[] { 2L, 3L, 1L }, q2);

                    transaction.Cms.MergeAsync("C", 2, new RedisValue[] { "A", "B" });

                    var q3 = transaction.Cms.QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
                    Assert.Equal(new long[] { 7L, 6L, 10L }, q3);

                    transaction.Cms.MergeAsync("C", 2, new RedisValue[] { "A", "B" }, new long[] { 1, 2 });

                    var q4 = transaction.Cms.QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
                    Assert.Equal(new long[] { 9L, 9L, 11L }, q4);

                    transaction.Cms.MergeAsync("C", 2, new RedisValue[] { "A", "B" }, new long[] { 2, 3 });


                    var q5 = transaction.Cms.QueryAsync("C", new RedisValue[] { "foo", "bar", "baz" });
                    Assert.Equal(new long[] { 16L, 15L, 21L }, (IEnumerable<long>)q5);
                }*/
    }
}
