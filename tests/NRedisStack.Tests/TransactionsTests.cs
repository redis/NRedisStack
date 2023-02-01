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
    }
}
