using Moq;
using StackExchange.Redis;
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
        public async Task TestModulsTransactions()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var transaction = new Transactions(db);
        }
    }
}
