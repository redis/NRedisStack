using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestGet : AbstractNRedisStackTest, IDisposable
    {

        private readonly string key = "GET_TESTS";

        public TestGet(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public void TestGetNotExists()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var ex = Assert.Throws<RedisServerException>(() => ts.Get(key));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestEmptyGet()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create(key);
            Assert.Null(ts.Get(key));
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.UtcNow;
            TimeSeriesTuple expected = new TimeSeriesTuple(now, 1.1);
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create(key);
            ts.Add(key, now, 1.1);
            TimeSeriesTuple actual = ts.Get(key);
            Assert.Equal(expected, actual);
        }
    }
}
