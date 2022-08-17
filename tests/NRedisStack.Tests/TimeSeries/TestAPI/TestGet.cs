using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestGet : AbstractTimeSeriesTest, IDisposable
    {

        private readonly string key = "GET_TESTS";

        public TestGet(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestGetNotExists()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ex = Assert.Throws<RedisServerException>(() => db.TS().Get(key));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestEmptyGet()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create(key);
            Assert.Null(db.TS().Get(key));
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.UtcNow;
            TimeSeriesTuple expected = new TimeSeriesTuple(now, 1.1);
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create(key);
            db.TS().Add(key, now, 1.1);
            TimeSeriesTuple actual = db.TS().Get(key);
            Assert.Equal(expected, actual);
        }
    }
}
