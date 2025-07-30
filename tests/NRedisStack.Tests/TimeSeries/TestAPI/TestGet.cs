#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestGet : AbstractNRedisStackTest, IDisposable
    {

        private readonly string key = "GET_TESTS";

        public TestGet(EndpointsFixture endpointsFixture) : base(endpointsFixture) { }


        [Fact]
        public void TestGetNotExists()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            var ex = Assert.Throws<RedisServerException>(() => ts.Get(key));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestEmptyGet()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            ts.Create(key);
            Assert.Null(ts.Get(key));
        }

        [Fact]
        public void TestAddAndGet()
        {
            DateTime now = DateTime.UtcNow;
            TimeSeriesTuple expected = new TimeSeriesTuple(now, 1.1);
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            ts.Create(key);
            ts.Add(key, now, 1.1);
            TimeSeriesTuple actual = ts.Get(key)!;
            Assert.Equal(expected, actual);
        }
    }
}
