using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System;
using System.Threading.Tasks;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestGetAsync : AbstractTimeSeriesTest
    {
        public TestGetAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestGetNotExists()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TS().GetAsync(key));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public async Task TestEmptyGet()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync(key);
            Assert.Null(await db.TS().GetAsync(key));
        }

        [Fact]
        public async Task TestAddAndGet()
        {
            var key = CreateKeyName();
            var now = DateTime.UtcNow;
            var expected = new TimeSeriesTuple(now, 1.1);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync(key);
            await db.TS().AddAsync(key, now, 1.1);
            var actual = await db.TS().GetAsync(key);
            Assert.Equal(expected, actual);
        }
    }
}
