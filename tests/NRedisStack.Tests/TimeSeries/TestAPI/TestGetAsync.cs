using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestGetAsync : AbstractNRedisStackTest
    {
        public TestGetAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestGetNotExists()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.GetAsync(key));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public async Task TestEmptyGet()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            await ts.CreateAsync(key);
            Assert.Null(await ts.GetAsync(key));
        }

        [Fact]
        public async Task TestAddAndGet()
        {
            var key = CreateKeyName();
            var now = DateTime.UtcNow;
            var expected = new TimeSeriesTuple(now, 1.1);
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            await ts.CreateAsync(key);
            await ts.AddAsync(key, now, 1.1);
            var actual = await ts.GetAsync(key);
            Assert.Equal(expected, actual);
        }
    }
}
