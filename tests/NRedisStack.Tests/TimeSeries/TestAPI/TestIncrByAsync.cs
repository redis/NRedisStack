using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestIncrByAsync : AbstractNRedisStackTest
    {
        public TestIncrByAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestDefaultIncrBy()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            Assert.True(await ts.IncrByAsync(key, value) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result!.Val);
        }

        [Fact]
        public async Task TestStarIncrBy()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            Assert.True(await ts.IncrByAsync(key, value, timestamp: "*") > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result!.Val);
        }

        [Fact]
        public async Task TestIncrByTimeStamp()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.IncrByAsync(key, value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), await ts.GetAsync(key));
        }

        [Fact]
        [Obsolete]
        public async Task TestDefaultIncrByWithRetentionTime()
        {
            var key = CreateKeyName();
            var value = 5.5;
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            Assert.True(await ts.IncrByAsync(key, value, retentionTime: retentionTime) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result!.Val);

            var info = await ts.InfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public async Task TestDefaultIncrByWithLabels()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var label = new TimeSeriesLabel("key", "value");
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var labels = new List<TimeSeriesLabel> { label };
            Assert.True(await ts.IncrByAsync(key, value, labels: labels) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result!.Val);

            var info = await ts.InfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestDefaultIncrByWithUncompressed()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            Assert.True(await ts.IncrByAsync(key, value, uncompressed: true) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result!.Val);
        }

        [Fact]
        public async Task TestWrongParameters()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.IncrByAsync(key, value, timestamp: "+"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.IncrByAsync(key, value, timestamp: "-"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
