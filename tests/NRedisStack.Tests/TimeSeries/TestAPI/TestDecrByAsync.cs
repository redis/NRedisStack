using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestDecrByAsync : AbstractNRedisStackTest
    {
        public TestDecrByAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestDefaultDecrBy()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(await ts.DecrByAsync(key, -value) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result.Val);
        }

        [Fact]
        public async Task TestStarDecrBy()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(await ts.DecrByAsync(key, -value, timestamp: "*") > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result.Val);
        }

        [Fact]
        public async Task TestDecrByTimeStamp()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.DecrByAsync(key, -value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), await ts.GetAsync(key));
        }

        [Fact]
        public async Task TestDefaultDecrByWithRetentionTime()
        {
            var key = CreateKeyName();
            var value = 5.5;
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(await ts.DecrByAsync(key, -value, retentionTime: retentionTime) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result.Val);

            var info = await ts.InfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestDefaultDecrByWithLabels()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var label = new TimeSeriesLabel("key", "value");
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var labels = new List<TimeSeriesLabel> { label };
            Assert.True(await ts.DecrByAsync(key, -value, labels: labels) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result.Val);

            var info = await ts.InfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestDefaultDecrByWithUncompressed()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(await ts.DecrByAsync(key, -value, uncompressed: true) > 0);

            var result = await ts.GetAsync(key);
            Assert.Equal(value, result.Val);
        }

        [Fact]
        public async Task TestWrongParameters()
        {
            var key = CreateKeyName();
            var value = 5.5;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.DecrByAsync(key, value, timestamp: "+"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.DecrByAsync(key, value, timestamp: "-"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
