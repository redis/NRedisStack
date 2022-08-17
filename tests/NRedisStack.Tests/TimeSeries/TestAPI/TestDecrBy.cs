using System;
using System.Collections.Generic;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestDecrBy : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "DECRBY_TESTS";

        public TestDecrBy(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestDefaultDecrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().DecrBy(key, -value) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
        }

        [Fact]
        public void TestStarDecrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().DecrBy(key, -value, timestamp: "*") > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
        }

        [Fact]
        public void TestDecrByTimeStamp()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TS().DecrBy(key, -value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), db.TS().Get(key));
        }

        [Fact]
        public void TestDefaultDecrByWithRetentionTime()
        {
            double value = 5.5;
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().DecrBy(key, -value, retentionTime: retentionTime) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestDefaultDecrByWithLabels()
        {
            double value = 5.5;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.True(db.TS().DecrBy(key, -value, labels: labels) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestDefaultDecrByWithUncompressed()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().DecrBy(key, -value, uncompressed: true) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ex = Assert.Throws<RedisServerException>(() => db.TS().DecrBy(key, value, timestamp: "+"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TS().DecrBy(key, value, timestamp: "-"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
