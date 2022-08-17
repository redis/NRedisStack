using System;
using System.Collections.Generic;
using NRedisStack.RedisStackCommands;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestIncrBy : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "INCRBY_TESTS";

        public TestIncrBy(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestDefaultIncrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().IncrBy(key, value) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
        }

        [Fact]
        public void TestStarIncrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().IncrBy(key, value, timestamp: "*") > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
        }

        [Fact]
        public void TestIncrByTimeStamp()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, db.TS().IncrBy(key, value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), db.TS().Get(key));
        }

        [Fact]
        public void TestDefaultIncrByWithRetentionTime()
        {
            double value = 5.5;
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().IncrBy(key, value, retentionTime: retentionTime) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestDefaultIncrByWithLabels()
        {
            double value = 5.5;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.True(db.TS().IncrBy(key, value, labels: labels) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestDefaultIncrByWithUncompressed()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().IncrBy(key, value, uncompressed: true) > 0);
            Assert.Equal(value, db.TS().Get(key).Val);
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ex = Assert.Throws<RedisServerException>(() => db.TS().IncrBy(key, value, timestamp: "+"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TS().IncrBy(key, value, timestamp: "-"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
