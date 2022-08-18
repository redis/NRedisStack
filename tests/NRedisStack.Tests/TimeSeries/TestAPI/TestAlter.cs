using System;
using System.Collections.Generic;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestAlter : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "ALTER_TESTS";

        public TestAlter(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestAlterRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create(key);
            Assert.True(db.TS().Alter(key, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestAlterLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create(key);
            Assert.True(db.TS().Alter(key, labels: labels));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(labels, info.Labels);
            labels.Clear();
            Assert.True(db.TS().Alter(key, labels: labels));
            info = db.TS().Info(key);
            Assert.Equal(labels, info.Labels);
        }

    }
}
