using System;
using System.Collections.Generic;
using StackExchange.Redis;
using NRedisTimeSeries;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Literals.Enums;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestCreate : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "CREATE_TESTS";

        public TestCreate(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestCreateOK()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key));
            TimeSeriesInformation info = db.TS().Info(key);
        }

        [Fact]
        public void TestCreateRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestCreateLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, labels: labels));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateEmptyLabels()
        {
            var labels = new List<TimeSeriesLabel>();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, labels: labels));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateUncompressed()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, uncompressed: true));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyFirst()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, duplicatePolicy: TsDuplicatePolicy.FIRST));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyLast()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, duplicatePolicy: TsDuplicatePolicy.LAST));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyMin()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, duplicatePolicy: TsDuplicatePolicy.MIN));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyMax()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, duplicatePolicy: TsDuplicatePolicy.MAX));
        }

        [Fact]
        public void TestCreatehDuplicatePolicySum()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            Assert.True(db.TS().Create(key, duplicatePolicy: TsDuplicatePolicy.SUM));
        }
    }
}
