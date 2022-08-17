using System;
using System.Collections.Generic;
using System.Threading;
using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Literals.Enums;
using Xunit;
namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestAdd : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "ADD_TESTS";

        public TestAdd(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        [Fact]
        public void TestAddNotExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            db.TS().Create(key);
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddStar()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            db.TS().Add(key, "*", 1.1);
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.True(info.FirstTimeStamp > 0);
            Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddWithRetentionTime()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            long retentionTime = 5000;
            Assert.Equal(now, db.TS().Add(key, now, 1.1, retentionTime: retentionTime));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestAddWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.Equal(now, db.TS().Add(key, now, 1.1, labels: labels));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestAddWithUncompressed()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create(key);
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1, uncompressed: true));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        public void TestAddWithChunkSize()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1, chunkSizeBytes: 128));
            TimeSeriesInformation info = db.TS().Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(128, info.ChunkSize);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyBlock()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));
            Assert.Throws<RedisServerException>(() => db.TS().Add(key, now, 1.2));
        }

        [Fact]
        public void TestAddWithDuplicatePolicyMin()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));

            // Insert a bigger number and check that it did not change the value.
            Assert.Equal(now, db.TS().Add(key, now, 1.2, duplicatePolicy: TsDuplicatePolicy.MIN));
            Assert.Equal(1.1, db.TS().Range(key, now, now)[0].Val);
            // Insert a smaller number and check that it changed.
            Assert.Equal(now, db.TS().Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.MIN));
            Assert.Equal(1.0, db.TS().Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyMax()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));

            // Insert a smaller number and check that it did not change the value.
            Assert.Equal(now, db.TS().Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.MAX));
            Assert.Equal(1.1, db.TS().Range(key, now, now)[0].Val);
            // Insert a bigger number and check that it changed.
            Assert.Equal(now, db.TS().Add(key, now, 1.2, duplicatePolicy: TsDuplicatePolicy.MAX));
            Assert.Equal(1.2, db.TS().Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicySum()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));
            Assert.Equal(now, db.TS().Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.SUM));
            Assert.Equal(2.1, db.TS().Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyFirst()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));
            Assert.Equal(now, db.TS().Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.FIRST));
            Assert.Equal(1.1, db.TS().Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyLast()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, db.TS().Add(key, now, 1.1));
            Assert.Equal(now, db.TS().Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.LAST));
            Assert.Equal(1.0, db.TS().Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestOldAdd()
        {
            TimeStamp old_dt = DateTime.UtcNow;
            Thread.Sleep(1000);
            TimeStamp new_dt = DateTime.UtcNow;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create(key);
            db.TS().Add(key, new_dt, 1.1);
            // Adding old event
            Assert.Equal(old_dt, db.TS().Add(key, old_dt, 1.1));
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 1.1;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ex = Assert.Throws<RedisServerException>(() => db.TS().Add(key, "+", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TS().Add(key, "-", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
