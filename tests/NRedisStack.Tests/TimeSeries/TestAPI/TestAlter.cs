﻿using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestAlter : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "ALTER_TESTS";

        public TestAlter(RedisFixture redisFixture) : base(redisFixture) { }


        [Fact]
        [Obsolete]
        public void TestAlterRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            ts.Create(key);
            Assert.True(ts.Alter(key, retentionTime: retentionTime));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public void TestAlterLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            ts.Create(key);
            Assert.True(ts.Alter(key, labels: labels));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(labels, info.Labels);
            labels.Clear();
            Assert.True(ts.Alter(key, labels: labels));
            info = ts.Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        [Obsolete]
        public void TestAlterPolicyAndChunk()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            ts.Create(key);
            Assert.True(ts.Alter(key, chunkSizeBytes: 128, duplicatePolicy: TsDuplicatePolicy.MIN));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(128, info.ChunkSize);
            Assert.Equal(TsDuplicatePolicy.MIN, info.DuplicatePolicy);
        }
    }
}
