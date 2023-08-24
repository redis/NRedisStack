using NRedisStack.DataTypes;
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

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().ExecuteBroadcast("FLUSHALL");
        }

        [Fact]
        public void TestAlterRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create(key);
            Assert.True(ts.Alter(key, retentionTime: retentionTime));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public void TestAlterLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
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
        public void TestAlterPolicyAndChunk()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create(key);
            Assert.True(ts.Alter(key, chunkSizeBytes: 128, duplicatePolicy: TsDuplicatePolicy.MIN));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(info.ChunkSize, 128);
            Assert.Equal(info.DuplicatePolicy, TsDuplicatePolicy.MIN);
        }
    }
}
