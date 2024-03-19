using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestAlterAsync : AbstractNRedisStackTest
    {
        public TestAlterAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        [Obsolete]
        public async Task TestAlterRetentionTime()
        {
            var key = CreateKeyName();
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            await ts.CreateAsync(key);
            Assert.True(await ts.AlterAsync(key, retentionTime: retentionTime));

            var info = await ts.InfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public async Task TestAlterLabels()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await ts.CreateAsync(key);
            Assert.True(await ts.AlterAsync(key, labels: labels));

            var info = await ts.InfoAsync(key);
            Assert.Equal(labels, info.Labels);

            labels.Clear();
            Assert.True(await ts.AlterAsync(key, labels: labels));

            info = await ts.InfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        [Obsolete]
        public async Task TestAlterPolicyAndChunkAsync()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            ts.Create(key);
            Assert.True(await ts.AlterAsync(key, chunkSizeBytes: 128, duplicatePolicy: TsDuplicatePolicy.MIN));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(128, info.ChunkSize);
            Assert.Equal(TsDuplicatePolicy.MIN, info.DuplicatePolicy);
        }

    }
}
