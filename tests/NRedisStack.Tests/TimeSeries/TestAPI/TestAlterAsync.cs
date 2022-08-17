using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestAlterAsync : AbstractNRedisStackTest
    {
        public TestAlterAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestAlterRetentionTime()
        {
            var key = CreateKeyName();
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync(key);
            Assert.True(await db.TS().AlterAsync(key, retentionTime: retentionTime));

            var info = await db.TS().InfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestAlterLabels()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TS().CreateAsync(key);
            Assert.True(await db.TS().AlterAsync(key, labels: labels));

            var info = await db.TS().InfoAsync(key);
            Assert.Equal(labels, info.Labels);

            labels.Clear();
            Assert.True(await db.TS().AlterAsync(key, labels: labels));

            info = await db.TS().InfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

    }
}
