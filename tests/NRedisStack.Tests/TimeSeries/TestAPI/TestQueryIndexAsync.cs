using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestQueryIndexAsync : AbstractTimeSeriesTest
    {
        public TestQueryIndexAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestTSQueryIndex()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label1 = new TimeSeriesLabel(keys[0], "value");
            var label2 = new TimeSeriesLabel(keys[1], "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            await db.TS().CreateAsync(keys[0], labels: labels1);
            await db.TS().CreateAsync(keys[1], labels: labels2);
            Assert.Equal(keys, db.TS().QueryIndex(new List<string> { $"{keys[0]}=value" }));
            Assert.Equal(new List<string> { keys[0] }, db.TS().QueryIndex(new List<string> { $"{keys[1]}=value2" }));
        }
    }
}
