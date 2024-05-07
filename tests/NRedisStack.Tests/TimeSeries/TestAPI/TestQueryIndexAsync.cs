using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestQueryIndexAsync : AbstractNRedisStackTest
    {
        public TestQueryIndexAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise, Is.EnterpriseOssCluster)]
        public async Task TestTSQueryIndex()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var label1 = new TimeSeriesLabel(keys[0], "value");
            var label2 = new TimeSeriesLabel(keys[1], "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            await ts.CreateAsync(keys[0], labels: labels1);
            await ts.CreateAsync(keys[1], labels: labels2);
            Assert.Equal(keys, ts.QueryIndex(new List<string> { $"{keys[0]}=value" }));
            Assert.Equal(new List<string> { keys[0] }, ts.QueryIndex(new List<string> { $"{keys[1]}=value2" }));
        }
    }
}
