using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestQueryIndex : AbstractNRedisStackTest, IDisposable
    {
        private readonly string[] keys = { "QUERYINDEX_TESTS_1", "QUERYINDEX_TESTS_2" };

        public TestQueryIndex(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            foreach (var key in keys)
            {
                redisFixture.Redis.GetDatabase().ExecuteBroadcast("FLUSHALL");
            }
        }

        [SkipIfRedis(Is.Cluster)]
        public void TestTSQueryIndex()
        {
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var label1 = new TimeSeriesLabel("QUERYINDEX_TESTS_1", "value");
            var label2 = new TimeSeriesLabel("QUERYINDEX_TESTS_2", "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            ts.Create(keys[0], labels: labels1);
            ts.Create(keys[1], labels: labels2);
            Assert.Equal(keys, ts.QueryIndex(new List<string> { "QUERYINDEX_TESTS_1=value" }));
            Assert.Equal(new List<string> { keys[0] }, ts.QueryIndex(new List<string> { "QUERYINDEX_TESTS_2=value2" }));
        }
    }
}
