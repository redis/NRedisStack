using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRules : AbstractNRedisStackTest, IDisposable
    {
        private string srcKey = "RULES_TEST_SRC";

        private Dictionary<TsAggregation, string> destKeys;

        public TestRules(RedisFixture redisFixture) : base(redisFixture)
        {

            destKeys = new Dictionary<TsAggregation, string>
            {
                { TsAggregation.Avg, "RULES_DEST_" + TsAggregation.Avg },
                { TsAggregation.Count, "RULES_DEST_" + TsAggregation.Count },
                { TsAggregation.First, "RULES_DEST_" + TsAggregation.First },
                { TsAggregation.Last, "RULES_DEST_" + TsAggregation.Last },
                { TsAggregation.Max, "RULES_DEST_" + TsAggregation.Max },
                { TsAggregation.Min, "RULES_DEST_" + TsAggregation.Min },
                { TsAggregation.Range, "RULES_DEST_" + TsAggregation.Range },
                { TsAggregation.StdP, "RULES_DEST_" + TsAggregation.StdP },
                { TsAggregation.StdS, "RULES_DEST_" + TsAggregation.StdS },
                { TsAggregation.Sum, "RULES_DEST_" + TsAggregation.Sum },
                { TsAggregation.VarP, "RULES_DEST_" + TsAggregation.VarP },
                { TsAggregation.VarS, "RULES_DEST_" + TsAggregation.VarS }
            };
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        [Obsolete]
        public void TestRulesAdditionDeletion()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            ts.Create(srcKey);
            foreach (var destKey in destKeys.Values)
            {
                ts.Create(destKey);
            }
            long timeBucket = 50;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<TsAggregation, TimeSeriesRule>();
            foreach (var aggregation in destKeys.Keys)
            {
                var rule = new TimeSeriesRule(destKeys[aggregation], timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(ts.CreateRule(srcKey, rule));
                TimeSeriesInformation info = ts.Info(srcKey);
                Assert.Equal(rules, info.Rules);
            }
            foreach (var aggregation in destKeys.Keys)
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(ts.DeleteRule(srcKey, rule.DestKey));
                TimeSeriesInformation info = ts.Info(srcKey);
                Assert.Equal(rules, info.Rules);
            }
        }

        [Fact]
        public void TestNonExistingSrc()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            string destKey = "RULES_DEST_" + TsAggregation.Avg;
            ts.Create(destKey);
            TimeSeriesRule rule = new TimeSeriesRule(destKey, 50, TsAggregation.Avg);
            var ex = Assert.Throws<RedisServerException>(() => ts.CreateRule(srcKey, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => ts.DeleteRule(srcKey, destKey));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestNonExisitingDestinaion()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            string destKey = "RULES_DEST_" + TsAggregation.Avg;
            ts.Create(srcKey);
            TimeSeriesRule rule = new TimeSeriesRule(destKey, 50, TsAggregation.Avg);
            var ex = Assert.Throws<RedisServerException>(() => ts.CreateRule(srcKey, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => ts.DeleteRule(srcKey, destKey));
            Assert.Equal("ERR TSDB: compaction rule does not exist", ex.Message);
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestAlignTimestamp()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            ts.Create("ts1");
            ts.Create("ts2");
            ts.Create("ts3");

            TimeSeriesRule rule1 = new TimeSeriesRule("ts2", 10, TsAggregation.Count);
            ts.CreateRule("ts1", rule1, 0);

            TimeSeriesRule rule2 = new TimeSeriesRule("ts3", 10, TsAggregation.Count);
            ts.CreateRule("ts1", rule2, 1);

            ts.Add("ts1", 1, 1);
            ts.Add("ts1", 10, 3);
            ts.Add("ts1", 21, 7);

            Assert.Equal(2, ts.Range("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10).Count);
            Assert.Equal(1, ts.Range("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10).Count);
        }
    }
}
