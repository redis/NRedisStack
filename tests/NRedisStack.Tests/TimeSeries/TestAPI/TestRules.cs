using System;
using System.Collections.Generic;
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

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().KeyDelete(srcKey);
            foreach (var key in destKeys.Values)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        [Fact]
        public void TestRulesAdditionDeletion()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create(srcKey);
            foreach (var destKey in destKeys.Values)
            {
                db.TS().Create(destKey);
            }
            long timeBucket = 50;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<TsAggregation, TimeSeriesRule>();
            foreach (var aggregation in destKeys.Keys)
            {
                var rule = new TimeSeriesRule(destKeys[aggregation], timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(db.TS().CreateRule(srcKey, rule));
                TimeSeriesInformation info = db.TS().Info(srcKey);
                Assert.Equal(rules, info.Rules);
            }
            foreach (var aggregation in destKeys.Keys)
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(db.TS().DeleteRule(srcKey, rule.DestKey));
                TimeSeriesInformation info = db.TS().Info(srcKey);
                Assert.Equal(rules, info.Rules);
            }
        }

        [Fact]
        public void TestNonExistingSrc()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            string destKey = "RULES_DEST_" + TsAggregation.Avg;
            db.TS().Create(destKey);
            TimeSeriesRule rule = new TimeSeriesRule(destKey, 50, TsAggregation.Avg);
            var ex = Assert.Throws<RedisServerException>(() => db.TS().CreateRule(srcKey, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TS().DeleteRule(srcKey, destKey));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestNonExisitingDestinaion()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            string destKey = "RULES_DEST_" + TsAggregation.Avg;
            db.TS().Create(srcKey);
            TimeSeriesRule rule = new TimeSeriesRule(destKey, 50, TsAggregation.Avg);
            var ex = Assert.Throws<RedisServerException>(() => db.TS().CreateRule(srcKey, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => db.TS().DeleteRule(srcKey, destKey));
            Assert.Equal("ERR TSDB: compaction rule does not exist", ex.Message);
        }
    }
}
