using System;
using NRedisStack.RedisStackCommands;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using Xunit;

namespace NRedisTimeSeries.Test.TestDataTypes
{
    public class TestTimeSeriesRule
    {
        public TestTimeSeriesRule() { }

        [Fact]
        public void TestRuleConstructor()
        {
            TimeSeriesRule rule = new TimeSeriesRule("key", 50, TsAggregation.Avg);
            Assert.Equal("key", rule.DestKey);
            Assert.Equal(TsAggregation.Avg, rule.Aggregation);
            Assert.Equal(50, rule.TimeBucket);
        }

        [Fact]
        public void TestRuleEquals()
        {
            TimeSeriesRule rule = new TimeSeriesRule("key", 50, TsAggregation.Avg);

            TimeSeriesRule rule1 = new TimeSeriesRule("key", 50, TsAggregation.Avg);
            TimeSeriesRule rule2 = new TimeSeriesRule("key2", 50, TsAggregation.Avg);
            TimeSeriesRule rule3 = new TimeSeriesRule("key", 51, TsAggregation.Avg);
            TimeSeriesRule rule4 = new TimeSeriesRule("key", 50, TsAggregation.Count);

            Assert.Equal(rule, rule1);
            Assert.NotEqual(rule, rule2);
            Assert.NotEqual(rule, rule3);
            Assert.NotEqual(rule, rule4);
        }

        [Fact]
        public void TestRuleHashCode()
        {
            TimeSeriesRule rule = new TimeSeriesRule("key", 50, TsAggregation.Avg);

            TimeSeriesRule rule1 = new TimeSeriesRule("key", 50, TsAggregation.Avg);
            TimeSeriesRule rule2 = new TimeSeriesRule("key2", 50, TsAggregation.Avg);
            TimeSeriesRule rule3 = new TimeSeriesRule("key", 51, TsAggregation.Avg);
            TimeSeriesRule rule4 = new TimeSeriesRule("key", 50, TsAggregation.Count);

            Assert.Equal(rule.GetHashCode(), rule1.GetHashCode());
            Assert.NotEqual(rule.GetHashCode(), rule2.GetHashCode());
            Assert.NotEqual(rule.GetHashCode(), rule3.GetHashCode());
            Assert.NotEqual(rule.GetHashCode(), rule4.GetHashCode());
        }

        [Fact]
        public void TestRuleToString()
        {
            TimeSeriesRule rule = new TimeSeriesRule("key", 50, TsAggregation.Avg);
            Assert.Equal("DestinationKey: key, TimeBucket: 50, Aggregation: AVG", (string)rule);
        }
    }
}
