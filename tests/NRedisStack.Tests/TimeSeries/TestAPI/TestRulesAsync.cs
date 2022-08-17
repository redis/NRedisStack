using System;
using System.Linq;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRulesAsync : AbstractNRedisStackTest
    {
        public TestRulesAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestRulesAdditionDeletion()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync(key);
            var aggregations = (TsAggregation[])Enum.GetValues(typeof(TsAggregation));

            foreach (var aggregation in aggregations)
            {
                await db.TS().CreateAsync($"{key}:{aggregation}");
            }

            var timeBucket = 50L;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<TsAggregation, TimeSeriesRule>();
            foreach (var aggregation in aggregations)
            {
                var rule = new TimeSeriesRule($"{key}:{aggregation}", timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(await db.TS().CreateRuleAsync(key, rule));

                var info = await db.TS().InfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            foreach (var aggregation in aggregations)
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(await db.TS().DeleteRuleAsync(key, rule.DestKey));

                var info = await db.TS().InfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            await db.KeyDeleteAsync(aggregations.Select(i => (RedisKey)$"{key}:{i}").ToArray());
        }

        [Fact]
        public async Task TestNonExistingSrc()
        {
            var key = CreateKeyName();
            var aggKey = $"{key}:{TsAggregation.Avg}";
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync(aggKey);
            var rule = new TimeSeriesRule(aggKey, 50, TsAggregation.Avg);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TS().CreateRuleAsync(key, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TS().DeleteRuleAsync(key, aggKey));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            await db.KeyDeleteAsync(aggKey);
        }

        [Fact]
        public async Task TestNonExisitingDestinaion()
        {
            var key = CreateKeyName();
            var aggKey = $"{key}:{TsAggregation.Avg}";
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync(key);
            var rule = new TimeSeriesRule(aggKey, 50, TsAggregation.Avg);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TS().CreateRuleAsync(key, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TS().DeleteRuleAsync(key, aggKey));
            Assert.Equal("ERR TSDB: compaction rule does not exist", ex.Message);
        }
    }
}
