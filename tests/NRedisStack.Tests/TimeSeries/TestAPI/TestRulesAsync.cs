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

        [Fact]
        public async Task TestAlignTimestampAsync()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync("ts1");
            await db.TS().CreateAsync("ts2");
            await db.TS().CreateAsync("ts3");

            TimeSeriesRule rule1 = new TimeSeriesRule("ts2", 10, TsAggregation.Count);
            await db.TS().CreateRuleAsync("ts1", rule1, 0);

            TimeSeriesRule rule2 = new TimeSeriesRule("ts3", 10, TsAggregation.Count);
            await db.TS().CreateRuleAsync("ts1", rule2, 1);

            await db.TS().AddAsync("ts1", 1, 1);
            await db.TS().AddAsync("ts1", 10, 3);
            await db.TS().AddAsync("ts1", 21, 7);

            Assert.Equal(2, (await db.TS().RangeAsync("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
            Assert.Equal(1, (await db.TS().RangeAsync("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
        }
    }
}
