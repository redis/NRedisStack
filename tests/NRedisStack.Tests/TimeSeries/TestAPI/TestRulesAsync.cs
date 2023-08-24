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

        [SkipIfRedis(Is.Cluster)]
        public async Task TestRulesAdditionDeletion()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            await ts.CreateAsync(key);
            var aggregations = (TsAggregation[])Enum.GetValues(typeof(TsAggregation));

            foreach (var aggregation in aggregations)
            {
                await ts.CreateAsync($"{key}:{aggregation}");
            }

            var timeBucket = 50L;
            var rules = new List<TimeSeriesRule>();
            var rulesMap = new Dictionary<TsAggregation, TimeSeriesRule>();
            foreach (var aggregation in aggregations)
            {
                var rule = new TimeSeriesRule($"{key}:{aggregation}", timeBucket, aggregation);
                rules.Add(rule);
                rulesMap[aggregation] = rule;
                Assert.True(await ts.CreateRuleAsync(key, rule));

                var info = await ts.InfoAsync(key);
                Assert.Equal(rules, info.Rules);
            }

            foreach (var aggregation in aggregations)
            {
                var rule = rulesMap[aggregation];
                rules.Remove(rule);
                Assert.True(await ts.DeleteRuleAsync(key, rule.DestKey));

                var info = await ts.InfoAsync(key);
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
            var ts = db.TS();
            await ts.CreateAsync(aggKey);
            var rule = new TimeSeriesRule(aggKey, 50, TsAggregation.Avg);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.CreateRuleAsync(key, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.DeleteRuleAsync(key, aggKey));
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
            var ts = db.TS();
            await ts.CreateAsync(key);
            var rule = new TimeSeriesRule(aggKey, 50, TsAggregation.Avg);
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.CreateRuleAsync(key, rule));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.DeleteRuleAsync(key, aggKey));
            Assert.Equal("ERR TSDB: compaction rule does not exist", ex.Message);
        }

        [SkipIfRedis(Is.Cluster)]
        public async Task TestAlignTimestampAsync()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            await ts.CreateAsync("ts1");
            await ts.CreateAsync("ts2");
            await ts.CreateAsync("ts3");

            TimeSeriesRule rule1 = new TimeSeriesRule("ts2", 10, TsAggregation.Count);
            await ts.CreateRuleAsync("ts1", rule1, 0);

            TimeSeriesRule rule2 = new TimeSeriesRule("ts3", 10, TsAggregation.Count);
            await ts.CreateRuleAsync("ts1", rule2, 1);

            await ts.AddAsync("ts1", 1, 1);
            await ts.AddAsync("ts1", 10, 3);
            await ts.AddAsync("ts1", 21, 7);

            Assert.Equal(2, (await ts.RangeAsync("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
            Assert.Equal(1, (await ts.RangeAsync("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
        }
    }
}
