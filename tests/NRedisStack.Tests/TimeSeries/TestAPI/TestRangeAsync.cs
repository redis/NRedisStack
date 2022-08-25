﻿using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRangeAsync : AbstractNRedisStackTest
    {
        public TestRangeAsync(RedisFixture redisFixture) : base(redisFixture) { }

        private async Task<List<TimeSeriesTuple>> CreateData(IDatabase db, string key, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (var i = 0; i < 10; i++)
            {
                var ts = await db.TS().AddAsync(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public async Task TestSimpleRange()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(tuples, await db.TS().RangeAsync(key, "-", "+"));
        }

        [Fact]
        public async Task TestRangeCount()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(tuples.GetRange(0, 5), await db.TS().RangeAsync(key, "-", "+", count: 5));
        }

        [Fact]
        public async Task TestRangeAggregation()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = await CreateData(db, key, 50);
            Assert.Equal(tuples, await db.TS().RangeAsync(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public async Task TestRangeAlign()
        {
            var key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 10),
                new TimeSeriesTuple(3, 5),
                new TimeSeriesTuple(11, 10),
                new TimeSeriesTuple(21, 11)
            };

            foreach (var tuple in tuples)
            {
                await db.TS().AddAsync(key, tuple.Time, tuple.Val);
            }

            // Aligh start
            var resStart = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 2),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(21, 1)
            };
            Assert.Equal(resStart, await db.TS().RangeAsync(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(0, 2),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(20, 1)
            };
            Assert.Equal(resEnd, await db.TS().RangeAsync(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, await db.TS().RangeAsync(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public async Task TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = await CreateData(db, key, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await db.TS().RangeAsync(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public async Task TestFilterBy()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = await CreateData(db, key, 50);

            var res = await db.TS().RangeAsync(key, "-", "+", filterByValue: (0, 2)); // The first 3 tuples
            Assert.Equal(3, res.Count);
            Assert.Equal(tuples.GetRange(0, 3), res);

            var filterTs = new List<TimeStamp> { 0, 50, 100 }; // Also the first 3 tuples
            res = await db.TS().RangeAsync(key, "-", "+", filterByTs: filterTs);
            Assert.Equal(tuples.GetRange(0, 3), res);

            res = await db.TS().RangeAsync(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5)); // The third tuple
            Assert.Equal(tuples.GetRange(2, 1), res);
        }

        [Fact]
        public async Task TestLatestAsync()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().CreateAsync("ts1");
            await db.TS().CreateAsync("ts2");
            await db.TS().CreateRuleAsync("ts1", new TimeSeriesRule("ts2", 10, TsAggregation.Sum));
            await db.TS().AddAsync("ts1", 1, 1);
            await db.TS().AddAsync("ts1", 2, 3);
            await db.TS().AddAsync("ts1", 11, 7);
            await db.TS().AddAsync("ts1", 13, 1);
            var range = await db.TS().RangeAsync("ts1", 0, 20);
            Assert.Equal(4, range.Count);

            var compact = new TimeSeriesTuple(0, 4);
            var latest = new TimeSeriesTuple(10, 8);

            // get
            Assert.Equal(compact, await db.TS().GetAsync("ts2"));

            Assert.Equal(latest, await db.TS().GetAsync("ts2", true));

            // range
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, await db.TS().RangeAsync("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { compact, latest }, await db.TS().RangeAsync("ts2", 0, 10, true));

            // revrange
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, await db.TS().RevRangeAsync("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { latest, compact }, await db.TS().RevRangeAsync("ts2", 0, 10, true));
        }

        [Fact]
        public async Task TestAlignTimestampAsync()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create("ts1");
            db.TS().Create("ts2");
            db.TS().Create("ts3");
            db.TS().CreateRule("ts1", new TimeSeriesRule("ts2", 10, TsAggregation.Count), 0);
            db.TS().CreateRule("ts1", new TimeSeriesRule("ts3", 10, TsAggregation.Count), 1);
            db.TS().Add("ts1", 1, 1);
            db.TS().Add("ts1", 10, 3);
            db.TS().Add("ts1", 21, 7);
            Assert.Equal(2, (await db.TS().RangeAsync("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
            Assert.Equal(1, (await db.TS().RangeAsync("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
        }
    }
}
