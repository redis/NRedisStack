using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using StackExchange.Redis;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMRevRangeAsync : AbstractNRedisStackTest
    {
        public TestMRevRangeAsync(RedisFixture redisFixture) : base(redisFixture) { }

        private async Task<List<TimeSeriesTuple>> CreateData(IDatabase db, string[] keys, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (var i = 0; i < 10; i++)
            {
                var ts = new TimeStamp(i * timeBucket);
                foreach (var key in keys)
                {
                    await db.TS().AddAsync(key, ts, i);
                }
                tuples.Add(new TimeSeriesTuple(ts, i));
            }

            return tuples;
        }

        [Fact]
        public async Task TestSimpleMRevRange()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" });
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public async Task TestMRevRangeWithLabels()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public async Task TestMRevRangeSelectLabels()
        {
            var keys = CreateKeyNames(2);
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label1 = new TimeSeriesLabel(keys[0], "value");
            TimeSeriesLabel[] labels = new TimeSeriesLabel[] { new TimeSeriesLabel("team", "CTO"), new TimeSeriesLabel("team", "AUT") };
            for (int i = 0; i < keys.Length; i++)
            {
                await db.TS().CreateAsync(keys[i], labels: new List<TimeSeriesLabel> { label1, labels[i] });
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, selectLabels: new List<string> { "team" });
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels[i], results[i].labels[0]);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public async Task TestMRevRangeFilter()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            await db.TS().CreateAsync(keys[0], labels: labels);
            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" });
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(ReverseData(tuples), results[0].values);
        }

        [Fact]
        public async Task TestMRevRangeCount()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var count = 5L;
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, count: count);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples).GetRange(0, (int)count), results[i].values);
            }
        }

        [Fact]
        public async Task TestMRangeAggregation()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, aggregation: TsAggregation.Min, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public async Task TestMRevRangeAlign()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            db.TS().Create(keys[0], labels: labels);
            await CreateData(db, keys, 50);
            var expected = new List<TimeSeriesTuple> {
                new TimeSeriesTuple(450,1),
                new TimeSeriesTuple(400,1),
                new TimeSeriesTuple(350,1)
            };
            var results = await db.TS().MRevRangeAsync(0, "+", new List<string> { $"{keys[0]}=value" }, align: "-", aggregation: TsAggregation.Count, timeBucket: 10, count: 3);
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(expected, results[0].values);
            results = await db.TS().MRevRangeAsync(0, 500, new List<string> { $"{keys[0]}=value" }, align: "+", aggregation: TsAggregation.Count, timeBucket: 10, count: 1);
            Assert.Equal(expected[0], results[0].values[0]);
        }

        [Fact]
        public async Task TestMissingFilter()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await db.TS().MRevRangeAsync("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE/MREVRANGE", ex.Message);
        }

        [Fact]
        public async Task TestMissingTimeBucket()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () =>
            {
                await db.TS().MRevRangeAsync("-", "+",
                    filter: new List<string>() { $"key=value" },
                    aggregation: TsAggregation.Avg);
            });
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public async Task TestMRevRangeGroupby()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            for (int i = 0; i < keys.Length; i++)
            {
                var label1 = new TimeSeriesLabel(keys[0], "value");
                var label2 = new TimeSeriesLabel("group", i.ToString());
                await db.TS().CreateAsync(keys[i], labels: new List<TimeSeriesLabel> { label1, label2 });
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: ("group", TsReduce.Min));
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal("group=" + i, results[i].key);
                Assert.Equal(new TimeSeriesLabel("group", i.ToString()), results[i].labels[0]);
                Assert.Equal(new TimeSeriesLabel("__reducer__", "min"), results[i].labels[1]);
                Assert.Equal(new TimeSeriesLabel("__source__", keys[i]), results[i].labels[2]);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [Fact]
        public async Task TestMRevRangeReduce()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            foreach (var key in keys)
            {
                var label = new TimeSeriesLabel(keys[0], "value");
                await db.TS().CreateAsync(key, labels: new List<TimeSeriesLabel> { label });
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: (keys[0], TsReduce.Sum));
            Assert.Equal(1, results.Count);
            Assert.Equal($"{keys[0]}=value", results[0].key);
            Assert.Equal(new TimeSeriesLabel(keys[0], "value"), results[0].labels[0]);
            Assert.Equal(new TimeSeriesLabel("__reducer__", "sum"), results[0].labels[1]);
            Assert.Equal(new TimeSeriesLabel("__source__", string.Join(",", keys)), results[0].labels[2]);
            tuples = ReverseData(tuples);
            for (int i = 0; i < results[0].values.Count; i++)
            {
                Assert.Equal(tuples[i].Val * 2, results[0].values[i].Val);
            }
        }

        [Fact]
        public async Task TestMRevRangeFilterBy()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                await db.TS().CreateAsync(key, labels: labels);
            }

            var tuples = await CreateData(db, keys, 50);
            var results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(ReverseData(tuples.GetRange(0, 3)), results[i].values);
            }

            results = await db.TS().MRevRangeAsync("-", "+", new List<string> { $"{keys[0]}=value" }, filterByTs: new List<TimeStamp> { 0 }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(ReverseData(tuples.GetRange(0, 1)), results[i].values);
            }
        }
    }
}
