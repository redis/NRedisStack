using System;
using System.Collections.Generic;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMRange : AbstractNRedisStackTest, IDisposable
    {
        private readonly string[] keys = { "MRANGE_TESTS_1", "MRANGE_TESTS_2" };

        public TestMRange(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            foreach (string key in keys)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        private List<TimeSeriesTuple> CreateData(IDatabase db, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = new TimeStamp(i * timeBucket);
                foreach (var key in keys)
                {
                    db.TS().Add(key, ts, i);
                }
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleMRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("MRANGEkey", "MRANGEvalue");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TS().Create(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TS().MRange("-", "+", new List<string> { "MRANGEkey=MRANGEvalue" });
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
        }
        [Fact]
        public void TestMRangeWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeWithLabels");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TS().Create(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeWithLabels" }, withLabels: true);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public void TestMRangeSelectLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label1 = new TimeSeriesLabel("key", "MRangeSelectLabels");
            TimeSeriesLabel[] labels = new TimeSeriesLabel[] { new TimeSeriesLabel("team", "CTO"), new TimeSeriesLabel("team", "AUT") };
            for (int i = 0; i < keys.Length; i++)
            {
                db.TS().Create(keys[i], labels: new List<TimeSeriesLabel> { label1, labels[i] });
            }

            var tuples = CreateData(db, 50);
            // selectLabels and withlabels are mutualy exclusive.
            var ex = Assert.Throws<ArgumentException>(() => db.TS().MRange("-", "+", new List<string> { "key=MRangeSelectLabels" },
                                                                                withLabels: true, selectLabels: new List<string> { "team" }));
            Assert.Equal("withLabels and selectLabels cannot be specified together.", ex.Message);

            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeSelectLabels" }, selectLabels: new List<string> { "team" });
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels[i], results[i].labels[0]);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public void TestMRangeFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeFilter");
            var labels = new List<TimeSeriesLabel> { label };
            db.TS().Create(keys[0], labels: labels);
            var tuples = CreateData(db, 50);
            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeFilter" });
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(tuples, results[0].values);
        }

        [Fact]
        public void TestMRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeCount");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TS().Create(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            long count = 5;
            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeCount" }, count: count);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples.GetRange(0, (int)count), results[i].values);
            }
        }

        [Fact]
        public void TestMRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeAggregation");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TS().Create(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeAggregation" }, aggregation: TsAggregation.Min, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public void TestMRangeAlign()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeAlign");
            var labels = new List<TimeSeriesLabel> { label };
            db.TS().Create(keys[0], labels: labels);
            CreateData(db, 50);
            var expected = new List<TimeSeriesTuple> {
                new TimeSeriesTuple(0,1),
                new TimeSeriesTuple(50,1),
                new TimeSeriesTuple(100,1)
            };
            var results = db.TS().MRange(0, "+", new List<string> { "key=MRangeAlign" }, align: "-", aggregation: TsAggregation.Count, timeBucket: 10, count: 3);
            Assert.Equal(1, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(expected, results[0].values);
            results = db.TS().MRange(1, 500, new List<string> { "key=MRangeAlign" }, align: "+", aggregation: TsAggregation.Count, timeBucket: 10, count: 1);
            Assert.Equal(expected[1], results[0].values[0]);
        }

        [Fact]
        public void TestMissingFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MissingFilter");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TS().Create(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TS().MRange("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE/MREVRANGE", ex.Message);
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MissingTimeBucket");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TS().Create(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TS().MRange("-", "+", new List<string> { "key=MissingTimeBucket" }, aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public void TestMRangeGroupby()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            for (int i = 0; i < keys.Length; i++)
            {
                var label1 = new TimeSeriesLabel("key", "MRangeGroupby");
                var label2 = new TimeSeriesLabel("group", i.ToString());
                db.TS().Create(keys[i], labels: new List<TimeSeriesLabel> { label1, label2 });
            }

            var tuples = CreateData(db, 50);
            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeGroupby" }, withLabels: true, groupbyTuple: ("group", TsReduce.Min));
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal("group=" + i, results[i].key);
                Assert.Equal(new TimeSeriesLabel("group", i.ToString()), results[i].labels[0]);
                Assert.Equal(new TimeSeriesLabel("__reducer__", "min"), results[i].labels[1]);
                Assert.Equal(new TimeSeriesLabel("__source__", keys[i]), results[i].labels[2]);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [Fact]
        public void TestMRangeReduce()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            foreach (var key in keys)
            {
                var label = new TimeSeriesLabel("key", "MRangeReduce");
                db.TS().Create(key, labels: new List<TimeSeriesLabel> { label });
            }

            var tuples = CreateData(db, 50);
            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeReduce" }, withLabels: true, groupbyTuple: ("key", TsReduce.Sum));
            Assert.Equal(1, results.Count);
            Assert.Equal("key=MRangeReduce", results[0].key);
            Assert.Equal(new TimeSeriesLabel("key", "MRangeReduce"), results[0].labels[0]);
            Assert.Equal(new TimeSeriesLabel("__reducer__", "sum"), results[0].labels[1]);
            Assert.Equal(new TimeSeriesLabel("__source__", string.Join(",", keys)), results[0].labels[2]);
            for (int i = 0; i < results[0].values.Count; i++)
            {
                Assert.Equal(tuples[i].Val * 2, results[0].values[i].Val);
            }
        }

        [Fact]
        public void TestMRangeFilterBy()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeFilterBy");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                db.TS().Create(key, labels: labels);
            }

            var tuples = CreateData(db, 50);
            var results = db.TS().MRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(tuples.GetRange(0, 3), results[i].values);
            }

            results = db.TS().MRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByTs: new List<TimeStamp> { 0 }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(tuples.GetRange(0, 1), results[i].values);
            }
        }
    }
}
