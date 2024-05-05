using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMRange : AbstractNRedisStackTest, IDisposable
    {
        private readonly string[] _keys = { "MRANGE_TESTS_1", "MRANGE_TESTS_2" };

        public TestMRange(RedisFixture redisFixture) : base(redisFixture) { }

        private List<TimeSeriesTuple> CreateData(ITimeSeriesCommands ts, int timeBucket, string[]? keys = null)
        {
            keys ??= _keys;
            var tuples = new List<TimeSeriesTuple>();

            for (int i = 0; i < 10; i++)
            {
                TimeStamp timeStamp = new TimeStamp(i * timeBucket);
                foreach (var key in keys)
                {
                    ts.Add(key, timeStamp, i);
                }
                tuples.Add(new TimeSeriesTuple(timeStamp, i));
            }
            return tuples;
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestSimpleMRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("MRANGEkey", "MRANGEvalue");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in _keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, 50);
            var results = ts.MRange("-", "+", new List<string> { "MRANGEkey=MRANGEvalue" });
            Assert.Equal(_keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(_keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
        }
        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeWithLabels");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in _keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, 50);
            var results = ts.MRange("-", "+", new List<string> { "key=MRangeWithLabels" }, withLabels: true);
            Assert.Equal(_keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(_keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeSelectLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label1 = new TimeSeriesLabel("key", "MRangeSelectLabels");
            TimeSeriesLabel[] labels = new TimeSeriesLabel[] { new TimeSeriesLabel("team", "CTO"), new TimeSeriesLabel("team", "AUT") };
            for (int i = 0; i < _keys.Length; i++)
            {
                ts.Create(_keys[i], labels: new List<TimeSeriesLabel> { label1, labels[i] });
            }

            var tuples = CreateData(ts, 50);
            // selectLabels and withlabels are mutualy exclusive.
            var ex = Assert.Throws<ArgumentException>(() => ts.MRange("-", "+", new List<string> { "key=MRangeSelectLabels" },
                                                                                withLabels: true, selectLabels: new List<string> { "team" }));
            Assert.Equal("withLabels and selectLabels cannot be specified together.", ex.Message);

            var results = ts.MRange("-", "+", new List<string> { "key=MRangeSelectLabels" }, selectLabels: new List<string> { "team" });
            Assert.Equal(_keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(_keys[i], results[i].key);
                Assert.Equal(labels[i], results[i].labels[0]);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeFilter");
            var labels = new List<TimeSeriesLabel> { label };
            ts.Create(_keys[0], labels: labels);
            var tuples = CreateData(ts, 50);
            var results = ts.MRange("-", "+", new List<string> { "key=MRangeFilter" });
            Assert.Equal(1, results.Count);
            Assert.Equal(_keys[0], results[0].key);
            Assert.Equal(0, results[0].labels.Count);
            Assert.Equal(tuples, results[0].values);
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeCount");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in _keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, 50);
            long count = 5;
            var results = ts.MRange("-", "+", new List<string> { "key=MRangeCount" }, count: count);
            Assert.Equal(_keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(_keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples.GetRange(0, (int)count), results[i].values);
            }
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeAggregation");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in _keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, 50);
            var results = ts.MRange("-", "+", new List<string> { "key=MRangeAggregation" }, aggregation: TsAggregation.Min, timeBucket: 50);
            Assert.Equal(_keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(_keys[i], results[i].key);
                Assert.Equal(0, results[i].labels.Count);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeAlign()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeAlign");
            var labels = new List<TimeSeriesLabel> { label };
            ts.Create(_keys[0], labels: labels);
            CreateData(ts, 50);
            var expected = new List<TimeSeriesTuple> {
                new TimeSeriesTuple(0,1),
                new TimeSeriesTuple(50,1),
                new TimeSeriesTuple(100,1)
            };
            var results = ts.MRange(0, "+", new List<string> { "key=MRangeAlign" }, align: "-", aggregation: TsAggregation.Count, timeBucket: 10, count: 3);
            Assert.Equal(1, results.Count);
            Assert.Equal(_keys[0], results[0].key);
            Assert.Equal(expected, results[0].values);
            results = ts.MRange(1, 500, new List<string> { "key=MRangeAlign" }, align: "+", aggregation: TsAggregation.Count, timeBucket: 10, count: 1);
            Assert.Equal(expected[1], results[0].values[0]);
        }

        [Fact]
        public void TestMissingFilter()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MissingFilter");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in _keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, 50);
            var ex = Assert.Throws<ArgumentException>(() => ts.MRange("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE/MREVRANGE", ex.Message);
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MissingTimeBucket");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in _keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, 50);
            var ex = Assert.Throws<ArgumentException>(() => ts.MRange("-", "+", new List<string> { "key=MissingTimeBucket" }, aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeGroupby()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            for (int i = 0; i < _keys.Length; i++)
            {
                var label1 = new TimeSeriesLabel("key", "MRangeGroupby");
                var label2 = new TimeSeriesLabel("group", i.ToString());
                ts.Create(_keys[i], labels: new List<TimeSeriesLabel> { label1, label2 });
            }

            var tuples = CreateData(ts, 50);
            var results = ts.MRange("-", "+", new List<string> { "key=MRangeGroupby" }, withLabels: true, groupbyTuple: ("group", TsReduce.Min));
            Assert.Equal(_keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal("group=" + i, results[i].key);
                Assert.Equal(new TimeSeriesLabel("group", i.ToString()), results[i].labels[0]);
                Assert.Equal(new TimeSeriesLabel("__reducer__", "min"), results[i].labels[1]);
                Assert.Equal(new TimeSeriesLabel("__source__", _keys[i]), results[i].labels[2]);
                Assert.Equal(tuples, results[i].values);
            }
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeReduce()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            foreach (var key in _keys)
            {
                var label = new TimeSeriesLabel("key", "MRangeReduce");
                ts.Create(key, labels: new List<TimeSeriesLabel> { label });
            }

            var tuples = CreateData(ts, 50);
            var results = ts.MRange("-", "+", new List<string> { "key=MRangeReduce" }, withLabels: true, groupbyTuple: ("key", TsReduce.Sum));
            Assert.Equal(1, results.Count);
            Assert.Equal("key=MRangeReduce", results[0].key);
            Assert.Equal(new TimeSeriesLabel("key", "MRangeReduce"), results[0].labels[0]);
            Assert.Equal(new TimeSeriesLabel("__reducer__", "sum"), results[0].labels[1]);
            Assert.Equal(new TimeSeriesLabel("__source__", string.Join(",", _keys)), results[0].labels[2]);
            for (int i = 0; i < results[0].values.Count; i++)
            {
                Assert.Equal(tuples[i].Val * 2, results[0].values[i].Val);
            }
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeFilterBy()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeSeriesLabel label = new TimeSeriesLabel("key", "MRangeFilterBy");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in _keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, 50);
            var results = ts.MRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(tuples.GetRange(0, 3), results[i].values);
            }

            results = ts.MRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByTs: new List<TimeStamp> { 0 }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(tuples.GetRange(0, 1), results[i].values);
            }
        }

        [SkipIfRedis(Is.StandaloneOSSCluster, Is.Enterprise)]
        public void TestMRangeLatest()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var label = new TimeSeriesLabel("key", "MRangeLatest");
            var compactedLabel = new TimeSeriesLabel("compact", "true");
            string primaryTsKey = _keys[0], compactedTsKey = _keys[1];
            var compactionRule = new TimeSeriesRule(
                compactedTsKey,
                (long)TimeSpan.FromHours(1).TotalMilliseconds, // 1h used to force partial bucket
                TsAggregation.Sum);

            ts.Create(primaryTsKey, labels: new[] { label });
            ts.Create(compactedTsKey, labels: new[] { label, compactedLabel });
            ts.CreateRule(primaryTsKey, compactionRule);
            var tuples = CreateData(ts, 50, new[] { primaryTsKey });

            var results = ts.MRange("-", "+", new[] { "key=MRangeLatest", "compact=true" }, latest: true);
            Assert.Single(results);
            Assert.Equal(compactedTsKey, results[0].key);
            Assert.NotEmpty(results[0].values);
            Assert.Equal(tuples.Sum(x => x.Val), results[0].values.Sum(x => x.Val));

            results = ts.MRange("-", "+", new[] { "key=MRangeLatest", "compact=true" }, latest: false);
            Assert.Single(results);
            Assert.Equal(compactedTsKey, results[0].key);
            Assert.Empty(results[0].values);
        }
    }
}