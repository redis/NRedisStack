#pragma  warning disable CS0618, CS0612 // allow testing obsolete methods
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMRevRange : AbstractNRedisStackTest
    {
        public TestMRevRange(EndpointsFixture endpointsFixture) : base(endpointsFixture)
        {
        }

        private List<TimeSeriesTuple> CreateData(ITimeSeriesCommands ts, string[] keys, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();

            for (var i = 0; i < 10; i++)
            {
                var timeStamp = new TimeStamp(i * timeBucket);
                foreach (var key in keys)
                {
                    ts.Add(key, timeStamp, i);
                }
                tuples.Add(new TimeSeriesTuple(timeStamp, i));
            }
            return tuples;
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestSimpleMRevRange(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { $"{keys[0]}=value" });
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Empty(results[i].labels);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeWithLabels(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true);

            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels, results[i].labels);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeSelectLabels(string endpointId)
        {
            var keys = CreateKeyNames(2);
            IDatabase db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            TimeSeriesLabel label1 = new TimeSeriesLabel("key", "MRangeSelectLabels");
            TimeSeriesLabel[] labels = new TimeSeriesLabel[] { new TimeSeriesLabel("team", "CTO"), new TimeSeriesLabel("team", "AUT") };
            for (int i = 0; i < keys.Length; i++)
            {
                ts.Create(keys[i], labels: new List<TimeSeriesLabel> { label1, labels[i] });
            }

            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { "key=MRangeSelectLabels" }, selectLabels: new List<string> { "team" });
            Assert.Equal(keys.Length, results.Count);
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Equal(labels[i], results[i].labels[0]);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeFilter(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            ts.Create(keys[0], labels: labels);
            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { $"{keys[0]}=value" });
            Assert.Single(results);
            Assert.Equal(keys[0], results[0].key);
            Assert.Empty(results[0].labels);
            Assert.Equal(ReverseData(tuples), results[0].values);
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeCount(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, keys, 50);
            var count = 5L;
            var results = ts.MRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, count: count);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Empty(results[i].labels);
                Assert.Equal(ReverseData(tuples).GetRange(0, (int)count), results[i].values);
            }
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeAggregation(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, aggregation: TsAggregation.Min, timeBucket: 50);
            Assert.Equal(keys.Length, results.Count);
            for (var i = 0; i < results.Count; i++)
            {
                Assert.Equal(keys[i], results[i].key);
                Assert.Empty(results[i].labels);
                Assert.Equal(ReverseData(tuples), results[i].values);
            }
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeAlign(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            ts.Create(keys[0], labels: labels);
            CreateData(ts, keys, 50);
            var expected = new List<TimeSeriesTuple> {
                new TimeSeriesTuple(450,1),
                new TimeSeriesTuple(400,1),
                new TimeSeriesTuple(350,1)
            };
            var results = ts.MRevRange(0, "+", new List<string> { $"{keys[0]}=value" }, align: "-", aggregation: TsAggregation.Count, timeBucket: 10, count: 3);
            Assert.Single(results);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(expected, results[0].values);
            results = ts.MRevRange(0, 500, new List<string> { $"{keys[0]}=value" }, align: "+", aggregation: TsAggregation.Count, timeBucket: 10, count: 1);
            Assert.Equal(expected[0], results[0].values[0]);
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMissingFilter(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, keys, 50);
            var ex = Assert.Throws<ArgumentException>(() => ts.MRevRange("-", "+", new List<string>()));
            Assert.Equal("There should be at least one filter on MRANGE/MREVRANGE", ex.Message);
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMissingTimeBucket(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (var key in keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, keys, 50);
            var ex = Assert.Throws<ArgumentException>(() => ts.MRevRange("-", "+", new List<string> { "key=MissingTimeBucket" }, aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeGroupby(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            for (int i = 0; i < keys.Length; i++)
            {
                var label1 = new TimeSeriesLabel(keys[0], "value");
                var label2 = new TimeSeriesLabel("group", i.ToString());
                ts.Create(keys[i], labels: new List<TimeSeriesLabel> { label1, label2 });
            }

            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: ("group", TsReduce.Min));
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

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeReduce(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            foreach (var key in keys)
            {
                var label = new TimeSeriesLabel(keys[0], "value");
                ts.Create(key, labels: new List<TimeSeriesLabel> { label });
            }

            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { $"{keys[0]}=value" }, withLabels: true, groupbyTuple: (keys[0], TsReduce.Sum));
            Assert.Single(results);
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

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public void TestMRevRangeFilterBy(string endpointId)
        {
            var keys = CreateKeyNames(2);
            var db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var label = new TimeSeriesLabel(keys[0], "value");
            var labels = new List<TimeSeriesLabel> { label };
            foreach (string key in keys)
            {
                ts.Create(key, labels: labels);
            }

            var tuples = CreateData(ts, keys, 50);
            var results = ts.MRevRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(ReverseData(tuples.GetRange(0, 3)), results[i].values);
            }

            results = ts.MRevRange("-", "+", new List<string> { "key=MRangeFilterBy" }, filterByTs: new List<TimeStamp> { 0 }, filterByValue: (0, 2));
            for (int i = 0; i < results.Count; i++)
            {
                Assert.Equal(ReverseData(tuples.GetRange(0, 1)), results[i].values);
            }
        }
    }
}