using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRange : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "RANGE_TESTS";

        public TestRange(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().ExecuteBroadcast("FLUSHALL");
        }

        private List<TimeSeriesTuple> CreateData(ITimeSeriesCommands ts, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TimeStamp timeStamp = ts.Add(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(timeStamp, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = CreateData(ts, 50);
            Assert.Equal(tuples, ts.Range(key, "-", "+"));
        }

        [Fact]
        public void TestRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = CreateData(ts, 50);
            Assert.Equal(tuples.GetRange(0, 5), ts.Range(key, "-", "+", count: 5));
        }

        [Fact]
        public void TestRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = CreateData(ts, 50);
            Assert.Equal(tuples, ts.Range(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public void TestRangeAlign()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 10),
                new TimeSeriesTuple(3, 5),
                new TimeSeriesTuple(11, 10),
                new TimeSeriesTuple(21, 11)
            };

            foreach (var tuple in tuples)
            {
                ts.Add(key, tuple.Time, tuple.Val);
            }

            // Aligh start
            var resStart = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 2),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(21, 1)
            };
            Assert.Equal(resStart, ts.Range(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(0, 2),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(20, 1)
            };
            Assert.Equal(resEnd, ts.Range(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, ts.Range(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = CreateData(ts, 50);
            var ex = Assert.Throws<ArgumentException>(() => ts.Range(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public void TestFilterBy()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = CreateData(ts, 50);

            var res = ts.Range(key, "-", "+", filterByValue: (0, 2)); // The first 3 tuples
            Assert.Equal(3, res.Count);
            Assert.Equal(tuples.GetRange(0, 3), res);

            var filterTs = new List<TimeStamp> { 0, 50, 100 }; // Also the first 3 tuples
            res = ts.Range(key, "-", "+", filterByTs: filterTs);
            Assert.Equal(tuples.GetRange(0, 3), res);

            res = ts.Range(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5)); // The third tuple
            Assert.Equal(tuples.GetRange(2, 1), res);
        }

        [SkipIfRedis(Is.OSSCluster)]
        public void latest()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create("ts1");
            ts.Create("ts2");
            ts.CreateRule("ts1", new TimeSeriesRule("ts2", 10, TsAggregation.Sum));
            ts.Add("ts1", 1, 1);
            ts.Add("ts1", 2, 3);
            ts.Add("ts1", 11, 7);
            ts.Add("ts1", 13, 1);
            var range = ts.Range("ts1", 0, 20);
            Assert.Equal(4, range.Count);

            var compact = new TimeSeriesTuple(0, 4);
            var latest = new TimeSeriesTuple(10, 8);

            // get
            Assert.Equal(compact, ts.Get("ts2"));

            Assert.Equal(latest, ts.Get("ts2", true));

            // range
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, ts.Range("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { compact, latest }, ts.Range("ts2", 0, 10, true));

            // revrange
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, ts.RevRange("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { latest, compact }, ts.RevRange("ts2", 0, 10, true));
        }

        [SkipIfRedis(Is.OSSCluster)]
        public void TestAlignTimestamp()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create("ts1");
            ts.Create("ts2");
            ts.Create("ts3");
            ts.CreateRule("ts1", new TimeSeriesRule("ts2", 10, TsAggregation.Count), 0);
            ts.CreateRule("ts1", new TimeSeriesRule("ts3", 10, TsAggregation.Count), 1);
            ts.Add("ts1", 1, 1);
            ts.Add("ts1", 10, 3);
            ts.Add("ts1", 21, 7);
            Assert.Equal(2, ts.Range("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10).Count);
            Assert.Equal(1, ts.Range("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10).Count);
        }

        [Fact]
        public void TestBucketTimestamp()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            ts.Create("t1");

            ts.Add("t1", 15, 1);
            ts.Add("t1", 17, 4);
            ts.Add("t1", 51, 3);
            ts.Add("t1", 73, 5);
            ts.Add("t1", 75, 3);

            var rangeHigh = ts.Range("t1", 0, 100,
                                      align: 0,
                                      aggregation: TsAggregation.Max,
                                      timeBucket: 10);

            var expected = new List<TimeSeriesTuple>();
            expected.Add(new TimeSeriesTuple(10, 4.0));
            expected.Add(new TimeSeriesTuple(50, 3.0));
            expected.Add(new TimeSeriesTuple(70, 5.0));
            Assert.Equal(rangeHigh, expected);

            rangeHigh = ts.Range("t1", 0, 100,
                                  align: 0,
                                  aggregation: TsAggregation.Max,
                                  timeBucket: 10,
                                  bt: TsBucketTimestamps.high);
            expected.Clear();
            expected.Add(new TimeSeriesTuple(20, 4.0));
            expected.Add(new TimeSeriesTuple(60, 3.0));
            expected.Add(new TimeSeriesTuple(80, 5.0));
            Assert.Equal(rangeHigh, expected);

            var rangeLow = ts.Range("t1", 0, 100,
                                  align: 0,
                                  aggregation: TsAggregation.Max,
                                  timeBucket: 10,
                                  bt: TsBucketTimestamps.low);
            expected.Clear();
            expected.Add(new TimeSeriesTuple(10, 4.0));
            expected.Add(new TimeSeriesTuple(50, 3.0));
            expected.Add(new TimeSeriesTuple(70, 5.0));
            Assert.Equal(rangeLow, expected);

            var rangeMid = ts.Range("t1", 0, 100,
                                  align: 0,
                                  aggregation: TsAggregation.Max,
                                  timeBucket: 10,
                                  bt: TsBucketTimestamps.mid);
            expected.Clear();
            expected.Add(new TimeSeriesTuple(15, 4.0));
            expected.Add(new TimeSeriesTuple(55, 3.0));
            expected.Add(new TimeSeriesTuple(75, 5.0));
            Assert.Equal(rangeMid, expected);
        }

        [Fact]
        public void TestEmpty()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            ts.Create("t1");

            ts.Add("t1", 15, 1);
            ts.Add("t1", 17, 4);
            ts.Add("t1", 51, 3);
            ts.Add("t1", 73, 5);
            ts.Add("t1", 75, 3);

            var range = ts.Range("t1", 0, 100,
                                      align: 0,
                                      aggregation: TsAggregation.Max,
                                      timeBucket: 10);

            var expected = new List<TimeSeriesTuple>();
            expected.Add(new TimeSeriesTuple(10, 4.0));
            expected.Add(new TimeSeriesTuple(50, 3.0));
            expected.Add(new TimeSeriesTuple(70, 5.0));
            Assert.Equal(range, expected);

            range = ts.Range("t1", 0, 100,
                                  align: 0,
                                  aggregation: TsAggregation.Max,
                                  timeBucket: 10,
                                  empty: true);

            expected.Clear();

            expected.Add(new TimeSeriesTuple(10, 4.0));
            expected.Add(new TimeSeriesTuple(20, double.NaN));
            expected.Add(new TimeSeriesTuple(30, double.NaN));
            expected.Add(new TimeSeriesTuple(40, double.NaN));
            expected.Add(new TimeSeriesTuple(50, 3.0));
            expected.Add(new TimeSeriesTuple(60, double.NaN));
            expected.Add(new TimeSeriesTuple(70, 5.0));

            for (int i = 0; i < range.Count(); i++)
            {
                Assert.Equal(range[i].Time.Value, expected[i].Time.Value);
                Assert.Equal(range[i].Val, expected[i].Val);
            }
        }
    }
}