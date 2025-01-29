using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRangeAsync : AbstractNRedisStackTest
    {
        public TestRangeAsync(EndpointsFixture endpointsFixture) : base(endpointsFixture)
        {
        }

        private async Task<List<TimeSeriesTuple>> CreateData(TimeSeriesCommands ts, string key, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (var i = 0; i < 10; i++)
            {
                var timeStamp = await ts.AddAsync(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(timeStamp, i));
            }
            return tuples;
        }

        [Fact]
        public async Task TestSimpleRange()
        {
            var key = CreateKeyName();
            var db = GetCleanDatabase();
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            Assert.Equal(tuples, await ts.RangeAsync(key, "-", "+"));
        }

        [Fact]
        public async Task TestRangeCount()
        {
            var key = CreateKeyName();
            var db = GetCleanDatabase();
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            Assert.Equal(tuples.GetRange(0, 5), await ts.RangeAsync(key, "-", "+", count: 5));
        }

        [Fact]
        public async Task TestRangeAggregation()
        {
            var key = CreateKeyName();
            var db = GetCleanDatabase();
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            Assert.Equal(tuples, await ts.RangeAsync(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public async Task TestRangeAlign()
        {
            var key = CreateKeyName();
            IDatabase db = GetCleanDatabase();
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
                await ts.AddAsync(key, tuple.Time, tuple.Val);
            }

            // Aligh start
            var resStart = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 2),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(21, 1)
            };
            Assert.Equal(resStart, await ts.RangeAsync(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(0, 2),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(20, 1)
            };
            Assert.Equal(resEnd, await ts.RangeAsync(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, await ts.RangeAsync(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public async Task TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = GetCleanDatabase();
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await ts.RangeAsync(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public async Task TestFilterBy()
        {
            var key = CreateKeyName();
            var db = GetCleanDatabase();
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);

            var res = await ts.RangeAsync(key, "-", "+", filterByValue: (0, 2)); // The first 3 tuples
            Assert.Equal(3, res.Count);
            Assert.Equal(tuples.GetRange(0, 3), res);

            var filterTs = new List<TimeStamp> { 0, 50, 100 }; // Also the first 3 tuples
            res = await ts.RangeAsync(key, "-", "+", filterByTs: filterTs);
            Assert.Equal(tuples.GetRange(0, 3), res);

            res = await ts.RangeAsync(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5)); // The third tuple
            Assert.Equal(tuples.GetRange(2, 1), res);
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public async Task TestLatestAsync(string endpointId)
        {
            IDatabase db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            await ts.CreateAsync("ts1");
            await ts.CreateAsync("ts2");
            await ts.CreateRuleAsync("ts1", new TimeSeriesRule("ts2", 10, TsAggregation.Sum));
            await ts.AddAsync("ts1", 1, 1);
            await ts.AddAsync("ts1", 2, 3);
            await ts.AddAsync("ts1", 11, 7);
            await ts.AddAsync("ts1", 13, 1);
            var range = await ts.RangeAsync("ts1", 0, 20);
            Assert.Equal(4, range.Count);

            var compact = new TimeSeriesTuple(0, 4);
            var latest = new TimeSeriesTuple(10, 8);

            // get
            Assert.Equal(compact, await ts.GetAsync("ts2"));

            Assert.Equal(latest, await ts.GetAsync("ts2", true));

            // range
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, await ts.RangeAsync("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { compact, latest }, await ts.RangeAsync("ts2", 0, 10, true));

            // revrange
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, await ts.RevRangeAsync("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { latest, compact }, await ts.RevRangeAsync("ts2", 0, 10, true));
        }

        [SkipIfRedis(Is.Enterprise)]
        [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
        public async Task TestAlignTimestampAsync(string endpointId)
        {
            IDatabase db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            ts.Create("ts1");
            ts.Create("ts2");
            ts.Create("ts3");
            ts.CreateRule("ts1", new TimeSeriesRule("ts2", 10, TsAggregation.Count), 0);
            ts.CreateRule("ts1", new TimeSeriesRule("ts3", 10, TsAggregation.Count), 1);
            ts.Add("ts1", 1, 1);
            ts.Add("ts1", 10, 3);
            ts.Add("ts1", 21, 7);
            Assert.Equal(2, (await ts.RangeAsync("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)).Count);
            Assert.Single((await ts.RangeAsync("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10)));
        }

        [Fact]
        public async Task TestBucketTimestampAsync()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();

            ts.Create("t1");

            ts.Add("t1", 15, 1);
            ts.Add("t1", 17, 4);
            ts.Add("t1", 51, 3);
            ts.Add("t1", 73, 5);
            ts.Add("t1", 75, 3);

            var rangeHigh = await ts.RangeAsync("t1", 0, 100,
                                      align: 0,
                                      aggregation: TsAggregation.Max,
                                      timeBucket: 10);

            var expected = new List<TimeSeriesTuple>();
            expected.Add(new TimeSeriesTuple(10, 4.0));
            expected.Add(new TimeSeriesTuple(50, 3.0));
            expected.Add(new TimeSeriesTuple(70, 5.0));
            Assert.Equal(rangeHigh, expected);

            rangeHigh = await ts.RangeAsync("t1", 0, 100,
                                  align: 0,
                                  aggregation: TsAggregation.Max,
                                  timeBucket: 10,
                                  bt: TsBucketTimestamps.high);

            expected.Clear();
            expected.Add(new TimeSeriesTuple(20, 4.0));
            expected.Add(new TimeSeriesTuple(60, 3.0));
            expected.Add(new TimeSeriesTuple(80, 5.0));
            Assert.Equal(rangeHigh, expected);

            var rangeLow = await ts.RangeAsync("t1", 0, 100,
                                  align: 0,
                                  aggregation: TsAggregation.Max,
                                  timeBucket: 10,
                                  bt: TsBucketTimestamps.low);
            expected.Clear();
            expected.Add(new TimeSeriesTuple(10, 4.0));
            expected.Add(new TimeSeriesTuple(50, 3.0));
            expected.Add(new TimeSeriesTuple(70, 5.0));
            Assert.Equal(rangeLow, expected);

            var rangeMid = await ts.RangeAsync("t1", 0, 100,
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
        public async Task TestEmptyAsync()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();

            ts.Create("t1");

            ts.Add("t1", 15, 1);
            ts.Add("t1", 17, 4);
            ts.Add("t1", 51, 3);
            ts.Add("t1", 73, 5);
            ts.Add("t1", 75, 3);

            var range = await ts.RangeAsync("t1", 0, 100,
                                      align: 0,
                                      aggregation: TsAggregation.Max,
                                      timeBucket: 10);

            var expected = new List<TimeSeriesTuple>();
            expected.Add(new TimeSeriesTuple(10, 4.0));
            expected.Add(new TimeSeriesTuple(50, 3.0));
            expected.Add(new TimeSeriesTuple(70, 5.0));
            Assert.Equal(range, expected);

            range = await ts.RangeAsync("t1", 0, 100,
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