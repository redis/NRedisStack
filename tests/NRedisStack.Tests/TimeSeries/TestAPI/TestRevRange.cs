using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRevRange : AbstractNRedisStackTest
    {
        public TestRevRange(RedisFixture redisFixture) : base(redisFixture) { }

        private List<TimeSeriesTuple> CreateData(ITimeSeriesCommands ts, string key, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (var i = 0; i < 10; i++)
            {
                var timeStamp = ts.Add(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(timeStamp, i));
            }
            return tuples;
        }

        [SkipIfRedis(Is.Enterprise)]
        public void TestSimpleRevRange()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var tuples = CreateData(ts, key, 50);
            Assert.Equal(ReverseData(tuples), ts.RevRange(key, "-", "+"));
        }

        [SkipIfRedis(Is.Enterprise)]
        public void TestRevRangeCount()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var tuples = CreateData(ts, key, 50);
            Assert.Equal(ReverseData(tuples).GetRange(0, 5), ts.RevRange(key, "-", "+", count: 5));
        }

        [SkipIfRedis(Is.Enterprise)]
        public void TestRevRangeAggregation()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var tuples = CreateData(ts, key, 50);
            Assert.Equal(ReverseData(tuples), ts.RevRange(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [SkipIfRedis(Is.Enterprise)]
        public void TestRevRangeAlign()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
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
                new TimeSeriesTuple(21, 1),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(1, 2)
            };
            Assert.Equal(resStart, ts.RevRange(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(20, 1),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(0, 2)
            };
            Assert.Equal(resEnd, ts.RevRange(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, ts.RevRange(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [SkipIfRedis(Is.Enterprise)]
        public void TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var tuples = CreateData(ts, key, 50);
            var ex = Assert.Throws<ArgumentException>(() => ts.RevRange(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }

        [SkipIfRedis(Is.Enterprise)]
        public void TestFilterBy()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.FlushAll();
            var ts = db.TS();
            var tuples = CreateData(ts, key, 50);

            var res = ts.RevRange(key, "-", "+", filterByValue: (0, 2));
            Assert.Equal(3, res.Count);
            Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

            var filterTs = new List<TimeStamp> { 0, 50, 100 };
            res = ts.RevRange(key, "-", "+", filterByTs: filterTs);
            Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

            res = ts.RevRange(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
            Assert.Equal(tuples.GetRange(2, 1), res);
        }
    }
}
