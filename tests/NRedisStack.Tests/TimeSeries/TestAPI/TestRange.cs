using System;
using System.Collections.Generic;
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
            redisFixture.Redis.GetDatabase().KeyDelete(key);
        }

        private List<TimeSeriesTuple> CreateData(IDatabase db, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (int i = 0; i < 10; i++)
            {
                TimeStamp ts = db.TS().Add(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples, db.TS().Range(key, "-", "+"));
        }

        [Fact]
        public void TestRangeCount()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples.GetRange(0, 5), db.TS().Range(key, "-", "+", count: 5));
        }

        [Fact]
        public void TestRangeAggregation()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, 50);
            Assert.Equal(tuples, db.TS().Range(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public void TestRangeAlign()
        {
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
                db.TS().Add(key, tuple.Time, tuple.Val);
            }

            // Aligh start
            var resStart = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(1, 2),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(21, 1)
            };
            Assert.Equal(resStart, db.TS().Range(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(0, 2),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(20, 1)
            };
            Assert.Equal(resEnd, db.TS().Range(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, db.TS().Range(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TS().Range(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public void TestFilterBy()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, 50);

            var res = db.TS().Range(key, "-", "+", filterByValue: (0, 2)); // The first 3 tuples
            Assert.Equal(3, res.Count);
            Assert.Equal(tuples.GetRange(0, 3), res);

            var filterTs = new List<TimeStamp> { 0, 50, 100 }; // Also the first 3 tuples
            res = db.TS().Range(key, "-", "+", filterByTs: filterTs);
            Assert.Equal(tuples.GetRange(0, 3), res);

            res = db.TS().Range(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5)); // The third tuple
            Assert.Equal(tuples.GetRange(2, 1), res);
        }

        [Fact]
        public void latest()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Create("ts1");
            db.TS().Create("ts2");
            db.TS().CreateRule("ts1", new TimeSeriesRule("ts2", 10, TsAggregation.Sum));
            db.TS().Add("ts1", 1, 1);
            db.TS().Add("ts1", 2, 3);
            db.TS().Add("ts1", 11, 7);
            db.TS().Add("ts1", 13, 1);
            var range = db.TS().Range("ts1", 0, 20);
            Assert.Equal(4, range.Count);

            var compact = new TimeSeriesTuple(0, 4);
            var latest = new TimeSeriesTuple(10, 8);

            // get
            Assert.Equal(compact, db.TS().Get("ts2"));

            Assert.Equal(latest, db.TS().Get("ts2", true));

            // range
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, db.TS().Range("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { compact, latest }, db.TS().Range("ts2", 0, 10, true));

            // revrange
            Assert.Equal(new List<TimeSeriesTuple>() { compact }, db.TS().RevRange("ts2", 0, 10));

            Assert.Equal(new List<TimeSeriesTuple>() { latest, compact }, db.TS().RevRange("ts2", 0, 10, true));
        }

        [Fact]
        public void TestAlignTimestamp()
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
            Assert.Equal(2, db.TS().Range("ts2", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10).Count);
            Assert.Equal(1, db.TS().Range("ts3", "-", "+", aggregation: TsAggregation.Count, timeBucket: 10).Count);
        }
    }
}