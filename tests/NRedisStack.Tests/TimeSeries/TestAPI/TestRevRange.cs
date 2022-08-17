using System;
using System.Collections.Generic;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRevRange : AbstractTimeSeriesTest
    {
        public TestRevRange(RedisFixture redisFixture) : base(redisFixture) { }

        private List<TimeSeriesTuple> CreateData(IDatabase db, string key, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (var i = 0; i < 10; i++)
            {
                var ts = db.TS().Add(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public void TestSimpleRevRange()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples), db.TS().RevRange(key, "-", "+"));
        }

        [Fact]
        public void TestRevRangeCount()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples).GetRange(0, 5), db.TS().RevRange(key, "-", "+", count: 5));
        }

        [Fact]
        public void TestRevRangeAggregation()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, key, 50);
            Assert.Equal(ReverseData(tuples), db.TS().RevRange(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public void TestRevRangeAlign()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
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
                new TimeSeriesTuple(21, 1),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(1, 2)
            };
            Assert.Equal(resStart, db.TS().RevRange(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(20, 1),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(0, 2)
            };
            Assert.Equal(resEnd, db.TS().RevRange(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, db.TS().RevRange(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public void TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, key, 50);
            var ex = Assert.Throws<ArgumentException>(() => db.TS().RevRange(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);

        }

        [Fact]
        public void TestFilterBy()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, key, 50);

            var res = db.TS().RevRange(key, "-", "+", filterByValue: (0, 2));
            Assert.Equal(3, res.Count);
            Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

            var filterTs = new List<TimeStamp> { 0, 50, 100 };
            res = db.TS().RevRange(key, "-", "+", filterByTs: filterTs);
            Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

            res = db.TS().RevRange(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
            Assert.Equal(tuples.GetRange(2, 1), res);
        }
    }
}
