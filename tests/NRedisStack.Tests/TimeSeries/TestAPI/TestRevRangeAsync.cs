using StackExchange.Redis;
using NRedisStack.Literals.Enums;
using NRedisStack.DataTypes;
using Xunit;
using NRedisStack.RedisStackCommands;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestRevRangeAsync : AbstractNRedisStackTest
    {
        public TestRevRangeAsync(RedisFixture redisFixture) : base(redisFixture) { }

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
        public async Task TestSimpleRevRange()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            Assert.Equal(ReverseData(tuples), await ts.RevRangeAsync(key, "-", "+"));
        }

        [Fact]
        public async Task TestRevRangeCount()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            Assert.Equal(ReverseData(tuples).GetRange(0, 5), await ts.RevRangeAsync(key, "-", "+", count: 5));
        }

        [Fact]
        public async Task TestRevRangeAggregation()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            Assert.Equal(ReverseData(tuples), await ts.RevRangeAsync(key, "-", "+", aggregation: TsAggregation.Min, timeBucket: 50));
        }

        [Fact]
        public async Task TestRevRangeAlign()
        {
            var key = CreateKeyName();
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
                await ts.AddAsync(key, tuple.Time, tuple.Val);
            }

            // Aligh start
            var resStart = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(21, 1),
                new TimeSeriesTuple(11, 1),
                new TimeSeriesTuple(1, 2)
            };
            Assert.Equal(resStart, await ts.RevRangeAsync(key, 1, 30, align: "-", aggregation: TsAggregation.Count, timeBucket: 10));

            // Aligh end
            var resEnd = new List<TimeSeriesTuple>()
            {
                new TimeSeriesTuple(20, 1),
                new TimeSeriesTuple(10, 1),
                new TimeSeriesTuple(0, 2)
            };
            Assert.Equal(resEnd, await ts.RevRangeAsync(key, 1, 30, align: "+", aggregation: TsAggregation.Count, timeBucket: 10));

            // Align 1
            Assert.Equal(resStart, await ts.RevRangeAsync(key, 1, 30, align: 1, aggregation: TsAggregation.Count, timeBucket: 10));
        }

        [Fact]
        public async Task TestMissingTimeBucket()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);
            var ex = await Assert.ThrowsAsync<ArgumentException>(async () => await ts.RevRangeAsync(key, "-", "+", aggregation: TsAggregation.Avg));
            Assert.Equal("RANGE Aggregation should have timeBucket value", ex.Message);
        }

        [Fact]
        public async Task TestFilterBy()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var tuples = await CreateData(ts, key, 50);

            var res = await ts.RevRangeAsync(key, "-", "+", filterByValue: (0, 2));
            Assert.Equal(3, res.Count);
            Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

            var filterTs = new List<TimeStamp> { 0, 50, 100 };
            res = await ts.RevRangeAsync(key, "-", "+", filterByTs: filterTs);
            Assert.Equal(ReverseData(tuples.GetRange(0, 3)), res);

            res = await ts.RevRangeAsync(key, "-", "+", filterByTs: filterTs, filterByValue: (2, 5));
            Assert.Equal(tuples.GetRange(2, 1), res);
        }
    }
}
