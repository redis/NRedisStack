using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using Xunit;


namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestDel : AbstractTimeSeriesTest, IDisposable
    {
        private readonly string key = "DEL_TESTS";

        public TestDel(RedisFixture redisFixture) : base(redisFixture) { }

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
        public void TestDelNotExists()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ex = Assert.Throws<RedisServerException>(() => db.TS().Del(key, "-", "+"));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public void TestDelRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var tuples = CreateData(db, 50);
            TimeStamp from = tuples[0].Time;
            TimeStamp to = tuples[5].Time;
            Assert.Equal(6, db.TS().Del(key, from, to));

            // check that the operation deleted the timestamps
            IReadOnlyList<TimeSeriesTuple> res = db.TS().Range(key, from, to);
            Assert.Equal(0, res.Count);
            Assert.NotNull(db.TS().Get(key));
        }
    }
}
