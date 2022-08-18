using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestDelAsync : AbstractNRedisStackTest
    {
        public TestDelAsync(RedisFixture redisFixture) : base(redisFixture) { }

        private async Task<List<TimeSeriesTuple>> CreateData(IDatabase db, string key, int timeBucket)
        {
            var tuples = new List<TimeSeriesTuple>();
            for (var i = 0; i < 10; i++)
            {
                var ts = await db.TS().AddAsync(key, i * timeBucket, i);
                tuples.Add(new TimeSeriesTuple(ts, i));
            }
            return tuples;
        }

        [Fact]
        public async Task TestDelNotExists()
        {
            var key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await db.TS().DelAsync(key, "-", "+"));
            Assert.Equal("ERR TSDB: the key does not exist", ex.Message);
        }

        [Fact]
        public async Task TestDelRange()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var key = CreateKeyName();
            var tuples = await CreateData(db, key, 50);
            TimeStamp from = tuples[0].Time;
            TimeStamp to = tuples[5].Time;
            Assert.Equal(6, await db.TS().DelAsync(key, from, to));

            // check that the operation deleted the timestamps
            IReadOnlyList<TimeSeriesTuple> res = await db.TS().RangeAsync(key, from, to);
            Assert.Equal(0, res.Count);
            Assert.NotNull(await db.TS().GetAsync(key));
        }
    }
}
