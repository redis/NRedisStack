using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestDecrBy : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "DECRBY_TESTS";

        public TestDecrBy(RedisFixture redisFixture) : base(redisFixture) { }


        [Fact]
        public void TestDefaultDecrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.DecrBy(key, -value) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
        }

        [Fact]
        public void TestStarDecrBy()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.DecrBy(key, -value, timestamp: "*") > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
        }

        [Fact]
        public void TestDecrByTimeStamp()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, ts.DecrBy(key, -value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), ts.Get(key));
        }

        [Fact]
        [Obsolete]
        public void TestDefaultDecrByWithRetentionTime()
        {
            double value = 5.5;
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.DecrBy(key, -value, retentionTime: retentionTime) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public void TestDefaultDecrByWithLabels()
        {
            double value = 5.5;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var labels = new List<TimeSeriesLabel> { label };
            Assert.True(ts.DecrBy(key, -value, labels: labels) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestDefaultDecrByWithUncompressed()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.DecrBy(key, -value, uncompressed: true) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 5.5;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var ex = Assert.Throws<RedisServerException>(() => ts.DecrBy(key, value, timestamp: "+"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => ts.DecrBy(key, value, timestamp: "-"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }

        [SkipIfRedis(Comparison.LessThan, "7.4.0")]
        public async void TestIncrDecryByAndIgnoreValues()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var decrParameters = new TsDecrByParamsBuilder().AddValue(1).AddIgnoreValues(15, 16).build();
            ts.DecrBy(key, decrParameters);

            int j = -1, k = -1;
            RedisResult info = TimeSeriesHelper.getInfo(db, key, out j, out k);
            Assert.NotNull(info);
            Assert.True(info.Length > 0);
            Assert.NotEqual(j, -1);
            Assert.NotEqual(k, -1);
            Assert.Equal(15, (long)info[j + 1]);
            Assert.Equal(16, (long)info[k + 1]);
        }
    }
}
