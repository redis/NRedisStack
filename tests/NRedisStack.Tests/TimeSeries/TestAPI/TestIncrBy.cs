using NRedisStack.RedisStackCommands;
using NRedisStack.DataTypes;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestIncrBy(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture), IDisposable
    {
        private readonly string key = "INCRBY_TESTS";

        [Fact]
        public void TestDefaultIncrBy()
        {
            double value = 5.5;
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.IncrBy(key, value) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
        }

        [Fact]
        public void TestStarIncrBy()
        {
            double value = 5.5;
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.IncrBy(key, value, timestamp: "*") > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
        }

        [Fact]
        public void TestIncrByTimeStamp()
        {
            double value = 5.5;
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, ts.IncrBy(key, value, timestamp: timeStamp));
            Assert.Equal(new TimeSeriesTuple(timeStamp, value), ts.Get(key));
        }

        [Fact]
        [Obsolete]
        public void TestDefaultIncrByWithRetentionTime()
        {
            double value = 5.5;
            long retentionTime = 5000;
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.IncrBy(key, value, retentionTime: retentionTime) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public void TestDefaultIncrByWithLabels()
        {
            double value = 5.5;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            var labels = new List<TimeSeriesLabel> { label };
            Assert.True(ts.IncrBy(key, value, labels: labels) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestDefaultIncrByWithUncompressed()
        {
            double value = 5.5;
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.IncrBy(key, value, uncompressed: true) > 0);
            Assert.Equal(value, ts.Get(key)!.Val);
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 5.5;
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            var ex = Assert.Throws<RedisServerException>(() => ts.IncrBy(key, value, timestamp: "+"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => ts.IncrBy(key, value, timestamp: "-"));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }

        [SkipIfRedis(Comparison.LessThan, "7.4.0")]
        [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
        public async void TestIncrByAndIgnoreValues(string endpointId)
        {
            IDatabase db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var incrParameters = new TsIncrByParamsBuilder().AddValue(1).AddIgnoreValues(15, 16).build();
            ts.IncrBy(key, incrParameters);

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
