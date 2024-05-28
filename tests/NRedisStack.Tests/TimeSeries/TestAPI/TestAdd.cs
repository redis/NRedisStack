using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Literals.Enums;
using Xunit;
namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestAdd : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "ADD_TESTS";

        public TestAdd(RedisFixture redisFixture) : base(redisFixture) { }


        [Fact]
        [Obsolete]
        public void TestAddNotExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        [Obsolete]
        public void TestAddExistingTimeSeries()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            ts.Create(key);
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        [Obsolete]
        public void TestAddStar()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            ts.Add(key, "*", 1.1);
            TimeSeriesInformation info = ts.Info(key);
            Assert.True(info.FirstTimeStamp! > 0);
            Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
        }

        [Fact]
        [Obsolete]
        public void TestAddWithRetentionTime()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            long retentionTime = 5000;
            Assert.Equal(now, ts.Add(key, now, 1.1, retentionTime: retentionTime));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public void TestAddWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.Equal(now, ts.Add(key, now, 1.1, labels: labels));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        [Obsolete]
        public void TestAddWithUncompressed()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create(key);
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1, uncompressed: true));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
        }

        [Fact]
        [Obsolete]
        public void TestAddWithChunkSize()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1, chunkSizeBytes: 128));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(now, info.FirstTimeStamp);
            Assert.Equal(now, info.LastTimeStamp);
            Assert.Equal(128, info.ChunkSize);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyBlock()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));
            Assert.Throws<RedisServerException>(() => ts.Add(key, now, 1.2));
        }

        [Fact]
        public void TestAddWithDuplicatePolicyMin()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));

            // Insert a bigger number and check that it did not change the value.
            Assert.Equal(now, ts.Add(key, now, 1.2, duplicatePolicy: TsDuplicatePolicy.MIN));
            Assert.Equal(1.1, ts.Range(key, now, now)[0].Val);
            // Insert a smaller number and check that it changed.
            Assert.Equal(now, ts.Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.MIN));
            Assert.Equal(1.0, ts.Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyMax()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));

            // Insert a smaller number and check that it did not change the value.
            Assert.Equal(now, ts.Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.MAX));
            Assert.Equal(1.1, ts.Range(key, now, now)[0].Val);
            // Insert a bigger number and check that it changed.
            Assert.Equal(now, ts.Add(key, now, 1.2, duplicatePolicy: TsDuplicatePolicy.MAX));
            Assert.Equal(1.2, ts.Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicySum()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));
            Assert.Equal(now, ts.Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.SUM));
            Assert.Equal(2.1, ts.Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyFirst()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));
            Assert.Equal(now, ts.Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.FIRST));
            Assert.Equal(1.1, ts.Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestAddWithDuplicatePolicyLast()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp now = DateTime.UtcNow;
            Assert.Equal(now, ts.Add(key, now, 1.1));
            Assert.Equal(now, ts.Add(key, now, 1.0, duplicatePolicy: TsDuplicatePolicy.LAST));
            Assert.Equal(1.0, ts.Range(key, now, now)[0].Val);
        }

        [Fact]
        public void TestOldAdd()
        {
            TimeStamp old_dt = DateTime.UtcNow;
            Thread.Sleep(1000);
            TimeStamp new_dt = DateTime.UtcNow;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Create(key);
            ts.Add(key, new_dt, 1.1);
            // Adding old event
            var res = ts.Add(key, old_dt, 1.1);
            Assert.Equal(old_dt.Value, res.Value);
        }

        [Fact]
        public void TestWrongParameters()
        {
            double value = 1.1;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var ex = Assert.Throws<RedisServerException>(() => ts.Add(key, "+", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
            ex = Assert.Throws<RedisServerException>(() => ts.Add(key, "-", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }

        [Fact]
        public void TestAddAndIgnoreValues()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var parameters = new TsAddParamsBuilder().AddTimestamp(101).AddValue(102).AddIgnoreValues(15, 16).build();
            ts.Add(key, parameters);

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
