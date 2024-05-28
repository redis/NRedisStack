using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Literals.Enums;
using Xunit;
using System.Runtime.CompilerServices;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestCreate : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "CREATE_TESTS";

        public TestCreate(RedisFixture redisFixture) : base(redisFixture) { }


        [Fact]
        [Obsolete]
        public void TestCreateOK()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key));
            TimeSeriesInformation info = ts.Info(key);
        }

        [Fact]
        [Obsolete]
        public void TestCreateRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, retentionTime: retentionTime));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public void TestCreateLabels()
        {
            TimeSeriesLabel label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, labels: labels));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        [Obsolete]
        public void TestCreateEmptyLabels()
        {
            var labels = new List<TimeSeriesLabel>();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, labels: labels));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateUncompressed()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, uncompressed: true));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyFirst()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.FIRST));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyLast()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.LAST));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyMin()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.MIN));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyMax()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.MAX));
        }

        [Fact]
        public void TestCreatehDuplicatePolicySum()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.SUM));
        }

        [Fact]
        public void TestCreateAndIgnoreValues()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var parameters = new TsCreateParamsBuilder().AddIgnoreValues(11, 12).build();
            Assert.True(ts.Create(key, parameters));

            int j = -1, k = -1;
            RedisResult info = TimeSeriesHelper.getInfo(db, key, out j, out k);

            Assert.NotEqual(j, -1);
            Assert.NotEqual(k, -1);
            Assert.Equal(11, (long)info[j + 1]);
            Assert.Equal(12, (long)info[k + 1]);
        }
    }
}
