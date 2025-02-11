using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Literals.Enums;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestCreate : AbstractNRedisStackTest, IDisposable
    {
        private readonly string key = "CREATE_TESTS";

        public TestCreate(EndpointsFixture endpointsFixture) : base(endpointsFixture)
        {
        }


        [Fact]
        [Obsolete]
        public void TestCreateOK()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key));
            TimeSeriesInformation info = ts.Info(key);
        }

        [Fact]
        [Obsolete]
        public void TestCreateRetentionTime()
        {
            long retentionTime = 5000;
            IDatabase db = GetCleanDatabase();
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
            IDatabase db = GetCleanDatabase();
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
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key, labels: labels));
            TimeSeriesInformation info = ts.Info(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public void TestCreateUncompressed()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key, uncompressed: true));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyFirst()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.FIRST));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyLast()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.LAST));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyMin()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.MIN));
        }

        [Fact]
        public void TestCreatehDuplicatePolicyMax()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.MAX));
        }

        [Fact]
        public void TestCreatehDuplicatePolicySum()
        {
            IDatabase db = GetCleanDatabase();
            var ts = db.TS();
            Assert.True(ts.Create(key, duplicatePolicy: TsDuplicatePolicy.SUM));
        }

        [SkipIfRedis(Comparison.LessThan, "7.4.0")]
        [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
        public void TestCreateAndIgnoreValues(string endpointId)
        {
            IDatabase db = GetCleanDatabase(endpointId);
            var ts = db.TS();
            var parameters = new TsCreateParamsBuilder().AddIgnoreValues(11, 12).build();
            Assert.True(ts.Create(key, parameters));

            int j = -1, k = -1;
            RedisResult info = TimeSeriesHelper.getInfo(db, key, out j, out k);

            Assert.NotEqual(-1, j);
            Assert.NotEqual(-1, k);
            Assert.Equal(11, (long)info[j + 1]);
            Assert.Equal(12, (long)info[k + 1]);
        }

        [Fact]
        public void TestParamsBuilder()
        {
            TsCreateParams parameters = new TsCreateParamsBuilder()
                        .AddChunkSizeBytes(1000)
                        .AddDuplicatePolicy(TsDuplicatePolicy.FIRST)
                        .AddIgnoreValues(11, 12)
                        .AddLabels(new List<TimeSeriesLabel>() { new TimeSeriesLabel("key", "value") })
                        .AddRetentionTime(5000)
                        .AddUncompressed(true).build();

            var command = TimeSeriesCommandsBuilder.Create(key, parameters);
            var expectedArgs = new object[] { key, "RETENTION", 5000L, "CHUNK_SIZE", 1000L, "LABELS", "key", "value", "UNCOMPRESSED", "DUPLICATE_POLICY", "FIRST", "IGNORE", 11L, 12L };
            Assert.Equal(expectedArgs, command.Args);

            parameters = new TsCreateParamsBuilder()
                        .AddChunkSizeBytes(1000)
                        .AddDuplicatePolicy(TsDuplicatePolicy.FIRST)
                        .AddIgnoreValues(11, 12)
                        .AddLabels(new List<TimeSeriesLabel>() { new TimeSeriesLabel("key", "value") })
                        .AddRetentionTime(5000)
                        .AddUncompressed(false).build();

            command = TimeSeriesCommandsBuilder.Create(key, parameters);
            expectedArgs = new object[] { key, "RETENTION", 5000L, "CHUNK_SIZE", 1000L, "LABELS", "key", "value", "COMPRESSED", "DUPLICATE_POLICY", "FIRST", "IGNORE", 11L, 12L };
            Assert.Equal(expectedArgs, command.Args);
        }
    }
}
