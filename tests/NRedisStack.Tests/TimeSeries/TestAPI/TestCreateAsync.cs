using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestCreateAsync : AbstractNRedisStackTest
    {
        public TestCreateAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestCreateOK()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key));
        }

        [Fact]
        [Obsolete]
        public async Task TestCreateRetentionTime()
        {
            var key = CreateKeyName();
            long retentionTime = 5000;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, retentionTime: retentionTime));

            var info = await ts.InfoAsync(key);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        [Obsolete]
        public async Task TestCreateLabels()
        {
            var key = CreateKeyName();
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, labels: labels));

            var info = await ts.InfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        [Obsolete]
        public async Task TestCreateEmptyLabels()
        {
            var key = CreateKeyName();
            var labels = new List<TimeSeriesLabel>();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, labels: labels));

            var info = await ts.InfoAsync(key);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestCreateUncompressed()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, uncompressed: true));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyFirst()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, duplicatePolicy: TsDuplicatePolicy.FIRST));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyLast()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, duplicatePolicy: TsDuplicatePolicy.LAST));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyMin()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, duplicatePolicy: TsDuplicatePolicy.MIN));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicyMax()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, duplicatePolicy: TsDuplicatePolicy.MAX));
        }

        [Fact]
        public async void TestCreatehDuplicatePolicySum()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();
            Assert.True(await ts.CreateAsync(key, duplicatePolicy: TsDuplicatePolicy.SUM));
        }
    }
}
