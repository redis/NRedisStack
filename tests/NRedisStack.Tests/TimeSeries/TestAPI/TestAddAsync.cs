using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestAddAsync : AbstractNRedisStackTest
    {
        public TestAddAsync(RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public async Task TestAddNotExistingTimeSeries()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));

            var info = await ts.InfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }

        [Fact]
        public async Task TestAddExistingTimeSeries()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            await ts.CreateAsync(key);
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));

            var info = await ts.InfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }

        [Fact]
        public async Task TestAddStar()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            await ts.AddAsync(key, "*", 1.1);
            var info = await ts.InfoAsync(key);
            Assert.True(info.FirstTimeStamp > 0);
            Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
        }

        [Fact]
        public async Task TestAddWithRetentionTime()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            long retentionTime = 5000;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1, retentionTime: retentionTime));

            var info = await ts.InfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(retentionTime, info.RetentionTime);
        }

        [Fact]
        public async Task TestAddWithLabels()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            var label = new TimeSeriesLabel("key", "value");
            var labels = new List<TimeSeriesLabel> { label };
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1, labels: labels));

            var info = await ts.InfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(labels, info.Labels);
        }

        [Fact]
        public async Task TestAddWithChunkSize()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1, chunkSizeBytes: 128));
            var info = await ts.InfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
            Assert.Equal(128, info.ChunkSize);
        }

        [Fact]
        public async Task TestAddWithUncompressed()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            await ts.CreateAsync(key);
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1, uncompressed: true));

            var info = await ts.InfoAsync(key);
            Assert.Equal(timeStamp, info.FirstTimeStamp);
            Assert.Equal(timeStamp, info.LastTimeStamp);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyBlock()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));
            await Assert.ThrowsAsync<RedisServerException>(async () => await ts.AddAsync(key, timeStamp, 1.2));
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyMin()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));

            // Insert a bigger number and check that it did not change the value.
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.2, duplicatePolicy: TsDuplicatePolicy.MIN));
            IReadOnlyList<TimeSeriesTuple> results = await ts.RangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.1, results[0].Val);

            // Insert a smaller number and check that it changed.
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.0, duplicatePolicy: TsDuplicatePolicy.MIN));
            results = await ts.RangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.0, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyMax()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));

            // Insert a smaller number and check that it did not change the value.
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.0, duplicatePolicy: TsDuplicatePolicy.MAX));
            IReadOnlyList<TimeSeriesTuple> results = await ts.RangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.1, results[0].Val);
            // Insert a bigger number and check that it changed.
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.2, duplicatePolicy: TsDuplicatePolicy.MAX));
            results = await ts.RangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.2, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicySum()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.0, duplicatePolicy: TsDuplicatePolicy.SUM));
            IReadOnlyList<TimeSeriesTuple> results = await ts.RangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(2.1, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyFirst()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.0, duplicatePolicy: TsDuplicatePolicy.FIRST));
            IReadOnlyList<TimeSeriesTuple> results = await ts.RangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.1, results[0].Val);
        }

        [Fact]
        public async Task TestAddWithDuplicatePolicyLast()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            TimeStamp timeStamp = DateTime.UtcNow;
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.1));
            Assert.Equal(timeStamp, await ts.AddAsync(key, timeStamp, 1.0, duplicatePolicy: TsDuplicatePolicy.LAST));
            IReadOnlyList<TimeSeriesTuple> results = await ts.RangeAsync(key, timeStamp, timeStamp);
            Assert.Equal(1.0, results[0].Val);
        }

        [Fact]
        public async Task TestOldAdd()
        {
            var key = CreateKeyName();
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var dateTime = DateTime.UtcNow;
            TimeStamp oldTimeStamp = dateTime.AddSeconds(-1);
            TimeStamp newTimeStamp = dateTime;
            await ts.CreateAsync(key);
            await ts.AddAsync(key, newTimeStamp, 1.1);
            // Adding old event
            var res = await ts.AddAsync(key, oldTimeStamp, 1.1);
            Assert.Equal(oldTimeStamp.Value, res.Value);
        }

        [Fact]
        public async Task TestWrongParameters()
        {
            var key = CreateKeyName();
            var value = 1.1;
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            var ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.AddAsync(key, "+", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);

            ex = await Assert.ThrowsAsync<RedisServerException>(async () => await ts.AddAsync(key, "-", value));
            Assert.Equal("ERR TSDB: invalid timestamp", ex.Message);
        }
    }
}
