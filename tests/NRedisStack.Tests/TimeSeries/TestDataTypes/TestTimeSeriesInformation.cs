using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using Xunit;
using NRedisStack.RedisStackCommands;
using NRedisStack.Tests;


namespace NRedisTimeSeries.Test.TestDataTypes
{
    public class TestInformation : AbstractNRedisStackTest
    {
        public TestInformation(NRedisStack.Tests.RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        [Obsolete]
        public void TestInformationSync()
        {
            string key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            ts.Add(key, "*", 1.1);
            ts.Add(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

            TimeSeriesInformation info = ts.Info(key);
            TimeSeriesInformation infoDebug = ts.Info(key, debug: true);

            Assert.Equal(4184, info.MemoryUsage);
            Assert.Equal(0, info.RetentionTime);
            Assert.Equal(1, info.ChunkCount);
            Assert.Null(info.DuplicatePolicy);
            Assert.Null(info.KeySelfName);
            Assert.Null(info.Chunks);

            Assert.Equal(4184, infoDebug.MemoryUsage);
            Assert.Equal(0, infoDebug.RetentionTime);
            Assert.Equal(1, infoDebug.ChunkCount);
            Assert.Null(infoDebug.DuplicatePolicy);
            Assert.Equal(infoDebug.KeySelfName, key);
            Assert.Equal(1, infoDebug.Chunks!.Count);
        }

        [Fact]
        [Obsolete]
        public async Task TestInformationAsync()
        {
            string key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();
            await ts.AddAsync(key, "*", 1.1);
            await ts.AddAsync(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

            TimeSeriesInformation info = await ts.InfoAsync(key);
            TimeSeriesInformation infoDebug = await ts.InfoAsync(key, debug: true);

            Assert.Equal(4184, info.MemoryUsage);
            Assert.Equal(0, info.RetentionTime);
            Assert.Equal(1, info.ChunkCount);
            Assert.Null(info.DuplicatePolicy);
            Assert.Null(info.KeySelfName);
            Assert.Null(info.Chunks);

            Assert.Equal(4184, infoDebug.MemoryUsage);
            Assert.Equal(0, infoDebug.RetentionTime);
            Assert.Equal(1, infoDebug.ChunkCount);
            Assert.Null(infoDebug.DuplicatePolicy);
            Assert.Equal(infoDebug.KeySelfName, key);
            Assert.Equal(1, infoDebug.Chunks!.Count);
        }
    }
}