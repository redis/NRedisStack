using StackExchange.Redis;
using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using System.Threading.Tasks;
using Xunit;
using NRedisStack.RedisStackCommands;
using NRedisStack.Tests.TimeSeries.TestAPI;
using NRedisStack.Tests;


namespace NRedisTimeSeries.Test.TestDataTypes
{
    public class TestInformation : AbstractNRedisStackTest
    {
        public TestInformation(NRedisStack.Tests.RedisFixture redisFixture) : base(redisFixture) { }

        [Fact]
        public void TestInformationSync()
        {
            string key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            db.TS().Add(key, "*", 1.1);
            db.TS().Add(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

            TimeSeriesInformation info = db.TS().Info(key);
            TimeSeriesInformation infoDebug = db.TS().Info(key, debug: true);

            Assert.Equal(4184, info.MemoryUsage);
            Assert.Equal(0, info.RetentionTime);
            Assert.Equal(1, info.ChunkCount);
            Assert.Equal(null, info.DuplicatePolicy);
            Assert.Null(info.KeySelfName);
            Assert.Null(info.Chunks);

            Assert.Equal(4184, infoDebug.MemoryUsage);
            Assert.Equal(0, infoDebug.RetentionTime);
            Assert.Equal(1, infoDebug.ChunkCount);
            Assert.Equal(null, infoDebug.DuplicatePolicy);
            Assert.Equal(infoDebug.KeySelfName, key);
            Assert.Equal(infoDebug.Chunks.Count, 1);
        }

        [Fact]
        public async Task TestInformationAsync()
        {
            string key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().AddAsync(key, "*", 1.1);
            await db.TS().AddAsync(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);

            TimeSeriesInformation info = await db.TS().InfoAsync(key);
            TimeSeriesInformation infoDebug = await db.TS().InfoAsync(key, debug: true);

            Assert.Equal(4184, info.MemoryUsage);
            Assert.Equal(0, info.RetentionTime);
            Assert.Equal(1, info.ChunkCount);
            Assert.Equal(null, info.DuplicatePolicy);
            Assert.Null(info.KeySelfName);
            Assert.Null(info.Chunks);

            Assert.Equal(4184, infoDebug.MemoryUsage);
            Assert.Equal(0, infoDebug.RetentionTime);
            Assert.Equal(1, infoDebug.ChunkCount);
            Assert.Equal(null, infoDebug.DuplicatePolicy);
            Assert.Equal(infoDebug.KeySelfName, key);
            Assert.Equal(infoDebug.Chunks.Count, 1);
        }
    }
}