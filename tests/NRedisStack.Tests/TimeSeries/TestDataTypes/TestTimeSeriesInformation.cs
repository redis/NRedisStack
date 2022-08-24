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
        public async Task TestInformationAsync()
        {
            string key = CreateKeyName();
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            await db.TS().AddAsync(key, "*", 1.1);
            await db.TS().AddAsync(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);
            TimeSeriesInformation info = await db.TS().InfoAsync(key);
            //Assert.Equal(2, info.TotalSamples);
            Assert.Equal(4184, info.MemoryUsage);
            Assert.Equal(0, info.RetentionTime);
            Assert.Equal(1, info.ChunkCount);
            Assert.Equal(null, info.DuplicatePolicy);
        }

        // TODO: add test for TS.INFO DEBUG
    }
}