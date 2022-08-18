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
        //TODO: fix this tests

        // [Fact]
        // public async Task TestInformationToStringAsync()
        // {
        //     string key = CreateKeyName();
        //     IDatabase db = redisFixture.Redis.GetDatabase();
        //     db.Execute("FLUSHALL");
        //     await db.TS().AddAsync(key, "*", 1.1);
        //     await db.TS().AddAsync(key, "*", 1.3, duplicatePolicy: TsDuplicatePolicy.LAST);
        //     TimeSeriesInformation info = await db.TS().InfoAsync(key);
        //     string[] infoProperties = ((string)info).Trim('{').Trim('}').Split(",");
        //     Assert.Equal("\"TotalSamples\":2", infoProperties[0]);
        //     Assert.Equal("\"MemoryUsage\":4184", infoProperties[1]);
        //     Assert.Equal("\"RetentionTime\":0", infoProperties[4]);
        //     Assert.Equal("\"ChunkCount\":1", infoProperties[5]);
        //     Assert.Equal("\"DuplicatePolicy\":null", infoProperties[11]);
        // }
    }
}
