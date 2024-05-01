using NRedisStack.DataTypes;
using NRedisStack.Literals.Enums;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMAddAsync : AbstractNRedisStackTest
    {
        public TestMAddAsync(RedisFixture redisFixture) : base(redisFixture) { }


        [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster)]
        [Obsolete]
        public async Task TestStarMADD()
        {
            var keys = CreateKeyNames(2);

            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            foreach (string key in keys)
            {
                await ts.CreateAsync(key);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, "*", 1.1));
            }
            var response = await ts.MAddAsync(sequence);

            Assert.Equal(keys.Length, response.Count);

            foreach (var key in keys)
            {
                TimeSeriesInformation info = await ts.InfoAsync(key);
                Assert.True(info.FirstTimeStamp! > 0);
                Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
            }
        }


        [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster)]
        public async Task TestSuccessfulMAdd()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            foreach (var key in keys)
            {
                await ts.CreateAsync(key);
            }

            var sequence = new List<(string, TimeStamp, double)>(keys.Length);
            var timestamps = new List<DateTime>(keys.Length);
            foreach (var keyname in keys)
            {
                var now = DateTime.UtcNow;
                timestamps.Add(now);
                sequence.Add((keyname, now, 1.1));
            }

            var response = await ts.MAddAsync(sequence);
            Assert.Equal(timestamps.Count, response.Count);
            for (var i = 0; i < response.Count; i++)
            {
                Assert.Equal(new DateTimeOffset(timestamps[i]).ToUnixTimeMilliseconds(), response[i].Value);
            }
        }

        [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster)]
        public async Task TestOverrideMAdd()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");
            var ts = db.TS();

            foreach (var key in keys)
            {
                await ts.CreateAsync(key, duplicatePolicy: TsDuplicatePolicy.MAX);
            }

            var oldTimeStamps = new List<DateTime>();
            foreach (var keyname in keys)
            {
                oldTimeStamps.Add(DateTime.UtcNow);
            }

            var sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, DateTime.UtcNow, 1.1));
            }

            await ts.MAddAsync(sequence);
            sequence.Clear();

            // Override the same events should not throw an error
            for (var i = 0; i < keys.Length; i++)
            {
                sequence.Add((keys[i], oldTimeStamps[i], 1.1));
            }

            var response = await ts.MAddAsync(sequence);

            Assert.Equal(oldTimeStamps.Count, response.Count);
            for (int i = 0; i < response.Count; i++)
            {
                Assert.Equal(new DateTimeOffset(oldTimeStamps[i]).ToUnixTimeMilliseconds(), response[i].Value);
            }
        }
    }
}
