using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading.Tasks;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMAddAsync : AbstractNRedisStackTest
    {
        public TestMAddAsync(RedisFixture redisFixture) : base(redisFixture) { }


        [Fact]
        public async Task TestStarMADD()
        {
            var keys = CreateKeyNames(2);

            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            foreach (string key in keys)
            {
                await db.TS().CreateAsync(key);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, "*", 1.1));
            }
            var response = await db.TS().MAddAsync(sequence);

            Assert.Equal(keys.Length, response.Count);

            foreach (var key in keys)
            {
                TimeSeriesInformation info = await db.TS().InfoAsync(key);
                Assert.True(info.FirstTimeStamp > 0);
                Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
            }
        }


        [Fact]
        public async Task TestSuccessfulMAdd()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key);
            }

            var sequence = new List<(string, TimeStamp, double)>(keys.Length);
            var timestamps = new List<DateTime>(keys.Length);
            foreach (var keyname in keys)
            {
                var now = DateTime.UtcNow;
                timestamps.Add(now);
                sequence.Add((keyname, now, 1.1));
            }

            var response = await db.TS().MAddAsync(sequence);
            Assert.Equal(timestamps.Count, response.Count);
            for (var i = 0; i < response.Count; i++)
            {
                Assert.Equal<DateTime>(timestamps[i], response[i]);
            }
        }

        [Fact]
        public async Task TestOverrideMAdd()
        {
            var keys = CreateKeyNames(2);
            var db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            foreach (var key in keys)
            {
                await db.TS().CreateAsync(key);
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

            await db.TS().MAddAsync(sequence);
            sequence.Clear();

            // Override the same events should not throw an error
            for (var i = 0; i < keys.Length; i++)
            {
                sequence.Add((keys[i], oldTimeStamps[i], 1.1));
            }

            var response = await db.TS().MAddAsync(sequence);

            Assert.Equal(oldTimeStamps.Count, response.Count);
            for (int i = 0; i < response.Count; i++)
            {
                Assert.Equal<DateTime>(oldTimeStamps[i], response[i]);
            }
        }
    }
}
