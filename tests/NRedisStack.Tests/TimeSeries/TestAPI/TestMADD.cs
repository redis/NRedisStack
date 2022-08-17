using System;
using System.Collections.Generic;
using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMADD : AbstractTimeSeriesTest, IDisposable
    {

        private readonly string[] keys = { "MADD_TESTS_1", "MADD_TESTS_2" };

        public TestMADD(RedisFixture redisFixture) : base(redisFixture) { }

        public void Dispose()
        {
            foreach (string key in keys)
            {
                redisFixture.Redis.GetDatabase().KeyDelete(key);
            }
        }

        [Fact]
        public void TestStarMADD()
        {

            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            foreach (string key in keys)
            {
                db.TS().Create(key);
            }
            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, "*", 1.1));
            }
            var response = db.TS().MAdd(sequence);

            Assert.Equal(keys.Length, response.Count);

            foreach (var key in keys)
            {
                TimeSeriesInformation info = db.TS().Info(key);
                Assert.True(info.FirstTimeStamp > 0);
                Assert.Equal(info.FirstTimeStamp, info.LastTimeStamp);
            }
        }

        [Fact]
        public void TestSuccessfulMADD()
        {

            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            foreach (string key in keys)
            {
                db.TS().Create(key);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            List<DateTime> timestamps = new List<DateTime>(keys.Length);
            foreach (var keyname in keys)
            {
                DateTime now = DateTime.UtcNow;
                timestamps.Add(now);
                sequence.Add((keyname, now, 1.1));
            }
            var response = db.TS().MAdd(sequence);

            Assert.Equal(timestamps.Count, response.Count);
            for (int i = 0; i < response.Count; i++)
            {
                Assert.Equal<DateTime>(timestamps[i], response[i]);
            }
        }

        [Fact]
        public void TestOverrideMADD()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute("FLUSHALL");

            foreach (string key in keys)
            {
                db.TS().Create(key);
            }

            List<DateTime> oldTimeStamps = new List<DateTime>();
            foreach (var keyname in keys)
            {
                oldTimeStamps.Add(DateTime.UtcNow);
            }

            List<(string, TimeStamp, double)> sequence = new List<(string, TimeStamp, double)>(keys.Length);
            foreach (var keyname in keys)
            {
                sequence.Add((keyname, DateTime.UtcNow, 1.1));
            }
            db.TS().MAdd(sequence);

            sequence.Clear();

            // Override the same events should not throw an error
            for (int i = 0; i < keys.Length; i++)
            {
                sequence.Add((keys[i], oldTimeStamps[i], 1.1));
            }
            var response = db.TS().MAdd(sequence);

            Assert.Equal(oldTimeStamps.Count, response.Count);
            for (int i = 0; i < response.Count; i++)
            {
                Assert.Equal<DateTime>(oldTimeStamps[i], response[i]);
            }
        }
    }
}
