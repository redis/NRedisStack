using NRedisStack.DataTypes;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests.TimeSeries.TestAPI
{
    public class TestMGet : AbstractNRedisStackTest, IDisposable
    {

        private readonly string[] keys = { "MGET_TESTS_1", "MGET_TESTS_2" };

        public TestMGet(RedisFixture redisFixture) : base(redisFixture) { }

        [SkipIfRedis(Is.OSSCluster, Is.Enterprise)]
        public void TestMGetQuery()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();

            var label1 = new TimeSeriesLabel("MGET_TESTS_1", "value");
            var label2 = new TimeSeriesLabel("MGET_TESTS_2", "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            TimeStamp ts1 = ts.Add(keys[0], "*", 1.1, labels: labels1);
            TimeSeriesTuple tuple1 = new TimeSeriesTuple(ts1, 1.1);
            TimeStamp ts2 = ts.Add(keys[1], "*", 2.2, labels: labels2);
            TimeSeriesTuple tuple2 = new TimeSeriesTuple(ts2, 2.2);
            var results = ts.MGet(new List<string> { "MGET_TESTS_1=value" });
            Assert.Equal(2, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(tuple1, results[0].value);
            Assert.Equal(new List<TimeSeriesLabel>(), results[0].labels);
            Assert.Equal(keys[1], results[1].key);
            Assert.Equal(tuple2, results[1].value);
            Assert.Equal(new List<TimeSeriesLabel>(), results[1].labels);

        }

        [SkipIfRedis(Is.OSSCluster, Is.Enterprise)]
        public void TestMGetQueryWithLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();

            var label1 = new TimeSeriesLabel("MGET_TESTS_1", "value");
            var label2 = new TimeSeriesLabel("MGET_TESTS_2", "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            TimeStamp ts1 = ts.Add(keys[0], "*", 1.1, labels: labels1);
            TimeSeriesTuple tuple1 = new TimeSeriesTuple(ts1, 1.1);
            TimeStamp ts2 = ts.Add(keys[1], "*", 2.2, labels: labels2);
            TimeSeriesTuple tuple2 = new TimeSeriesTuple(ts2, 2.2);

            var results = ts.MGet(new List<string> { "MGET_TESTS_1=value" }, withLabels: true);
            Assert.Equal(2, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(tuple1, results[0].value);
            Assert.Equal(labels1, results[0].labels);
            Assert.Equal(keys[1], results[1].key);
            Assert.Equal(tuple2, results[1].value);
            Assert.Equal(labels2, results[1].labels);
        }

        [SkipIfRedis(Is.OSSCluster, Is.Enterprise)]
        public void TestMGetQuerySelectedLabels()
        {
            IDatabase db = redisFixture.Redis.GetDatabase();
            db.Execute(new SerializedCommand("FLUSHALL", RequestPolicy.AllShards));
            var ts = db.TS();

            var label1 = new TimeSeriesLabel("MGET_TESTS_1", "value");
            var label2 = new TimeSeriesLabel("MGET_TESTS_2", "value2");
            var labels1 = new List<TimeSeriesLabel> { label1, label2 };
            var labels2 = new List<TimeSeriesLabel> { label1 };

            TimeStamp ts1 = ts.Add(keys[0], "*", 1.1, labels: labels1);
            TimeSeriesTuple tuple1 = new TimeSeriesTuple(ts1, 1.1);
            TimeStamp ts2 = ts.Add(keys[1], "*", 2.2, labels: labels2);
            TimeSeriesTuple tuple2 = new TimeSeriesTuple(ts2, 2.2);

            var results = ts.MGet(new List<string> { "MGET_TESTS_1=value" }, selectedLabels: new List<string> { "MGET_TESTS_1" });
            Assert.Equal(2, results.Count);
            Assert.Equal(keys[0], results[0].key);
            Assert.Equal(tuple1, results[0].value);
            Assert.Equal(labels2, results[0].labels);
            Assert.Equal(keys[1], results[1].key);
            Assert.Equal(tuple2, results[1].value);
            Assert.Equal(labels2, results[1].labels);
        }
    }
}
