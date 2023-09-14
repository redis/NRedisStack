
using NRedisStack.DataTypes;
using StackExchange.Redis;
using System.Runtime.CompilerServices;
using Xunit;
namespace NRedisStack.Tests
{
    public abstract class AbstractNRedisStackTest : IClassFixture<RedisFixture>, IAsyncLifetime
    {
        protected internal RedisFixture redisFixture;

        protected internal AbstractNRedisStackTest(RedisFixture redisFixture) => this.redisFixture = redisFixture;

        private List<string> keyNames = new List<string>();

        protected internal string CreateKeyName([CallerMemberName] string memberName = "") => CreateKeyNames(1, memberName)[0];

        protected internal string[] CreateKeyNames(int count, [CallerMemberName] string memberName = "")
        {
            if (count < 1) throw new ArgumentOutOfRangeException(nameof(count), "Must be greater than zero.");

            var newKeys = new string[count];
            for (var i = 0; i < count; i++)
            {
                newKeys[i] = $"{GetType().Name}:{memberName}:{i}";
            }

            keyNames.AddRange(newKeys);

            return newKeys;
        }

        protected internal static List<TimeSeriesTuple> ReverseData(List<TimeSeriesTuple> data)
        {
            var tuples = new List<TimeSeriesTuple>(data.Count);
            for (var i = data.Count - 1; i >= 0; i--)
            {
                tuples.Add(data[i]);
            }

            return tuples;
        }

        public Task InitializeAsync() => Task.CompletedTask;

        public void Dispose()
        {
            redisFixture.Redis.GetDatabase().ExecuteBroadcast("FLUSHALL");
        }

        public async Task DisposeAsync()
        {
            var redis = redisFixture.Redis.GetDatabase();
            // await redis.KeyDeleteAsync(keyNames.Select(i => (RedisKey)i).ToArray());
            await redis.ExecuteBroadcastAsync("FLUSHALL");
        }
    }
}