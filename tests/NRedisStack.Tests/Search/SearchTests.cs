using Xunit;
using StackExchange.Redis;
using NRedisStack.Core.RedisStackCommands;
using Moq;


namespace NRedisStack.Tests.Search;

public class SearchTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "SEARCH_TESTS";
    public SearchTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

}