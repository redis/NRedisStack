using Xunit;
using StackExchange.Redis;
using Moq;

namespace NRedisStack.Tests.Gears;

public class GearsTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "BLOOM_TESTS";
    public GearsTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }


    [Fact]
    public void TestTFunctionLoadDelete()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(db.TFunctionLoad("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(db.TFunctionDelete("lib"));
    }

    [Fact]
    public async Task TestTFunctionLoadDeleteAsync()
    {
        IDatabase db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        Assert.True(await db.TFunctionLoadAsync("#!js api_version=1.0 name=lib\n redis.registerFunction('foo', ()=>{return 'bar'})"));
        Assert.True(await db.TFunctionDeleteAsync("lib"));
    }
}