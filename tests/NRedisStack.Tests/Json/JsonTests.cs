using Xunit;
using StackExchange.Redis;
using Moq;
using NRedisStack.Core.RedisStackCommands;


namespace NRedisStack.Tests;

public class JsonTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string key = "JSON_TESTS";
    public JsonTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void TestJsonSet()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        _mock.Object.JSON().Set("Person:Shachar", "$", obj, When.Exists);
        _mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}", "XX"));
    }

    [Fact]
    public void TestJsonSetNotExist()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        _mock.Object.JSON().Set("Person:Shachar", "$", obj, When.NotExists);
        _mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}", "NX"));
    }

    [Fact]
    public void TestSimpleJsonGet()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        IDatabase db = redisFixture.Redis.GetDatabase();

        db.JSON().Set(key, "$", obj);
        string expected = "{\"Name\":\"Shachar\",\"Age\":23}";
        Assert.Equal(db.JSON().Get(key).ToString(), expected);
    }

    [Fact]
    public void TestJsonGet()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        IDatabase db = redisFixture.Redis.GetDatabase();

        db.JSON().Set(key, "$", obj);

        string expected = "[222111\"Shachar\"222]";
        Assert.Equal(db.JSON().Get(key, "111", "222", "333", "$.Name").ToString(), expected);
    }
}