using Xunit;
using StackExchange.Redis;
using Moq;
using NRedisStack.RedisStackCommands;


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

    // [Fact]
    // public void TestJsonSet()
    // {
    //     var obj = new Person { Name = "Shachar", Age = 23 };
    //     _mock.Object.JSON().Set("Person:Shachar", "$", obj, When.Exists);
    //     _mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}", "XX"));
    // }

    [Fact]
    public void TestJsonSetNotExist()
    {
        var obj = new Person { Name = "Shachar", Age = 23 };
        _mock.Object.JSON().Set("Person:Shachar", "$", obj, When.NotExists);
        _mock.Verify(x => x.Execute("JSON.SET", "Person:Shachar", "$", "{\"Name\":\"Shachar\",\"Age\":23}", "NX"));
    }

    //TODO: understand why this 2 tests are not pass what we do
    //"dotnet test" but they pass when we do "dotnet test --filter ..."
    // [Fact]
    // public void TestSimpleJsonGet()
    // {
    //     var obj = new Person { Name = "Shachar", Age = 23 };
    //     IDatabase db = redisFixture.Redis.GetDatabase();
    //     db.Execute("FLUSHALL");
    //     var cf = db.JSON();

    //     json.Set(key, "$", obj);
    //     string expected = "{\"Name\":\"Shachar\",\"Age\":23}";
    //     var result = json.Get(key).ToString();
    //     if(result == null)
    //         throw new ArgumentNullException(nameof(result));

    //     Assert.Equal(result, expected);
    // }

    // [Fact]
    // public void TestJsonGet()
    // {
    //     var obj = new Person { Name = "Shachar", Age = 23 };
    //     IDatabase db = redisFixture.Redis.GetDatabase();
    //     db.Execute("FLUSHALL");
    //     var cf = db.JSON();

    //     json.Set(key, "$", obj);

    //     var expected = "[222111\"Shachar\"222]";
    //     var result = json.Get(key, "111", "222", "333", "$.Name");
    //     // if(result == null)
    //     //     throw new ArgumentNullException(nameof(result));
    //     Assert.Equal(result.ToString(), expected);
    // }
}