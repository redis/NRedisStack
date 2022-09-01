using System.Diagnostics;
using Xunit;
using StackExchange.Redis;
using Moq;
using NRedisStack.RedisStackCommands;


namespace NRedisStack.Tests;

public class JsonTests : AbstractNRedisStackTest, IDisposable
{
    Mock<IDatabase> _mock = new Mock<IDatabase>();
    private readonly string _testName = "JSON_TESTS";
    public JsonTests(RedisFixture redisFixture) : base(redisFixture) { }

    public void Dispose()
    {
        redisFixture.Redis.GetDatabase().KeyDelete(_testName);
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
        _mock.Setup(x => x.Execute(It.IsAny<string>(), It.IsAny<object[]>())).Returns((RedisResult.Create(new RedisValue("OK"))));
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


    [Fact]
    public void TestModulePrefixs()
    {
        IDatabase db1 = redisFixture.Redis.GetDatabase();
        IDatabase db2 = redisFixture.Redis.GetDatabase();

        var json1 = db1.JSON();
        var json2 = db2.JSON();

        Assert.NotEqual(json1.GetHashCode(), json2.GetHashCode());
    }

    [Fact]
    public void TestModulePrefixs1()
    {
        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var json = db.JSON();
            // ...
            conn.Dispose();
        }

        {
            var conn = ConnectionMultiplexer.Connect("localhost");
            IDatabase db = conn.GetDatabase();

            var json = db.JSON();
            // ...
            conn.Dispose();
        }
    }

    [Fact]
    public void TestResp()
    {
        //arrange
        var conn = redisFixture.Redis;
        var db = conn.GetDatabase();
        IJsonCommands commands = new JsonCommands(db);
        var keys = CreateKeyNames(1);

        var key = keys[0];
        commands.Set(key, "$", new { name = "Steve", age = 33 });
        
        //act
        var respResult = commands.Resp(key);
        
        //assert
        var i = 0;
        Assert.Equal("{", respResult[i++]!.ToString());
        Assert.Equal("name", respResult[i++]!.ToString());
        Assert.Equal("Steve", respResult[i++]!.ToString());
        Assert.Equal("age", respResult[i++]!.ToString());
        Assert.Equal(33, (long)respResult[i]!);
        conn.GetDatabase().KeyDelete(key);
    }

    [Fact]
    public void TestStringAppend()
    {
        //arrange
        var conn = redisFixture.Redis;
        var db = conn.GetDatabase();
        IJsonCommands commands = new JsonCommands(db);
        var keys = CreateKeyNames(2);

        var key = keys[0];
        commands.Set(key, "$", new { name = "Steve", sibling = new {name = "christopher"}, age = 33});
        var simpleStringKey = keys[1];
        commands.Set(simpleStringKey, "$", "\"foo\"");
        
        //act
        var nullResult = commands.StringAppend(key, "$.age", " Lorello");
        var keyResult = commands.StringAppend(key, "$..name", " Lorello");
        var simpleKeyResult = commands.StringAppend(simpleStringKey, null, "bar");
        
        //assert
        var i = 0;
        Assert.Equal(2, keyResult.Length);
        Assert.Equal(13, keyResult[i++]);
        Assert.Equal(19, keyResult[i++]);
        Assert.Null(nullResult[0]);
        Assert.Equal(6, simpleKeyResult[0]);
    }

    [Fact]
    public void StringLength()
    {
        //arrange
        var conn = redisFixture.Redis;
        var db = conn.GetDatabase();
        IJsonCommands commands = new JsonCommands(db);
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleStringKey = keys[1];
        
        commands.Set(key, "$", new { name = "Steve", sibling = new {name = "christopher"}, age = 33});
        commands.Set(simpleStringKey, "$", "\"foo\"");

        var normalResult = commands.StringLength(key, "$..name");
        var nullResult = commands.StringLength(key, "$.age");
        var simpleResult = commands.StringLength(simpleStringKey);

        var i = 0;
        Assert.Equal(5, normalResult[i++]);
        Assert.Equal(11, normalResult[i]);
        Assert.Null(nullResult[0]);
        Assert.Equal(3,simpleResult[0]);
    }

    [Fact]
    public void Toggle()
    {
        //arrange
        var conn = redisFixture.Redis;
        var db = conn.GetDatabase();
        IJsonCommands commands = new JsonCommands(db);
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];

        commands.Set(key, "$", new { @bool = true, other = new {@bool = false}, age = 33});
        commands.Set(simpleKey, "$", true);

        var result = commands.Toggle(key, "$..bool");
        var simpleResult = commands.Toggle(simpleKey);
        
        Assert.False(result[0]);
        Assert.True(result[1]);
        Assert.False(simpleResult[0]);
    }

    [Fact]
    public void Type()
    {
        //arrange
        var conn = redisFixture.Redis;
        var db = conn.GetDatabase();
        IJsonCommands commands = new JsonCommands(db);
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];
        commands.Set(key, "$", new { name = "Steve", sibling = new {name = "christopher"}, age = 33, aDouble = 3.5});
        commands.Set(simpleKey, "$", "true");

        var result = commands.Type(key, "$..name");
        Assert.Equal(JsonType.STRING, result[0]);
        Assert.Equal(JsonType.STRING, result[1]);
        result = commands.Type(key, "$..age");
        Assert.Equal(JsonType.INTEGER, result[0]);
        result = commands.Type(key, "$..aDouble");
        Assert.Equal(JsonType.NUMBER, result[0]);
        result = commands.Type(simpleKey);
        Assert.Equal(JsonType.BOOLEAN, result[0]);
    }

    [Fact]
    public void ArrayAppend()
    {
        var conn = redisFixture.Redis;
        var db = conn.GetDatabase();
        IJsonCommands commands = new JsonCommands(db);
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var complexKey = keys[1];
        
        commands.Set(key, "$", new { name = "Elizabeth", nickNames = new[] { "Beth" } });
        commands.Set(complexKey, "$", new { name = "foo", people = new[] { new { name = "steve" } } });
        var result = commands.ArrayAppend(key, "$.nickNames", "Elle", "Liz","Betty");
        Assert.Equal(4, result[0]);
        result = commands.ArrayAppend(complexKey, "$.people", new { name = "bob" });
        Assert.Equal(2, result[0]);
    }

    [Fact]
    public void ArrayIndex()
    {
        var conn = redisFixture.Redis;
        var db = conn.GetDatabase();
        IJsonCommands commands = new JsonCommands(db);
        var keys = CreateKeyNames(1);
        var key = keys[0];
        commands.Set(key, "$", new { name = "Elizabeth", nicknames = new[] { "Beth", "Betty", "Liz" }, sibling = new {name="Johnathan", nicknames = new [] {"Jon", "Johnny"}} });
        var res = commands.ArrayIndex(key, "$..nicknames", "Betty", 0,5);
        Assert.Equal(1,res[0]);
        Assert.Equal(-1,res[1]);
    }

    [Fact]
    public void ArrayInsert()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];

        commands.Set(key, "$", new { name = "Alice", nicknames = new[] { "Al", "Ali", "Ally" } });
        commands.Set(simpleKey, "$", new[] { "Al", "Ali", "Ally" });

        var result = commands.ArrayInsert(key, $"$.nicknames", 1, "Lys");
        Assert.Equal(4,result[0]);
        result = commands.ArrayInsert(simpleKey, "$", 1, "Lys");
        Assert.Equal(4, result[0]);
    }

    [Fact]
    public void ArrayLength()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];
        commands.Set(key, "$", new { name = "Alice", nicknames = new[] { "Al", "Ali", "Ally" } });
        commands.Set(simpleKey, "$", new[] { "Al", "Ali", "Ally" });

        var result = commands.ArrayLength(key, $"$.nicknames");
        Assert.Equal(3, result[0]);
        result = commands.ArrayLength(simpleKey);
        Assert.Equal(3, result[0]);
    }

    [Fact]
    public void ArrayPop()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];
        commands.Set(key, "$", new { name = "Alice", nicknames = new[] { "Al", "Ali", "Ally" } });
        commands.Set(simpleKey, "$", new[] { "Al", "Ali", "Ally" });

        var result = commands.ArrayPop(key, "$.nicknames", 1);
        Assert.Equal("\"Ali\"", result[0].ToString());
        result = commands.ArrayPop(key, "$.nicknames");
        Assert.Equal("\"Ally\"", result[0].ToString());
        result = commands.ArrayPop(simpleKey);
        Assert.Equal("\"Ally\"", result[0].ToString());
    }

    [Fact]
    public void ArrayTrim()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];
        commands.Set(key, "$", new { name = "Alice", nicknames = new[] { "Al", "Ali", "Ally" } });
        commands.Set(simpleKey, "$", new[] { "Al", "Ali", "Ally" });

        var result = commands.ArrayTrim(key, "$.nicknames", 0, 0);
        Assert.Equal(1,result[0]);
        result = commands.ArrayTrim(simpleKey, "$", 0, 1);
        Assert.Equal(2,result[0]);
    }

    [Fact]
    public void Clear()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];
        commands.Set(key, "$", new { name = "Alice", nicknames = new[] { "Al", "Ali", "Ally" } });
        commands.Set(simpleKey, "$", new[] { "Al", "Ali", "Ally" });

        var result = commands.Clear(key, "$.nicknames");
        Assert.Equal(1,result);
        result = commands.Clear(simpleKey);
        Assert.Equal(1,result);
    }

    [Fact]
    public void Del()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];
        commands.Set(key, "$", new { name = "Alice", nicknames = new[] { "Al", "Ali", "Ally" } });
        commands.Set(simpleKey, "$", new[] { "Al", "Ali", "Ally" });

        var result = commands.Del(key, "$.nicknames");
        Assert.Equal(1,result);
        result = commands.Del(simpleKey);
        Assert.Equal(1,result);
    }
    
    [Fact]
    public void Forget()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var simpleKey = keys[1];
        commands.Set(key, "$", new { name = "Alice", nicknames = new[] { "Al", "Ali", "Ally" } });
        commands.Set(simpleKey, "$", new[] { "Al", "Ali", "Ally" });

        var result = commands.Forget(key, "$.nicknames");
        Assert.Equal(1,result);
        result = commands.Forget(simpleKey);
        Assert.Equal(1,result);
    }

    [Fact]
    public void Get()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key = keys[0];
        var complexKey = keys[1];
        commands.Set(key, "$", new Person(){Age = 35, Name = "Alice"});
        commands.Set(complexKey, "$", new {a=new Person(){Age = 35, Name = "Alice"}, b = new {a = new Person(){Age = 35, Name = "Alice"}}});
        var result = commands.Get<Person>(key);
        Assert.Equal("Alice", result!.Name);
        Assert.Equal(35, result.Age);
        var people = commands.GetEnumerable<Person>(complexKey, "$..a").ToArray();
        Assert.Equal(2, people.Length);
        Assert.Equal("Alice", people[0]!.Name);
        Assert.Equal(35, people[0]!.Age);
        Assert.Equal("Alice", people[1]!.Name);
        Assert.Equal(35, people[1]!.Age);
    }

    [Fact]
    public void MGet()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(2);
        var key1 = keys[0];
        var key2 = keys[1];
        commands.Set(key1, "$", new { a = "hello" });
        commands.Set(key2, "$", new { a = "world" });
        var result = commands.MGet(keys.Select(x => new RedisKey(x)).ToArray(), "$.a");
        
        Assert.Equal("[\"hello\"]", result[0].ToString());
        Assert.Equal("[\"world\"]", result[1].ToString());
    }

    [Fact]
    public void NumIncrby()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(1);
        var key = keys[0];
        commands.Set(key, "$", new { age = 33, a = new { age = 34 }, b = new {age = "cat"} });
        var result = commands.NumIncrby(key, "$..age", 2);
        Assert.Equal(35, result[0]);
        Assert.Equal(36, result[1]);
        Assert.Null(result[2]);
    }

    [Fact]
    public void ObjectKeys()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(3);
        var key = keys[0];
        commands.Set(key, "$", new { a = 5, b = 10, c = "hello", d = new { a = new { a = 6, b = "hello" }, b = 7 } });
        var result = commands.ObjectKeys(key).ToArray();
        Assert.Contains("a", result[0]);
        Assert.Contains("b", result[0]);
        Assert.Contains("c", result[0]);
        Assert.Contains("d", result[0]);
        result = commands.ObjectKeys(key, "$..a").ToArray();
        Assert.Empty(result[0]);
        Assert.Contains("a", result[1]);
        Assert.Contains("b", result[1]);
    }

    [Fact]
    public void ObjectLength()
    {
        IJsonCommands commands = new JsonCommands(redisFixture.Redis.GetDatabase());
        var keys = CreateKeyNames(3);
        var key = keys[0];
        commands.Set(key, "$", new { a = 5, b = 10, c = "hello", d = new { a = new { a = 6, b = "hello" }, b = 7 } });
        var result = commands.ObjectLength(key);
        Assert.Equal(4, result[0]);
        result = commands.ObjectLength(key, $"$..a");
        Assert.Null(result[0]);
        Assert.Equal(2, result[1]);
        Assert.Null(result[2]);
        
    }
}