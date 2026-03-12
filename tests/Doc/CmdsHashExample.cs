// EXAMPLE: cmds_hash
// HIDE_START
using StackExchange.Redis;
// HIDE_END
// REMOVE_START
using NRedisStack.Tests;

namespace Doc;

[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class CmdsHashExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public CmdsHashExample(EndpointsFixture fixture) : base(fixture) { }

    [Fact]
    // REMOVE_END
    public void Run()
    {
        //REMOVE_START
        // This is needed because we're constructing ConfigurationOptions in the test before calling GetConnection
        SkipIfTargetConnectionDoesNotExist(EndpointsFixture.Env.Standalone);
        var _ = GetCleanDatabase(EndpointsFixture.Env.Standalone);
        //REMOVE_END
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete("myhash");
        //REMOVE_END
        // HIDE_END

        // STEP_START hget
        bool res1 = db.HashSet("myhash", "field1", "foo");

        RedisValue res2 = db.HashGet("myhash", "field1");
        Console.WriteLine(res2);    // >>> foo

        RedisValue res3 = db.HashGet("myhash", "field2");
        Console.WriteLine(res3);    // >>> Null

        // STEP_END
        // REMOVE_START
        Assert.True(res1);
        Assert.Equal("foo", res2);
        Assert.Equal(RedisValue.Null, res3);
        db.KeyDelete("myhash");
        // REMOVE_END

        // STEP_START hset
        bool res4 = db.HashSet("myhash", "field1", "Hello");
        RedisValue res5 = db.HashGet("myhash", "field1");
        Console.WriteLine(res5);    // >>> Hello

        db.HashSet(
            "myhash",
            [
                new("field2", "Hi"),
                new("field3", "World")
            ]
        );

        RedisValue res6 = db.HashGet("myhash", "field2");
        Console.WriteLine(res6);    // >>> Hi

        RedisValue res7 = db.HashGet("myhash", "field3");
        Console.WriteLine(res7);    // >>> World

        HashEntry[] res8 = db.HashGetAll("myhash");
        Console.WriteLine($"{string.Join(", ", res8.Select(h => $"{h.Name}: {h.Value}"))}");
        // >>> field1: Hello, field2: Hi, field3: World

        // STEP_END
        // REMOVE_START
        Assert.True(res4);
        Assert.Equal("Hello", res5);
        Assert.Equal("Hi", res6);
        Assert.Equal("World", res7);
        Assert.Equal(
            "field1: Hello, field2: Hi, field3: World",
            string.Join(", ", res8.Select(h => $"{h.Name}: {h.Value}"))
        );
        db.KeyDelete("myhash");
        // REMOVE_END

        // STEP_START hgetall
        db.HashSet("myhash",
            [
                new("field1", "Hello"),
                new("field2", "World")
            ]
        );

        HashEntry[] hGetAllResult = db.HashGetAll("myhash");
        Array.Sort(hGetAllResult, (a1, a2) => a1.Name.CompareTo(a2.Name));
        Console.WriteLine(
            string.Join(", ", hGetAllResult.Select(e => $"{e.Name}: {e.Value}"))
        );
        // >>> field1: Hello, field2: World
        // STEP_END
        // REMOVE_START
        Assert.Equal("field1: Hello, field2: World", string.Join(", ", hGetAllResult.Select(e => $"{e.Name}: {e.Value}")));
        db.KeyDelete("myhash");
        // REMOVE_END

        // STEP_START hvals
        db.HashSet("myhash",
            [
                new("field1", "Hello"),
                new("field2", "World")
            ]
        );

        RedisValue[] hValsResult = db.HashValues("myhash");
        Array.Sort(hValsResult);
        Console.WriteLine(string.Join(", ", hValsResult));
        // >>> Hello, World
        // STEP_END
        // REMOVE_START
        Assert.Equal("Hello, World", string.Join(", ", hValsResult));
        // REMOVE_END
        // HIDE_START
    }
}
// HIDE_END
