// EXAMPLE: cmds_hash
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class CmdsHashExample
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
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

        // Tests for 'hget' step.
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
            new HashEntry[] {
                new HashEntry("field2", "Hi"),
                new HashEntry("field3", "World")
            }
        );

        RedisValue res6 = db.HashGet("myhash", "field2");
        Console.WriteLine(res6);    // >>> Hi

        RedisValue res7 = db.HashGet("myhash", "field3");
        Console.WriteLine(res7);    // >>> World

        HashEntry[] res8 = db.HashGetAll("myhash");
        Console.WriteLine($"{string.Join(", ", res8.Select(h => $"{h.Name}: {h.Value}"))}");
        // >>> field1: Hello, field2: Hi, field3: World
        // STEP_END
        
        // Tests for 'hset' step.
        // REMOVE_START
        Assert.True(res4);
        Assert.Equal("Hello", res5);
        Assert.Equal("Hi", res6);
        Assert.Equal("World", res7);
        Assert.Equal(
            "field1: Hello, field2: Hi, field3: World",
            string.Join(", ", res8.Select(h => $"{h.Name}: {h.Value}"))
        );
        // REMOVE_END
// HIDE_START
    }
}
// HIDE_END

