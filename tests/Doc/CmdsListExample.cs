// EXAMPLE: cmds_list
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;

[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class CmdsListExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public CmdsListExample(EndpointsFixture fixture) : base(fixture) { }

    [SkippableFact]
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
        db.KeyDelete("mylist");
        //REMOVE_END
        // HIDE_END

        // STEP_START llen
        long lLenResult1 = db.ListLeftPush("mylist", "World");
        Console.WriteLine(lLenResult1); // >>> 1

        long lLenResult2 = db.ListLeftPush("mylist", "Hello");
        Console.WriteLine(lLenResult2); // >>> 2

        long lLenResult3 = db.ListLength("mylist");
        Console.WriteLine(lLenResult3); // >>> 2
        // STEP_END

        // Tests for 'llen' step.
        // REMOVE_START
        Assert.Equal(1, lLenResult1);
        Assert.Equal(2, lLenResult2);
        Assert.Equal(2, lLenResult3);
        db.KeyDelete("mylist");
        // REMOVE_END

        // STEP_START lpop
        long lPopResult1 = db.ListRightPush("mylist", ["one", "two", "three", "four", "five"]);
        Console.WriteLine(lPopResult1); // >>> 5

        RedisValue lPopResult2 = db.ListLeftPop("mylist");
        Console.WriteLine(lPopResult2); // >>> one

        RedisValue[] lPopResult3 = db.ListLeftPop("mylist", 2);
        Console.WriteLine($"[{string.Join(", ", lPopResult3)}]");
        // >>> [two, three]

        RedisValue[] lPopResult4 = db.ListRange("mylist", 0, -1);
        Console.WriteLine($"[{string.Join(", ", lPopResult4)}]");
        // >>> [four, five]
        // STEP_END

        // Tests for 'lpop' step.
        // REMOVE_START
        Assert.Equal(5, lPopResult1);
        Assert.Equal("one", lPopResult2);
        Assert.Equal("[two, three]", $"[{string.Join(", ", lPopResult3)}]");
        Assert.Equal("[four, five]", $"[{string.Join(", ", lPopResult4)}]");
        db.KeyDelete("mylist");
        // REMOVE_END

        // STEP_START lpush
        long lPushResult1 = db.ListLeftPush("mylist", "World");
        Console.WriteLine(lPushResult1); // >>> 1

        long lPushResult2 = db.ListLeftPush("mylist", "Hello");
        Console.WriteLine(lPushResult2); // >>> 2

        RedisValue[] lPushResult3 = db.ListRange("mylist", 0, -1);
        Console.WriteLine($"[{string.Join(", ", lPushResult3)}]");
        // >>> [Hello, World]
        // STEP_END

        // Tests for 'lpush' step.
        // REMOVE_START
        Assert.Equal(1, lPushResult1);
        Assert.Equal(2, lPushResult2);
        Assert.Equal("[Hello, World]", $"[{string.Join(", ", lPushResult3)}]");
        db.KeyDelete("mylist");
        // REMOVE_END

        // STEP_START lrange
        long lRangeResult1 = db.ListRightPush("mylist", ["one", "two", "three"]);
        Console.WriteLine(lRangeResult1); // >>> 3

        RedisValue[] lRangeResult2 = db.ListRange("mylist", 0, 0);
        Console.WriteLine($"[{string.Join(", ", lRangeResult2)}]");
        // >>> [one]

        RedisValue[] lRangeResult3 = db.ListRange("mylist", -3, 2);
        Console.WriteLine($"[{string.Join(", ", lRangeResult3)}]");
        // >>> [one, two, three]

        RedisValue[] lRangeResult4 = db.ListRange("mylist", -100, 100);
        Console.WriteLine($"[{string.Join(", ", lRangeResult4)}]");
        // >>> [one, two, three]

        RedisValue[] lRangeResult5 = db.ListRange("mylist", 5, 10);
        Console.WriteLine($"[{string.Join(", ", lRangeResult5)}]");
        // >>> []
        // STEP_END

        // Tests for 'lrange' step.
        // REMOVE_START
        Assert.Equal(3, lRangeResult1);
        Assert.Equal("[one]", $"[{string.Join(", ", lRangeResult2)}]");
        Assert.Equal("[one, two, three]", $"[{string.Join(", ", lRangeResult3)}]");
        Assert.Equal("[one, two, three]", $"[{string.Join(", ", lRangeResult4)}]");
        Assert.Equal("[]", $"[{string.Join(", ", lRangeResult5)}]");
        db.KeyDelete("mylist");
        // REMOVE_END

        // STEP_START rpop
        long rPopResult1 = db.ListRightPush("mylist", ["one", "two", "three", "four", "five"]);
        Console.WriteLine(rPopResult1); // >>> 5

        RedisValue rPopResult2 = db.ListRightPop("mylist");
        Console.WriteLine(rPopResult2); // >>> five

        RedisValue[] rPopResult3 = db.ListRightPop("mylist", 2);
        Console.WriteLine($"[{string.Join(", ", rPopResult3)}]");
        // >>> [four, three]

        RedisValue[] rPopResult4 = db.ListRange("mylist", 0, -1);
        Console.WriteLine($"[{string.Join(", ", rPopResult4)}]");
        // >>> [one, two]
        // STEP_END

        // Tests for 'rpop' step.
        // REMOVE_START
        Assert.Equal(5, rPopResult1);
        Assert.Equal("five", rPopResult2);
        Assert.Equal("[four, three]", $"[{string.Join(", ", rPopResult3)}]");
        Assert.Equal("[one, two]", $"[{string.Join(", ", rPopResult4)}]");
        db.KeyDelete("mylist");
        // REMOVE_END

        // STEP_START rpush
        long rPushResult1 = db.ListRightPush("mylist", "hello");
        Console.WriteLine(rPushResult1); // >>> 1

        long rPushResult2 = db.ListRightPush("mylist", "world");
        Console.WriteLine(rPushResult2); // >>> 2

        RedisValue[] rPushResult3 = db.ListRange("mylist", 0, -1);
        Console.WriteLine($"[{string.Join(", ", rPushResult3)}]");
        // >>> [hello, world]
        // STEP_END

        // Tests for 'rpush' step.
        // REMOVE_START
        Assert.Equal(1, rPushResult1);
        Assert.Equal(2, rPushResult2);
        Assert.Equal("[hello, world]", $"[{string.Join(", ", rPushResult3)}]");
        db.KeyDelete("mylist");
        // REMOVE_END

        // HIDE_START
    }
}
// HIDE_END
