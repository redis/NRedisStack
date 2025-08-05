// EXAMPLE: sets_tutorial
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

//REMOVE_START
namespace Doc;
[Collection("DocsTests")]
//REMOVE_END

// HIDE_START
public class SetsExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public SetsExample(EndpointsFixture fixture) : base(fixture) { }

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
        bool delRes = db.KeyDelete("bikes:racing:france");
        delRes = db.KeyDelete("bikes:racing:usa");
        //REMOVE_END
        // HIDE_END


        // STEP_START sadd
        long res1 = db.SetAdd("bikes:racing:france", ["bike:1"]);
        Console.WriteLine(res1);    // >>> 1

        long res2 = db.SetAdd("bikes:racing:france", ["bike:1"]);
        Console.WriteLine(res2);    // >>> 0

        long res3 = db.SetAdd("bikes:racing:france", ["bike:2", "bike:3"]);
        Console.WriteLine(res3);    // >>> 2

        long res4 = db.SetAdd("bikes:racing:usa", ["bike:1", "bike:4"]);
        Console.WriteLine(res4);    // >>> 2
        // STEP_END

        // Tests for 'sadd' step.
        // REMOVE_START
        Assert.Equal(1, res1);
        Assert.Equal(0, res2);
        Assert.Equal(2, res3);
        Assert.Equal(2, res4);
        // REMOVE_END


        // STEP_START sismember
        bool res5 = db.SetContains("bikes:racing:france", "bike:1");
        Console.WriteLine(res5);    // >>> True

        bool res6 = db.SetContains("bikes:racing:usa", "bike:2");
        Console.WriteLine(res6);    // >>> False
        // STEP_END

        // Tests for 'sismember' step.
        // REMOVE_START
        Assert.True(res5);
        Assert.False(res6);
        // REMOVE_END


        // STEP_START sinter
        long res7 = db.SetAdd("{bikes:racing}:france", ["bike:1", "bike:2", "bike:3"]);
        long res8 = db.SetAdd("{bikes:racing}:usa", ["bike:1", "bike:4"]);

        RedisValue[] res9 = db.SetCombine(SetOperation.Intersect, ["{bikes:racing}:france", "{bikes:racing}:usa"]);
        Console.WriteLine(string.Join(", ", res9)); // >>> bike:1
        // STEP_END

        // Tests for 'sinter' step.
        // REMOVE_START
        Assert.Equal(3, res7);
        Assert.Equal(2, res8);
        Assert.Equal("bike:1", string.Join(", ", res9));
        // REMOVE_END


        // STEP_START scard
        long res10 = db.SetAdd("bikes:racing:france", ["bike:1", "bike:2", "bike:3"]);
        long res11 = db.SetLength("bikes:racing:france");
        Console.WriteLine(res11);   // >>> 3
        // STEP_END

        // Tests for 'scard' step.
        // REMOVE_START
        Assert.Equal(3, res11);
        delRes = db.KeyDelete("bikes:racing:france");
        // REMOVE_END


        // STEP_START sadd_smembers
        long res12 = db.SetAdd("bikes:racing:france", ["bike:1", "bike:2", "bike:3"]);
        RedisValue[] res13 = db.SetMembers("bikes:racing:france");
        Console.WriteLine(string.Join(", ", res13));    // >>> bike:3, bike:2, bike:1
        // STEP_END

        // Tests for 'sadd_smembers' step.
        // REMOVE_START
        Assert.Equal(3, res12);
        // REMOVE_END


        // STEP_START smismember
        bool res14 = db.SetContains("bikes:racing:france", "bike:1");
        Console.WriteLine(res14);   // >>> true

        bool[] res15 = db.SetContains("bikes:racing:france", ["bike:2", "bike:3", "bike:4"]);
        Console.WriteLine(string.Join(", ", res15));    // >>> True, True, False
        // STEP_END

        // Tests for 'smismember' step.
        // REMOVE_START
        Assert.True(res14);
        Assert.Equal("True, True, False", string.Join(", ", res15));
        // REMOVE_END


        // STEP_START sdiff
        long res16 = db.SetAdd("{bikes:racing}:france", ["bike:1", "bike:2", "bike:3"]);
        long res17 = db.SetAdd("{bikes:racing}:usa", ["bike:1", "bike:4"]);
        RedisValue[] res18 = db.SetCombine(SetOperation.Difference, ["{bikes:racing}:france", "{bikes:racing}:usa"]);
        Console.WriteLine(string.Join(", ", res18));    // >>> bike:2, bike:3
        // STEP_END

        // Tests for 'sdiff' step.
        // REMOVE_START
        Assert.Equal(0, res16);
        Assert.Equal(0, res17);
        // REMOVE_END


        // STEP_START multisets
        long res19 = db.SetAdd("{bikes:racing}:france", ["bike:1", "bike:2", "bike:3"]);
        long res20 = db.SetAdd("{bikes:racing}:usa", ["bike:1", "bike:4"]);
        long res21 = db.SetAdd("{bikes:racing}:italy", ["bike:1", "bike:2", "bike:3", "bike:4"]);

        RedisValue[] res22 = db.SetCombine(SetOperation.Intersect, ["{bikes:racing}:france", "{bikes:racing}:usa", "{bikes:racing}:italy"
        ]);
        Console.WriteLine(string.Join(", ", res22));    // >>> bike:1

        RedisValue[] res23 = db.SetCombine(SetOperation.Union, ["{bikes:racing}:france", "{bikes:racing}:usa", "{bikes:racing}:italy"
        ]);
        Console.WriteLine(string.Join(", ", res23));    // >>> bike:1, bike:2, bike:3, bike:4

        RedisValue[] res24 = db.SetCombine(SetOperation.Difference, ["{bikes:racing}:france", "{bikes:racing}:usa", "{bikes:racing}:italy"
        ]);
        Console.WriteLine(string.Join(", ", res24));    // >>> <empty set>

        RedisValue[] res25 = db.SetCombine(SetOperation.Difference, ["{bikes:racing}:usa", "{bikes:racing}:france"]);
        Console.WriteLine(string.Join(", ", res25));    // >>> bike:4

        RedisValue[] res26 = db.SetCombine(SetOperation.Difference, ["{bikes:racing}:france", "{bikes:racing}:usa"]);
        Console.WriteLine(string.Join(", ", res26));    // >>> bike:2, bike:3
        // STEP_END

        // Tests for 'multisets' step.
        // REMOVE_START
        Assert.Equal(0, res19);
        Assert.Equal(0, res20);
        Assert.Equal(4, res21);
        Assert.Equal("bike:1", string.Join(", ", res22));
        Assert.Equal("", string.Join(", ", res24));
        Assert.Equal("bike:4", string.Join(", ", res25));
        // REMOVE_END


        // STEP_START srem
        long res27 = db.SetAdd("bikes:racing:france", ["bike:1", "bike:2", "bike:3", "bike:4", "bike:5"]);

        bool res28 = db.SetRemove("bikes:racing:france", "bike:1");
        Console.WriteLine(res28);   // >>> True

        RedisValue res29 = db.SetPop("bikes:racing:france");
        Console.WriteLine(res29);   // >>> bike:3

        RedisValue[] res30 = db.SetMembers("bikes:racing:france");
        Console.WriteLine(string.Join(", ", res30));    // >>> bike:2, bike:4, bike:5

        RedisValue res31 = db.SetRandomMember("bikes:racing:france");
        Console.WriteLine(res31);   // >>> bike:4
        // STEP_END

        // Tests for 'srem' step.
        // REMOVE_START
        Assert.Equal(2, res27);
        Assert.True(res28);
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

