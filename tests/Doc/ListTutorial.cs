//EXAMPLE: list_tutorial
//HIDE_START
//REMOVE_START
using NRedisStack;
using NRedisStack.Tests;
using StackExchange.Redis;
//REMOVE_END


//REMOVE_START
namespace Doc;

[Collection("DocsTests")]
//REMOVE_END
public class ListExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public ListExample(EndpointsFixture fixture) : base(fixture) { }

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
        //HIDE_END
        //REMOVE_START
        db.KeyDelete("bikes:repairs");
        db.KeyDelete("bikes:finished");
        db.KeyDelete("{bikes}:repairs");
        db.KeyDelete("{bikes}:finished");
        //REMOVE_END


        //STEP_START queue
        long res1 = db.ListLeftPush("bikes:repairs", "bike:1");
        Console.WriteLine(res1);    // >>> 1

        long res2 = db.ListLeftPush("bikes:repairs", "bike:2");
        Console.WriteLine(res2);    // >>> 2

        RedisValue res3 = db.ListRightPop("bikes:repairs");
        Console.WriteLine(res3);    // >>> "bike:1"

        RedisValue res4 = db.ListRightPop("bikes:repairs");
        Console.WriteLine(res4); // >>> "bike:2"
        //STEP_END

        //REMOVE_START
        Assert.Equal(1, res1);
        Assert.Equal(2, res2);
        Assert.Equal("bike:1", res3);
        Assert.Equal("bike:2", res4);
        //REMOVE_END

        //STEP_START stack
        long res5 = db.ListLeftPush("bikes:repairs", "bike:1");
        Console.WriteLine(res5);    // >>> 1

        long res6 = db.ListLeftPush("bikes:repairs", "bike:2");
        Console.WriteLine(res6);    // >>> 2

        RedisValue res7 = db.ListLeftPop("bikes:repairs");
        Console.WriteLine(res7);    // >>> "bike:2"

        RedisValue res8 = db.ListLeftPop("bikes:repairs");
        Console.WriteLine(res8);    // >>> "bike:1"
        //STEP_END

        //REMOVE_START
        Assert.Equal("bike:2", res7);
        Assert.Equal("bike:1", res8);
        //REMOVE_END


        //STEP_START llen
        long res9 = db.ListLength("bikes:repairs");
        Console.WriteLine(res9);    // >>> 0
        //STEP_END

        //REMOVE_START
        Assert.Equal(0, res9);
        //REMOVE_END


        //STEP_START lmove_lrange
        long res10 = db.ListLeftPush("{bikes}:repairs", "bike:1");
        Console.WriteLine(res10);   // >>> 1

        long res11 = db.ListLeftPush("{bikes}:repairs", "bike:2");
        Console.WriteLine(res11);   // >>> 2

        RedisValue res12 = db.ListMove("{bikes}:repairs", "{bikes}:finished", ListSide.Left, ListSide.Left);
        Console.Write(res12);   // >>> "bike:2"

        RedisValue[] res13 = db.ListRange("{bikes}:repairs", 0, -1);
        Console.WriteLine(string.Join(", ", res13));    // >>> "bike:1"

        RedisValue[] res14 = db.ListRange("{bikes}:finished", 0, -1);
        Console.WriteLine(string.Join(", ", res14));    // >>> "bike:2"
        //STEP_END

        //REMOVE_START
        Assert.Equal(1, res10);
        Assert.Equal(2, res11);
        Assert.Equal("bike:2", res12);
        Assert.Equal("bike:1", string.Join(", ", res13));
        Assert.Equal("bike:2", string.Join(", ", res14));
        bool delRes = db.KeyDelete("{bikes}:repairs");
        delRes = db.KeyDelete("{bikes}:finished");
        //REMOVE_END

        //STEP_START lpush_rpush
        long res15 = db.ListRightPush("bikes:repairs", "bike:1");
        Console.WriteLine(res15);   // >>> 1

        long res16 = db.ListRightPush("bikes:repairs", "bike:2");
        Console.WriteLine(res16);   // >>> 2

        long res17 = db.ListLeftPush("bikes:repairs", "bike:important_bike");
        Console.WriteLine(res17);   // >>> 3

        RedisValue[] res18 = db.ListRange("bikes:repairs", 0, -1);
        Console.WriteLine(string.Join(", ", res18));    // >>> "bike:important_bike, bike:1, bike:2"
        //STEP_END

        //REMOVE_START
        Assert.Equal(1, res15);
        Assert.Equal(2, res16);
        Assert.Equal(3, res17);
        Assert.Equal("bike:important_bike, bike:1, bike:2", string.Join(", ", res18));
        delRes = db.KeyDelete("bikes:repairs");
        //REMOVE_END

        //STEP_START variadic
        long res19 = db.ListRightPush("bikes:repairs", ["bike:1", "bike:2", "bike:3"]);
        Console.WriteLine(res19);   // >>> 3

        long res20 = db.ListLeftPush("bikes:repairs", ["bike:important_bike", "bike:very_important_bike"]);
        Console.WriteLine(res20);   // >>> 5

        RedisValue[] res21 = db.ListRange("bikes:repairs", 0, -1);
        Console.WriteLine(string.Join(", ", res21));
        // >>> "bike:very_important_bike, bike:important_bike, bike:1, bike:2, bike:3"
        //STEP_END

        //REMOVE_START
        Assert.Equal(3, res19);
        Assert.Equal(5, res20);
        Assert.Equal("bike:very_important_bike, bike:important_bike, bike:1, bike:2, bike:3", string.Join(", ", res21));
        delRes = db.KeyDelete("bikes:repairs");
        //REMOVE_END

        //STEP_START lpop_rpop
        long res22 = db.ListRightPush("bikes:repairs", ["bike:1", "bike:2", "bike:3"]);
        Console.WriteLine(res22);   // >>> 3

        RedisValue res23 = db.ListRightPop("bikes:repairs");
        Console.WriteLine(res23);   // >>> "bike:3"

        RedisValue res24 = db.ListLeftPop("bikes:repairs");
        Console.WriteLine(res24);   // >>> "bike:1"

        RedisValue res25 = db.ListRightPop("bikes:repairs");
        Console.WriteLine(res25);   // >>> "bike:2"

        RedisValue res26 = db.ListRightPop("bikes:repairs");
        Console.WriteLine(res26);   // >>> <Empty string>
        //STEP_END

        //REMOVE_START
        Assert.Equal(3, res22);
        Assert.Equal("bike:3", res23);
        Assert.Equal("bike:1", res24);
        Assert.Equal("bike:2", res25);
        Assert.Equal("", string.Join(", ", res26));
        //REMOVE_END

        //STEP_START ltrim
        long res27 = db.ListLeftPush("bikes:repairs", ["bike:1", "bike:2", "bike:3", "bike:4", "bike:5"]);
        Console.WriteLine(res27);   // >>> 5

        db.ListTrim("bikes:repairs", 0, 2);
        RedisValue[] res28 = db.ListRange("bikes:repairs", 0, -1);
        Console.WriteLine(string.Join(", ", res28));    // "bike:5, bike:4, bike:3"
        //STEP_END

        //REMOVE_START
        Assert.Equal(5, res27);
        Assert.Equal("bike:5, bike:4, bike:3", string.Join(", ", res28));
        delRes = db.KeyDelete("bikes:repairs");
        //REMOVE_END

        //STEP_START ltrim_end_of_list
        long res29 = db.ListRightPush("bikes:repairs", ["bike:1", "bike:2", "bike:3", "bike:4", "bike:5"]);
        Console.WriteLine(res29);   // >>> 5

        db.ListTrim("bikes:repairs", -3, -1);
        RedisValue[] res30 = db.ListRange("bikes:repairs", 0, -1);
        Console.WriteLine(string.Join(", ", res30));    // >>> "bike:3, bike:4, bike:5"
        //STEP_END

        //REMOVE_START
        Assert.Equal(5, res29);
        Assert.Equal("bike:3, bike:4, bike:5", string.Join(", ", res30));
        delRes = db.KeyDelete("bikes:repairs");
        //REMOVE_END

        //STEP_START brpop
        long res31 = db.ListRightPush("bikes:repairs", ["bike:1", "bike:2"]);
        Console.WriteLine(res31);   // >>> 2

        Tuple<RedisKey, RedisValue>? res32 = db.BRPop(["bikes:repairs"], 1);

        if (res32 != null)
            Console.WriteLine($"{res32.Item1} -> {res32.Item2}"); // >>> "bikes:repairs -> bike:2"

        Tuple<RedisKey, RedisValue>? res33 = db.BRPop(["bikes:repairs"], 1);

        if (res33 != null)
            Console.WriteLine($"{res33.Item1} -> {res33.Item2}"); // >>> "bikes:repairs -> bike:1"

        Tuple<RedisKey, RedisValue>? res34 = db.BRPop(["bikes:repairs"], 1);
        Console.WriteLine(res34);   // >>> "Null"
        //STEP_END

        //REMOVE_START
        Assert.Equal(2, res31);

        if (res32 != null)
            Assert.Equal("bikes:repairs -> bike:2", $"{res32.Item1} -> {res32.Item2}");
        if (res33 != null)
            Assert.Equal("bikes:repairs -> bike:1", $"{res33.Item1} -> {res33.Item2}");

        Assert.Null(res34);
        //REMOVE_END

        //STEP_START rule_1
        bool res35 = db.KeyDelete("new_bikes");
        Console.WriteLine(res35);   // >>> False

        long res36 = db.ListRightPush("new_bikes", ["bike:1", "bike:2", "bike:3"]);
        Console.WriteLine(res36);   // >>> 3
        //STEP_END

        //REMOVE_START
        Assert.False(res35);
        Assert.Equal(3, res36);
        //REMOVE_END

        //STEP_START rule_1.1
        bool res37 = db.StringSet("new_bikes", "bike:1");
        Console.WriteLine(res37);   // >>> True

        RedisType res38 = db.KeyType("new_bikes");
        Console.WriteLine(res38);   // >>> RedisType.String

        try
        {
            long res39 = db.ListRightPush("new_bikes", ["bike:2", "bike:3"]);
        }
        catch (Exception e)
        {
            Console.WriteLine(e);
        }
        //STEP_END

        //REMOVE_START
        Assert.True(res37);
        Assert.Equal(RedisType.String, res38);
        delRes = db.KeyDelete("new_bikes");
        //REMOVE_END

        //STEP_START rule_2
        long res40 = db.ListLeftPush("bikes:repairs", ["bike:1", "bike:2", "bike:3"]);
        Console.WriteLine(res40);   // >>> 3

        bool res41 = db.KeyExists("bikes:repairs");
        Console.WriteLine(res41);   // >>> True

        RedisValue res42 = db.ListLeftPop("bikes:repairs");
        Console.WriteLine(res42);   // >>> "bike:3"

        RedisValue res43 = db.ListLeftPop("bikes:repairs");
        Console.WriteLine(res43);   // >>> "bike:2"

        RedisValue res44 = db.ListLeftPop("bikes:repairs");
        Console.WriteLine(res44);   //  >>> "bike:1"

        bool res45 = db.KeyExists("bikes:repairs");
        Console.WriteLine(res45);   // >>> False
        //STEP_END

        //REMOVE_START
        Assert.Equal(3, res40);
        Assert.True(res41);
        Assert.Equal("bike:3", res42);
        Assert.Equal("bike:2", res43);
        Assert.Equal("bike:1", res44);
        Assert.False(res45);
        //REMOVE_END

        //STEP_START rule_3
        bool res46 = db.KeyDelete("bikes:repairs");
        Console.WriteLine(res46);   // >>> False

        long res47 = db.ListLength("bikes:repairs");
        Console.WriteLine(res47);   // >>> 0

        RedisValue res48 = db.ListLeftPop("bikes:repairs");
        Console.WriteLine(res48);   // >>> Null
        //STEP_END

        //REMOVE_START
        Assert.False(res46);
        Assert.Equal(0, res47);
        Assert.Equal(RedisValue.Null, res48);
        //REMOVE_END

        //STEP_START ltrim.1
        long res49 = db.ListLeftPush("bikes:repairs", ["bike:1", "bike:2", "bike:3", "bike:4", "bike:5"]);
        Console.WriteLine(res49);   // >>> 5

        db.ListTrim("bikes:repairs", 0, 2);
        RedisValue[] res50 = db.ListRange("bikes:repairs", 0, -1);
        Console.WriteLine(string.Join(", ", res50));    // >>> "bike:5, bike:4, bike:3"
        //STEP_END

        //REMOVE_START
        Assert.Equal(5, res49);
        Assert.Equal("bike:5, bike:4, bike:3", string.Join(", ", res50));
        //REMOVE_END

        //HIDE_START
    }
}
//HIDE_END
