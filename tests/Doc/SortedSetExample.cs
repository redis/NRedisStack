//EXAMPLE: ss_tutorial
//HIDE_START
//REMOVE_START
using NRedisStack.Tests;
//REMOVE_END
using StackExchange.Redis;

//REMOVE_START
namespace Doc;
[Collection("DocsTests")]
//REMOVE_END
public class SortedSetExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START

    public SortedSetExample(EndpointsFixture fixture) : base(fixture) { }

    [SkippableFact]
    // REMOVE_END
    public void run()
    {
        //REMOVE_START
        // This is needed because we're constructing ConfigurationOptions in the test before calling GetConnection
        SkipIfTargetConnectionDoesNotExist(EndpointsFixture.Env.Standalone);
        var _ = GetCleanDatabase(EndpointsFixture.Env.Standalone);
        //REMOVE_END
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        db.KeyDelete("racer_scores");
        //REMOVE_END
        //HIDE_END

        //STEP_START zadd
        bool res1 = db.SortedSetAdd("racer_scores", "Norem", 10);
        Console.WriteLine(res1); // >>> True
        //REMOVE_START
        Assert.True(res1);
        //REMOVE_END

        bool res2 = db.SortedSetAdd("racer_scores", "Castilla", 12);
        Console.WriteLine(res2); // >>> True
        //REMOVE_START
        Assert.True(res2);
        //REMOVE_END

        long res3 = db.SortedSetAdd("racer_scores", new[]{
            new SortedSetEntry("Sam-Bodden", 8),
            new SortedSetEntry("Royce", 10),
            new SortedSetEntry("Ford", 6),
            new SortedSetEntry("Prickett", 14),
            new SortedSetEntry("Castilla", 12)
        });
        Console.WriteLine(res3); // >>> 4
        //REMOVE_START
        Assert.Equal(4, res3);
        //REMOVE_END
        //STEP_END

        //STEP_START zrange
        RedisValue[] res4 = db.SortedSetRangeByRank("racer_scores", 0, -1);
        Console.WriteLine(string.Join(", ", res4)); // >>> Ford, Sam-Bodden, Norem, Royce, Castilla, Prickett
        //REMOVE_START
        Assert.Equal(6, res4.Length);
        Assert.Equal("Ford, Sam-Bodden, Norem, Royce, Castilla, Prickett", string.Join(", ", res4));
        //REMOVE_END

        RedisValue[] res5 = db.SortedSetRangeByRank("racer_scores", 0, -1, Order.Descending);
        Console.WriteLine(string.Join(", ", res5)); // >>> Prickett, Castilla, Royce, Norem, Sam-Bodden, Ford
        //REMOVE_START
        Assert.Equal(6, res5.Length);
        Assert.Equal("Prickett, Castilla, Royce, Norem, Sam-Bodden, Ford", string.Join(", ", res5));
        //REMOVE_END
        //STEP_END

        //STEP_START zrange_withscores
        SortedSetEntry[] res6 = db.SortedSetRangeByRankWithScores("racer_scores", 0, -1);
        Console.WriteLine(string.Join(", ", res6)); // >>> Ford: 6, Sam-Bodden: 8, Norem: 10, Royce: 10, Castilla: 12, Prickett: 14
        //REMOVE_START
        Assert.Equal(6, res6.Length);
        Assert.Equal("Ford: 6, Sam-Bodden: 8, Norem: 10, Royce: 10, Castilla: 12, Prickett: 14", string.Join(", ", res6));
        //REMOVE_END
        //STEP_END

        //STEP_START zrangebyscore
        RedisValue[] res7 = db.SortedSetRangeByScore("racer_scores", double.NegativeInfinity, 10);
        Console.WriteLine(string.Join(", ", res7)); // >>> Ford, Sam-Bodden, Norem, Royce
        //REMOVE_START
        Assert.Equal(4, res7.Length);
        Assert.Equal("Ford, Sam-Bodden, Norem, Royce", string.Join(", ", res7));
        //REMOVE_END
        //STEP_END

        //STEP_START zremrangebyscore
        bool res8 = db.SortedSetRemove("racer_scores", "Castilla");
        Console.WriteLine(res8); // >>> True
        //REMOVE_START
        Assert.True(res8);
        //REMOVE_END

        long res9 = db.SortedSetRemoveRangeByScore("racer_scores", double.NegativeInfinity, 9);
        Console.WriteLine(res9); // >>> 2
        //REMOVE_START
        Assert.Equal(2, res9);
        //REMOVE_END

        RedisValue[] res10 = db.SortedSetRangeByRank("racer_scores", 0, -1);
        Console.WriteLine(string.Join(", ", res10)); // >>> Norem, Royce, Prickett
        //REMOVE_START
        Assert.Equal(3, res10.Length);
        Assert.Equal("Norem, Royce, Prickett", string.Join(", ", res10));
        //REMOVE_END
        //STEP_END

        //REMOVE_START
        Assert.Equal(3, db.SortedSetLength("racer_scores"));
        //REMOVE_END

        //STEP_START zrank
        long? res11 = db.SortedSetRank("racer_scores", "Norem");
        Console.WriteLine(res11); // >>> 0
        //REMOVE_START
        Assert.Equal(0, res11);
        //REMOVE_END

        long? res12 = db.SortedSetRank("racer_scores", "Norem", Order.Descending);
        Console.WriteLine(res12); // >>> 2
        //REMOVE_START
        Assert.Equal(2, res12);
        //REMOVE_END
        //STEP_END

        //STEP_START zadd_lex
        long res13 = db.SortedSetAdd("racer_scores", new[] {
            new SortedSetEntry("Norem", 0),
            new SortedSetEntry("Sam-Bodden", 0),
            new SortedSetEntry("Royce", 0),
            new SortedSetEntry("Ford", 0),
            new SortedSetEntry("Prickett", 0),
            new SortedSetEntry("Castilla", 0)
        });
        Console.WriteLine(res13); // >>> 3
        //REMOVE_START
        Assert.Equal(3, res13);
        //REMOVE_END

        RedisValue[] res14 = db.SortedSetRangeByRank("racer_scores", 0, -1);
        Console.WriteLine(string.Join(", ", res14)); // >>> Castilla, Ford, Norem, Pricket, Royce, Sam-Bodden
        //REMOVE_START
        Assert.Equal(6, res14.Length);
        Assert.Equal("Castilla, Ford, Norem, Prickett, Royce, Sam-Bodden", string.Join(", ", res14));
        //REMOVE_END

        RedisValue[] res15 = db.SortedSetRangeByValue("racer_scores", "A", "L", Exclude.None);
        Console.WriteLine(string.Join(", ", res15)); // >>> Castilla, Ford
        //REMOVE_START
        Assert.Equal(2, res15.Length);
        Assert.Equal("Castilla, Ford", string.Join(", ", res15));
        //REMOVE_END
        //STEP_END

        //STEP_START leaderboard
        bool res16 = db.SortedSetAdd("racer_scores", "Wood", 100);
        Console.WriteLine(res16); // >>> True
        //REMOVE_START
        Assert.True(res16);
        //REMOVE_END

        bool res17 = db.SortedSetAdd("racer_scores", "Henshaw", 100);
        Console.WriteLine(res17); // >>> True
        //REMOVE_START
        Assert.True(res17);
        //REMOVE_END

        bool res18 = db.SortedSetAdd("racer_scores", "Henshaw", 150);
        Console.WriteLine(res18); // >>> False
        //REMOVE_START
        Assert.False(res18);
        //REMOVE_END

        double res19 = db.SortedSetIncrement("racer_scores", "Wood", 50);
        Console.WriteLine(res19); // >>> 150.0
        //REMOVE_START
        Assert.Equal(150, res19);
        //REMOVE_END

        double res20 = db.SortedSetIncrement("racer_scores", "Henshaw", 50);
        Console.WriteLine(res20); // >>> 200.0
        //REMOVE_START
        Assert.Equal(200, res20);
        //REMOVE_END
        //STEP_END
        //HIDE_START
    }
}
//HIDE_END
