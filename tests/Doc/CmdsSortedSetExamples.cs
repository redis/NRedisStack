// EXAMPLE: cmds_sorted_set
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class CmdsSortedSet
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.

        //REMOVE_END
// HIDE_END


        // STEP_START bzmpop

        // STEP_END

        // Tests for 'bzmpop' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START bzpopmax

        // STEP_END

        // Tests for 'bzpopmax' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START bzpopmin

        // STEP_END

        // Tests for 'bzpopmin' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zadd
        RedisValue zAddResult1 = db.SortedSetAdd("myzset", "one", 1);
        Console.WriteLine(zAddResult1); // >>> 1

        RedisValue zAddResult2 = db.SortedSetAdd("myzset", "uno", 1);
        Console.WriteLine(zAddResult2);

        RedisValue zAddResult3 = db.SortedSetAdd(
            "myzset",
            new SortedSetEntry[] {
                new SortedSetEntry("two", 2),
                new SortedSetEntry("three", 3)
            }
        );
        Console.WriteLine(zAddResult3); // >>> 2

        SortedSetEntry[] zAddResult4 = db.SortedSetRangeByRankWithScores("myzset", 0, -1);
        Console.WriteLine($"{string.Join(", ", zAddResult4.Select(b => $"{b.Element}: {b.Score}"))}");
        // >>> one: 1, uno: 1, two: 2, three: 3
        // STEP_END

        // Tests for 'zadd' step.
        // REMOVE_START
        Assert.Equal(1, zAddResult1);
        Assert.Equal(1, zAddResult2);
        Assert.Equal(2, zAddResult3);
        Assert.Equal(
            "one: 1, uno: 1, two: 2, three: 3",
            string.Join(", ", zAddResult4.Select(b => $"{b.Element}: {b.Score}"))
        );
        db.KeyDelete("myzset");
        // REMOVE_END


        // STEP_START zcard

        // STEP_END

        // Tests for 'zcard' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zcount

        // STEP_END

        // Tests for 'zcount' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zdiff

        // STEP_END

        // Tests for 'zdiff' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zdiffstore

        // STEP_END

        // Tests for 'zdiffstore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zincrby

        // STEP_END

        // Tests for 'zincrby' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zinter

        // STEP_END

        // Tests for 'zinter' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zintercard

        // STEP_END

        // Tests for 'zintercard' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zinterstore

        // STEP_END

        // Tests for 'zinterstore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zlexcount

        // STEP_END

        // Tests for 'zlexcount' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zmpop

        // STEP_END

        // Tests for 'zmpop' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zmscore

        // STEP_END

        // Tests for 'zmscore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zpopmax

        // STEP_END

        // Tests for 'zpopmax' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zpopmin

        // STEP_END

        // Tests for 'zpopmin' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrandmember

        // STEP_END

        // Tests for 'zrandmember' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrange1

        // STEP_END

        // Tests for 'zrange1' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrange2

        // STEP_END

        // Tests for 'zrange2' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrange3

        // STEP_END

        // Tests for 'zrange3' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrangebylex

        // STEP_END

        // Tests for 'zrangebylex' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrangebyscore

        // STEP_END

        // Tests for 'zrangebyscore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrangestore

        // STEP_END

        // Tests for 'zrangestore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrank

        // STEP_END

        // Tests for 'zrank' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrem

        // STEP_END

        // Tests for 'zrem' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zremrangebylex

        // STEP_END

        // Tests for 'zremrangebylex' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zremrangebyrank

        // STEP_END

        // Tests for 'zremrangebyrank' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zremrangebyscore

        // STEP_END

        // Tests for 'zremrangebyscore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrevrange

        // STEP_END

        // Tests for 'zrevrange' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrevrangebylex

        // STEP_END

        // Tests for 'zrevrangebylex' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrevrangebyscore

        // STEP_END

        // Tests for 'zrevrangebyscore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zrevrank

        // STEP_END

        // Tests for 'zrevrank' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zscan

        // STEP_END

        // Tests for 'zscan' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zscore

        // STEP_END

        // Tests for 'zscore' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zunion

        // STEP_END

        // Tests for 'zunion' step.
        // REMOVE_START

        // REMOVE_END


        // STEP_START zunionstore

        // STEP_END

        // Tests for 'zunionstore' step.
        // REMOVE_START

        // REMOVE_END


// HIDE_START
    }
}
// HIDE_END

