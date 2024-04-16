// EXAMPLE: hll_tutorial
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class Hll_tutorial
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete(new RedisKey[] { "bikes", "commuter_bikes", "all_bikes" });
        //REMOVE_END
        // HIDE_END


        // STEP_START pfadd
        bool res1 = db.HyperLogLogAdd("bikes", new RedisValue[] { "Hyperion", "Deimos", "Phoebe", "Quaoar" });
        Console.WriteLine(res1);    // >>> True

        long res2 = db.HyperLogLogLength("bikes");
        Console.WriteLine(res2);    // >>> 4

        bool res3 = db.HyperLogLogAdd("commuter_bikes", new RedisValue[] { "Salacia", "Mimas", "Quaoar" });
        Console.WriteLine(res3);    // >>> True

        db.HyperLogLogMerge("all_bikes", "bikes", "commuter_bikes");
        long res4 = db.HyperLogLogLength("all_bikes");
        Console.WriteLine(res4);    // >>> 6
        // STEP_END

        // Tests for 'pfadd' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.Equal(4, res2);
        Assert.True(res3);
        Assert.Equal(6, res4);
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

