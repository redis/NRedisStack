// EXAMPLE: bitfield_tutorial
// HIDE_START

using NRedisStack.RedisStackCommands;
using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class Bitfield_tutorial
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete("bike:1:stats");
        //REMOVE_END
        // HIDE_END


        // STEP_START bf
        long res1 = db.StringIncrement("bike:1:stats", 1000);
        Console.WriteLine(res1);    // >>> 1000

        long res2 = db.StringDecrement("bike:1:stats", 50);
        Console.WriteLine(res2);    // >>> 950

        long res3 = db.StringIncrement("bike:1:stats", 500);
        Console.WriteLine(res3);    // >>> 1450
                                    // STEP_END

        // Tests for 'bf' step.
        // REMOVE_START
        Assert.Equal(1000, res1);
        Assert.Equal(950, res2);
        Assert.Equal(1450, res3);
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

