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
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public Hll_tutorial(EndpointsFixture fixture) : base(fixture) { }

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
        // Clear any keys here before using them in tests.
        db.KeyDelete(new RedisKey[] { "{bikes}", "commuter_{bikes}", "all_{bikes}" });
        //REMOVE_END
        // HIDE_END


        // STEP_START pfadd
        bool res1 = db.HyperLogLogAdd("{bikes}", new RedisValue[] { "Hyperion", "Deimos", "Phoebe", "Quaoar" });
        Console.WriteLine(res1);    // >>> True

        long res2 = db.HyperLogLogLength("{bikes}");
        Console.WriteLine(res2);    // >>> 4

        bool res3 = db.HyperLogLogAdd("commuter_{bikes}", new RedisValue[] { "Salacia", "Mimas", "Quaoar" });
        Console.WriteLine(res3);    // >>> True

        db.HyperLogLogMerge("all_{bikes}", "{bikes}", "commuter_{bikes}");
        long res4 = db.HyperLogLogLength("all_{bikes}");
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

