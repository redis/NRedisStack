// EXAMPLE: cuckoo_tutorial
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
public class Cuckoo_tutorial
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START

    public Cuckoo_tutorial(EndpointsFixture fixture) : base(fixture) { }

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
        db.KeyDelete("bikes:models");
        //REMOVE_END
        // HIDE_END


        // STEP_START cuckoo
        bool res1 = db.CF().Reserve("bikes:models", 1000000);
        Console.WriteLine(res1);    // >>> True

        bool res2 = db.CF().Add("bikes:models", "Smoky Mountain Striker");
        Console.WriteLine(res2);    // >>> True

        bool res3 = db.CF().Exists("bikes:models", "Smoky Mountain Striker");
        Console.WriteLine(res3);    // >>> True

        bool res4 = db.CF().Exists("bikes:models", "Terrible Bike Name");
        Console.WriteLine(res4);    // >>> False

        bool res5 = db.CF().Del("bikes:models", "Smoky Mountain Striker");
        Console.WriteLine(res5);    // >>> True
        // STEP_END

        // Tests for 'cuckoo' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.True(res2);
        Assert.True(res3);
        Assert.False(res4);
        Assert.True(res5);
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

