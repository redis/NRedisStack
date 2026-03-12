// EXAMPLE: bf_tutorial
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
public class BfTutorial
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public BfTutorial(EndpointsFixture fixture) : base(fixture) { }

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
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete("bikes:models");
        //REMOVE_END
        // HIDE_END


        // STEP_START bloom
        bool res1 = db.BF().Reserve("bikes:models", 0.01, 1000);
        Console.WriteLine(res1);    // >>> True

        bool res2 = db.BF().Add("bikes:models", "Smoky Mountain Striker");
        Console.WriteLine(res2);    // >>> True

        bool res3 = db.BF().Exists("bikes:models", "Smoky Mountain Striker");
        Console.WriteLine(res3);    // >>> True

        bool[] res4 = db.BF().MAdd("bikes:models", new RedisValue[]{
            "Rocky Mountain Racer",
            "Cloudy City Cruiser",
            "Windy City Wippet"
        });
        Console.WriteLine(string.Join(", ", res4)); // >>> True, True, True

        bool[] res5 = db.BF().MExists("bikes:models", [
            "Rocky Mountain Racer",
            "Cloudy City Cruiser",
            "Windy City Wippet"
        ]);
        Console.WriteLine(string.Join(", ", res5)); // >>> True, True, True
        // STEP_END

        // Tests for 'bloom' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.True(res2);
        Assert.True(res3);
        Assert.Equal("True, True, True", string.Join(", ", res4));
        Assert.Equal("True, True, True", string.Join(", ", res5));
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

