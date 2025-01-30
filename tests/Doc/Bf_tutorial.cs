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
public class Bf_tutorial : AbstractNRedisStackTest, IDisposable
{
    public Bf_tutorial(EndpointsFixture fixture) : base(fixture) { }
    
    [SkippableTheory]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void run(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
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

        bool[] res5 = db.BF().MExists("bikes:models", new RedisValue[]{
            "Rocky Mountain Racer",
            "Cloudy City Cruiser",
            "Windy City Wippet"
        });
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

