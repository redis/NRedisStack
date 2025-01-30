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
public class Cuckoo_tutorial : AbstractNRedisStackTest, IDisposable
{
    public Cuckoo_tutorial(EndpointsFixture fixture) : base(fixture) { }

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

