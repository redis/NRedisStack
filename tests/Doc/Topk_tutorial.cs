// EXAMPLE: topk_tutorial
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
public class TopkTutorial
// REMOVE_START
    (EndpointsFixture fixture) : AbstractNRedisStackTest(fixture), IDisposable
// REMOVE_END
{
    // REMOVE_START

    [SkippableFact]
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
        db.KeyDelete("bikes:keywords");
        //REMOVE_END
        // HIDE_END


        // STEP_START topk
        bool res1 = db.TOPK().Reserve("bikes:keywords", 5, 2000, 7, 0.925);
        Console.WriteLine(res1);    // >>> True

        RedisResult[]? res2 = db.TOPK().Add("bikes:keywords",
                "store",
                "seat",
                "handlebars",
                "handles",
                "pedals",
                "tires",
                "store",
                "seat"
        );

        if (res2 is not null)
        {
            Console.WriteLine(string.Join(", ", string.Join(", ", res2.Select(r => $"{(r.IsNull ? "Null" : r)}"))));
            // >>> Null, Null, Null, Null, Null, handlebars, Null, Null
        }

        RedisResult[] res3 = db.TOPK().List("bikes:keywords");

        if (res3 is not null)
        {
            Console.WriteLine(string.Join(", ", string.Join(", ", res3.Select(r => $"{(r.IsNull ? "Null" : r)}"))));
            // >>> store, seat, pedals, tires, handles
        }

        bool[] res4 = db.TOPK().Query("bikes:keywords", "store", "handlebars");
        Console.WriteLine(string.Join(", ", res4)); // >>> True, False
        // STEP_END

        // Tests for 'topk' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.NotNull(res2);
        Assert.Equal(
            "Null, Null, Null, Null, Null, handlebars, Null, Null",
            string.Join(", ", string.Join(", ", res2.Select(r => $"{(r.IsNull ? "Null" : r)}")))
        );
        Assert.NotNull(res3);
        Assert.Equal(
            "store, seat, pedals, tires, handles",
            string.Join(", ", string.Join(", ", res3.Select(r => $"{(r.IsNull ? "Null" : r)}")))
        );
        Assert.Equal("True, False", string.Join(", ", res4));
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

