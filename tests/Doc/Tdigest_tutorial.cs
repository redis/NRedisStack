// EXAMPLE: tdigest_tutorial
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
public class TdigestTutorial
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public TdigestTutorial(EndpointsFixture fixture) : base(fixture) { }

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
        db.KeyDelete("racer_ages");
        db.KeyDelete("bikes:sales");
        //REMOVE_END
        // HIDE_END


        // STEP_START tdig_start
        bool res1 = db.TDIGEST().Create("bikes:sales", 100);
        Console.WriteLine(res1);    // >>> True

        bool res2 = db.TDIGEST().Add("bikes:sales", 21);
        Console.WriteLine(res2);    // >>> True

        bool res3 = db.TDIGEST().Add("bikes:sales", 150, 95, 75, 34);
        Console.WriteLine(res3);    // >>> true
        // STEP_END

        // Tests for 'tdig_start' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.True(res2);
        Assert.True(res3);
        // REMOVE_END


        // STEP_START tdig_cdf
        bool res4 = db.TDIGEST().Create("racer_ages");
        Console.WriteLine(res4);    // >>> True

        bool res5 = db.TDIGEST().Add("racer_ages",
            45.88,
            44.2,
            58.03,
            19.76,
            39.84,
            69.28,
            50.97,
            25.41,
            19.27,
            85.71,
            42.63
        );
        Console.WriteLine(res5);    // >>> True

        long[] res6 = db.TDIGEST().Rank("racer_ages", 50);
        Console.WriteLine(string.Join(", ", res6)); // >>> 7

        long[] res7 = db.TDIGEST().Rank("racer_ages", 50, 40);
        Console.WriteLine(string.Join(", ", res7)); // >>> 7, 4
        // STEP_END

        // Tests for 'tdig_cdf' step.
        // REMOVE_START
        Assert.True(res4);
        Assert.True(res5);
        Assert.Equal("7", string.Join(", ", res6));
        Assert.Equal("7, 4", string.Join(", ", res7));
        // REMOVE_END


        // STEP_START tdig_quant
        double[] res8 = db.TDIGEST().Quantile("racer_ages", 0.5); ;
        Console.WriteLine(string.Join(", ", res8)); // >>> 44.2

        double[] res9 = db.TDIGEST().ByRank("racer_ages", 4);
        Console.WriteLine(string.Join(", ", res9)); // >>> 42.63
        // STEP_END

        // Tests for 'tdig_quant' step.
        // REMOVE_START
        Assert.Equal("44.2", string.Join(", ", res8));
        Assert.Equal("42.63", string.Join(", ", res9));
        // REMOVE_END


        // STEP_START tdig_min
        double res10 = db.TDIGEST().Min("racer_ages");
        Console.WriteLine(res10);   // >>> 19.27

        double res11 = db.TDIGEST().Max("racer_ages");
        Console.WriteLine(res11);   // >>> 85.71
        // STEP_END

        // Tests for 'tdig_min' step.
        // REMOVE_START
        Assert.Equal(19.27, res10);
        Assert.Equal(85.71, res11);
        // REMOVE_END


        // STEP_START tdig_reset
        bool res12 = db.TDIGEST().Reset("racer_ages");
        Console.WriteLine(res12);   // >>> True
        // STEP_END

        // Tests for 'tdig_reset' step.
        // REMOVE_START
        Assert.True(res12);
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

