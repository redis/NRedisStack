// EXAMPLE: home_prob_dts
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;

// REMOVE_START
using NRedisStack.Tests;

namespace Doc;

[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class HomeProbExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public HomeProbExample(EndpointsFixture fixture) : base(fixture) { }

    [Fact]
    // REMOVE_END
    public void Run()
    {
        // REMOVE_START
        // This is needed because we're constructing ConfigurationOptions in the test before calling GetConnection
        SkipIfTargetConnectionDoesNotExist(EndpointsFixture.Env.Standalone);
        var _ = GetCleanDatabase(EndpointsFixture.Env.Standalone);
        // REMOVE_END

        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        // REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete([
            "recorded_users", "other_users",
            "group:1", "group:2", "both_groups",
            "items_sold",
            "male_heights", "female_heights", "all_heights",
            "top_3_songs"
        ]);
        // REMOVE_END
        // HIDE_END

        // STEP_START bloom
        bool[] res1 = db.BF().MAdd(
            "recorded_users", "andy", "cameron", "david", "michelle"
        );
        Console.WriteLine(string.Join(", ", res1));
        // >>> true, true, true, true

        bool res2 = db.BF().Exists("recorded_users", "cameron");
        Console.WriteLine(res2); // >>> true

        bool res3 = db.BF().Exists("recorded_users", "kaitlyn");
        Console.WriteLine(res3); // >>> false
        // STEP_END
        // REMOVE_START
        Assert.Equal(new[] { true, true, true, true }, res1);
        // REMOVE_END

        // STEP_START cuckoo
        bool res4 = db.CF().Add("other_users", "paolo");
        Console.WriteLine(res4); // >>> true

        bool res5 = db.CF().Add("other_users", "kaitlyn");
        Console.WriteLine(res5); // >>> true

        bool res6 = db.CF().Add("other_users", "rachel");
        Console.WriteLine(res6); // >>> true

        bool[] res7 = db.CF().MExists("other_users", "paolo", "rachel", "andy");
        Console.WriteLine(string.Join(", ", res7));
        // >>> true, true, false

        bool res8 = db.CF().Del("other_users", "paolo");
        Console.WriteLine(res8); // >>> true

        bool res9 = db.CF().Exists("other_users", "paolo");
        Console.WriteLine(res9); // >>> false
        // STEP_END
        // REMOVE_START
        Assert.True(res4);
        Assert.True(res5);
        Assert.True(res6);
        Assert.Equal(new[] { true, true, false }, res7);
        Assert.True(res8);
        Assert.False(res9);
        // REMOVE_END

        // STEP_START hyperloglog
        bool res10 = db.HyperLogLogAdd(
            "group:1",
            ["andy", "cameron", "david"]
        );
        Console.WriteLine(res10); // >>> true

        long res11 = db.HyperLogLogLength("group:1");
        Console.WriteLine(res11); // >>> 3

        bool res12 = db.HyperLogLogAdd(
            "group:2",
            ["kaitlyn", "michelle", "paolo", "rachel"]
        );
        Console.WriteLine(res12); // >>> true

        long res13 = db.HyperLogLogLength("group:2");
        Console.WriteLine(res13); // >>> 4

        db.HyperLogLogMerge(
            "both_groups",
            "group:1", "group:2"
        );

        long res14 = db.HyperLogLogLength("both_groups");
        Console.WriteLine(res14); // >>> 7
        // STEP_END
        // REMOVE_START
        Assert.True(res10);
        Assert.Equal(3, res11);
        Assert.True(res12);
        Assert.Equal(4, res13);
        Assert.Equal(7, res14);
        // REMOVE_END

        // STEP_START cms
        // Specify that you want to keep the counts within 0.01
        // (0.1%) of the true value with a 0.005 (0.05%) chance
        // of going outside this limit.
        bool res15 = db.CMS().InitByProb("items_sold", 0.01, 0.005);
        Console.WriteLine(res15); // >>> true

        long[] res16 = db.CMS().IncrBy(
            "items_sold",
            [
                new("bread", 300),
                new("tea", 200),
                new("coffee", 200),
                new("beer", 100)
            ]
        );
        Console.WriteLine(string.Join(", ", res16));
        // >>> 300, 200, 200, 100

        long[] res17 = db.CMS().IncrBy(
            "items_sold",
            [
                new("bread", 100),
                new("coffee", 150)
            ]
        );
        Console.WriteLine(string.Join(", ", res17));
        // >>> 400, 350

        long[] res18 = db.CMS().Query(
            "items_sold",
            "bread", "tea", "coffee", "beer"
        );
        Console.WriteLine(string.Join(", ", res18));
        // >>> 400, 200, 350, 100
        // STEP_END
        // REMOVE_START
        Assert.True(res15);
        Assert.Equal(new long[] { 300, 200, 200, 100 }, res16);
        Assert.Equal(new long[] { 400, 350 }, res17);
        Assert.Equal(new long[] { 400, 200, 350, 100 }, res18);
        // REMOVE_END

        // STEP_START tdigest
        bool res19 = db.TDIGEST().Create("male_heights");
        Console.WriteLine(res19); // >>> true

        bool res20 = db.TDIGEST().Add(
            "male_heights",
            175.5, 181, 160.8, 152, 177, 196, 164
        );
        Console.WriteLine(res20); // >>> true

        double res21 = db.TDIGEST().Min("male_heights");
        Console.WriteLine(res21); // >>> 152.0

        double res22 = db.TDIGEST().Max("male_heights");
        Console.WriteLine(res22); // >>> 196.0

        double[] res23 = db.TDIGEST().Quantile("male_heights", 0.75);
        Console.WriteLine(string.Join(", ", res23)); // >>> 181.0

        // Note that the CDF value for 181.0 is not exactly
        // 0.75. Both values are estimates.
        double[] res24 = db.TDIGEST().CDF("male_heights", 181.0);
        Console.WriteLine(string.Join(", ", res24)); // >>> 0.7857142857142857

        bool res25 = db.TDIGEST().Create("female_heights");
        Console.WriteLine(res25); // >>> true

        bool res26 = db.TDIGEST().Add(
            "female_heights",
            155.5, 161, 168.5, 170, 157.5, 163, 171
        );
        Console.WriteLine(res26); // >>> true

        double[] res27 = db.TDIGEST().Quantile("female_heights", 0.75);
        Console.WriteLine(string.Join(", ", res27)); // >>> 170.0

        // Specify 0 for `compression` and false for `override`.
        bool res28 = db.TDIGEST().Merge(
            "all_heights", 0, false, "male_heights", "female_heights"
        );
        Console.WriteLine(res28); // >>> true

        double[] res29 = db.TDIGEST().Quantile("all_heights", 0.75);
        Console.WriteLine(string.Join(", ", res29)); // >>> 175.5
        // STEP_END
        // REMOVE_START
        Assert.True(res19);
        Assert.True(res20);
        Assert.Equal(152.0, res21);
        Assert.Equal(196.0, res22);
        Assert.Equal(new[] { 181.0 }, res23);
        Assert.Equal(new[] { 0.7857142857142857 }, res24);
        Assert.True(res25);
        Assert.True(res26);
        Assert.Equal(new[] { 170.0 }, res27);
        Assert.True(res28);
        Assert.Equal(new[] { 175.5 }, res29);
        // REMOVE_END

        // STEP_START topk
        bool res30 = db.TOPK().Reserve("top_3_songs", 3, 7, 8, 0.9);
        Console.WriteLine(res30); // >>> true

        RedisResult[] res31 = db.TOPK().IncrBy(
            "top_3_songs",
            new Tuple<RedisValue, long>[] {
                new("Starfish Trooper", 3000),
                new("Only one more time", 1850),
                new("Rock me, Handel", 1325),
                new("How will anyone know?", 3890),
                new("Average lover", 4098),
                new("Road to everywhere", 770)
            }
        );
        Console.WriteLine(
            string.Join(
                ", ",
                string.Join(
                    ", ",
                    res31.Select(
                        r => $"{(r.IsNull ? "Null" : r)}"
                    )
                )
            )
        );
        // >>> Null, Null, Null, Rock me, Handel, Only one more time, Null

        RedisResult[] res32 = db.TOPK().List("top_3_songs");
        Console.WriteLine(
            string.Join(
                ", ",
                string.Join(
                    ", ",
                    res32.Select(
                        r => $"{(r.IsNull ? "Null" : r)}"
                    )
                )
            )
        );
        // >>> Average lover, How will anyone know?, Starfish Trooper

        bool[] res33 = db.TOPK().Query(
            "top_3_songs",
            "Starfish Trooper", "Road to everywhere"
        );
        Console.WriteLine(string.Join(", ", res33));
        // >>> true, false
        // STEP_END
        // REMOVE_START
        Assert.True(res30);
        Assert.Equal(
            "Null, Null, Null, Rock me, Handel, Only one more time, Null",
            string.Join(
                ", ",
                string.Join(
                    ", ",
                    res31.Select(
                        r => $"{(r.IsNull ? "Null" : r)}"
                    )
                )
            )
        );

        Assert.Equal(
            "Average lover, How will anyone know?, Starfish Trooper",
            string.Join(
                ", ",
                string.Join(
                    ", ",
                    res32.Select(
                        r => $"{(r.IsNull ? "Null" : r)}"
                    )
                )
            )
        );

        Assert.Equal(new[] { true, false }, res33);
        // REMOVE_END
    }
}