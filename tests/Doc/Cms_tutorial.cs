// EXAMPLE: cms_tutorial
// HIDE_START

using NRedisStack.CountMinSketch.DataTypes;
using NRedisStack.RedisStackCommands;
using NRedisStack.Tests;
using StackExchange.Redis;

// HIDE_END

// REMOVE_START
namespace Doc;
[Collection("DocsTests")]
// REMOVE_END

// HIDE_START
public class Cms_tutorial
{

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        //REMOVE_START
        // Clear any keys here before using them in tests.
        db.KeyDelete("bikes:profit");
        //REMOVE_END
        // HIDE_END


        // STEP_START cms
        bool res1 = db.CMS().InitByProb("bikes:profit", 0.001, 0.002);
        Console.WriteLine(res1);    // >>> True

        long res2 = db.CMS().IncrBy("bikes:profit", "Smoky Mountain Striker", 100);
        Console.WriteLine(res2);    // >>> 100

        long[] res3 = db.CMS().IncrBy("bikes:profit",
            new Tuple<RedisValue, long>[]{
                new Tuple<RedisValue, long>("Rocky Mountain Racer", 200),
                new Tuple<RedisValue, long>("Cloudy City Cruiser", 150)
            }
        );
        Console.WriteLine(string.Join(", ", res3)); // >>> 200, 150

        long[] res4 = db.CMS().Query("bikes:profit", new RedisValue[] { "Smoky Mountain Striker" });
        Console.WriteLine(string.Join(", ", res4)); // >>> 100

        CmsInformation res5 = db.CMS().Info("bikes:profit");
        Console.WriteLine($"Width: {res5.Width}, Depth: {res5.Depth}, Count: {res5.Count}");
        // >>> Width: 2000, Depth: 9, Count: 450
        // STEP_END

        // Tests for 'cms' step.
        // REMOVE_START
        Assert.True(res1);
        Assert.Equal(100, res2);
        Assert.Equal("200, 150", string.Join(", ", res3));
        Assert.Equal("100", string.Join(", ", res4));
        Assert.Equal(2000, res5.Width);
        Assert.Equal(9, res5.Depth);
        Assert.Equal(450, res5.Count);
        // REMOVE_END


        // HIDE_START
    }
}
// HIDE_END

