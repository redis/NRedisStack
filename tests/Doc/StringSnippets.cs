// EXAMPLE: set_tutorial
// HIDE_START

//REMOVE_START

using NRedisStack.Tests;
using StackExchange.Redis;

namespace Doc;
[Collection("DocsTests")]
//REMOVE_END
public class StringSnippets
{
    private readonly ITestOutputHelper testOutputHelper;

    public StringSnippets(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper;
    }

    //REMOVE_START
    [SkipIfRedis(Is.OSSCluster)]
    //REMOVE_END
    public void run()
    {
        var redis = ConnectionMultiplexer.Connect("localhost:6379");
        var db = redis.GetDatabase();

        //HIDE_END

        //REMOVE_START
        db.KeyDelete(new RedisKey[] { "bike:1", "bike:2", "bike:3", "total_crashes" });
        //REMOVE_END

        // STEP_START set_get
        var res1 = db.StringSet("bike:1", "Deimos");
        testOutputHelper.WriteLine(res1.ToString()); // true
        var res2 = db.StringGet("bike:1");
        testOutputHelper.WriteLine(res2); // Deimos
        // STEP_END

        //REMOVE_START
        Assert.True(res1);
        Assert.Equal("Deimos", res2);
        //REMOVE_END

        //STEP_START setnx_xx
        var res3 = db.StringSet("bike:1", "bike", when: When.NotExists);
        testOutputHelper.WriteLine(res3.ToString()); // false
        testOutputHelper.WriteLine(db.StringGet("bike:1"));
        var res4 = db.StringSet("bike:1", "bike", when: When.Exists);
        testOutputHelper.WriteLine(res4.ToString()); // true
        //STEP_END

        //REMOVE_START
        Assert.False(res3);
        Assert.True(res4);
        //REMOVE_END

        //STEP_START mset
        var res5 = db.StringSet(new KeyValuePair<RedisKey, RedisValue>[]
        {
            new ("bike:1", "Deimos"), new("bike:2", "Ares"), new("bike:3", "Vanth")
        });
        testOutputHelper.WriteLine(res5.ToString());
        var res6 = db.StringGet(new RedisKey[] { "bike:1", "bike:2", "bike:3" });
        testOutputHelper.WriteLine(res6.ToString());
        //STEP_END

        //REMOVE_START
        Assert.True(res5);
        Assert.Equal(new[] { "Deimos", "Ares", "Vanth" }, res6.Select(x => x.ToString()).ToArray());
        //REMOVE_END

        //STEP_START incr
        db.StringSet("total_crashes", 0);
        var res7 = db.StringIncrement("total_crashes");
        testOutputHelper.WriteLine(res7.ToString()); // 1
        var res8 = db.StringIncrement("total_crashes", 10);
        testOutputHelper.WriteLine(res8.ToString());
        //STEP_END

        //REMOVE_START
        Assert.Equal(1, res7);
        Assert.Equal(11, res8);
        //REMOVE_END
    }
}