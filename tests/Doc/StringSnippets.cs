// EXAMPLE: set_tutorial
// HIDE_START

//REMOVE_START

using NRedisStack.Tests;
using StackExchange.Redis;

namespace NRedisStack.Doc;
[Collection("DocsTests")]
//REMOVE_END
public class StringSnippets
{
    //REMOVE_START
    [SkipIfRedis(Is.OSSCluster)]
    //REMOVE_END
    public void run()
    {
        var redis = ConnectionMultiplexer.Connect("localhost:6379");
        var db = redis.GetDatabase();
        
        //HIDE_END

        //REMOVE_START
        db.KeyDelete(new RedisKey[] {"bike:1", "bike:2", "bike:3", "total_crashes"});
        //REMOVE_END
        
        // STEP_START set_get
        var res1 = db.StringSet("bike:1", "Deimos");
        Console.WriteLine(res1); // true
        var res2 = db.StringGet("bike:1");
        Console.WriteLine(res2); // Deimos
        // STEP_END
        
        //REMOVE_START
        Assert.True(res1);
        Assert.Equal("Deimos", res2);
        //REMOVE_END
        
        //STEP_START setnx_xx
        var res3 = db.StringSet("bike:1", "bike", when: When.NotExists);
        Console.WriteLine(res3); // false
        Console.WriteLine(db.StringGet("bike:1"));
        var res4 = db.StringSet("bike:1", "bike", when: When.Exists);
        Console.WriteLine(res4); // true
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
        Console.WriteLine(res5);
        var res6 = db.StringGet(new RedisKey[] { "bike:1", "bike:2", "bike:3" });
        Console.WriteLine(res6);
        //STEP_END
        
        //REMOVE_START
        Assert.True(res5);
        Assert.Equal(new[]{"Deimos", "Ares", "Vanth"}, res6.Select(x=>x.ToString()).ToArray());
        //REMOVE_END
        
        //STEP_START incr
        db.StringSet("total_crashes", 0);
        var res7 = db.StringIncrement("total_crashes");
        Console.WriteLine(res7); // 1
        var res8 = db.StringIncrement("total_crashes", 10);
        Console.WriteLine(res8);
        //STEP_END
        
        //REMOVE_START
        Assert.Equal(1, res7);
        Assert.Equal(11, res8);
        //REMOVE_END
    }
}