// EXAMPLE: set_and_get
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

//REMOVE_START
namespace Doc;
[Collection("DocsTests")]
//REMOVE_END
public class SetGetExample
{
    private readonly ITestOutputHelper testOutputHelper;

    public SetGetExample(ITestOutputHelper testOutputHelper)
    {
        this.testOutputHelper = testOutputHelper;
    }

    [SkipIfRedis(Is.OSSCluster)]
    public void run()
    {
        var redis = ConnectionMultiplexer.Connect("localhost:6379");
        var db = redis.GetDatabase();

        //HIDE_END
        bool status = db.StringSet("bike:1", "Process 134");

        if (status)
            testOutputHelper.WriteLine("Successfully added a bike.");

        var value = db.StringGet("bike:1");

        if (value.HasValue)
            testOutputHelper.WriteLine("The name of the bike is: " + value + ".");

        //REMOVE_START
        Assert.True(status);
        Assert.Equal("Process 134", value.ToString());
        //REMOVE_END
        //HIDE_START
    }
}
//HIDE_END
