// EXAMPLE: set_and_get
// HIDE_START

using NRedisStack.Tests;
using StackExchange.Redis;

//REMOVE_START
namespace Doc;

[Collection("DocsTests")]
//REMOVE_END
public class SetGetExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public SetGetExample(EndpointsFixture fixture) : base(fixture) { }

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

        //HIDE_END
        bool status = db.StringSet("bike:1", "Process 134");

        if (status)
            Console.WriteLine("Successfully added a bike.");

        var value = db.StringGet("bike:1");

        if (value.HasValue)
            Console.WriteLine("The name of the bike is: " + value + ".");

        //REMOVE_START
        Assert.True(status);
        Assert.Equal("Process 134", value.ToString());
        //REMOVE_END
        //HIDE_START
    }
}
//HIDE_END
