// EXAMPLE: pipe_trans_tutorial
using NRedisStack;
using StackExchange.Redis;
//REMOVE_START
using NRedisStack.Tests;

namespace Doc;
[Collection("DocsTests")]
//REMOVE_END
public class PipeTransExample
// REMOVE_START
: AbstractNRedisStackTest, IDisposable
// REMOVE_END
{
    // REMOVE_START
    public PipeTransExample(EndpointsFixture fixture) : base(fixture) { }

    [SkippableFact]
    // REMOVE_END
    public async Task run()
    {
        //REMOVE_START
        // This is needed because we're constructing ConfigurationOptions in the test before calling GetConnection
        SkipIfTargetConnectionDoesNotExist(EndpointsFixture.Env.Standalone);
        var _ = GetCleanDatabase(EndpointsFixture.Env.Standalone);
        //REMOVE_END
        var muxer = ConnectionMultiplexer.Connect("localhost:6379");
        var db = muxer.GetDatabase();
        // REMOVE_START
        db.KeyDelete(new RedisKey[] {
            "counter:1", "counter:2", "counter:3",
            "seat:0", "seat:1", "seat:2", "seat:3", "seat:4",
            "customer:39182",
            "Details"
        });
        // REMOVE_END

        // STEP_START basic_pipe
        var pipeline = new Pipeline(db);

        for (int i = 0; i < 5; i++)
        {
            pipeline.Db.StringSetAsync($"seat:{i}", $"#{i}");
        }
        pipeline.Execute();

        var resp1 = db.StringGet("seat:0");
        Console.WriteLine(resp1); // >>> #0

        var resp2 = db.StringGet("seat:3");
        Console.WriteLine(resp2); // >>> #3

        var resp3 = db.StringGet("seat:4");
        Console.WriteLine(resp2); // >>> #4
        // STEP_END
        // REMOVE_START
        Assert.Equal("#0", resp1);
        Assert.Equal("#3", resp2);
        Assert.Equal("#4", resp3);
        // REMOVE_END

        // STEP_START basic_trans
        var trans = new Transaction(db);

        trans.Db.StringIncrementAsync("counter:1", 1);
        trans.Db.StringIncrementAsync("counter:2", 2);
        trans.Db.StringIncrementAsync("counter:3", 3);

        trans.Execute();

        var resp4 = db.StringGet("counter:1");
        Console.WriteLine(resp4); // >>> 1

        var resp5 = db.StringGet("counter:2");
        Console.WriteLine(resp5); // >>> 2

        var resp6 = db.StringGet("counter:3");
        Console.WriteLine(resp6);  // >>> 3
        // STEP_END
        // REMOVE_START
        Assert.Equal("1", resp4);
        Assert.Equal("2", resp5);
        Assert.Equal("3", resp6);
        // REMOVE_END

        // STEP_START trans_watch
        var watchedTrans = new Transaction(db);

        watchedTrans.AddCondition(Condition.KeyNotExists("customer:39182"));

        watchedTrans.Db.HashSetAsync(
            "customer:39182",
            new HashEntry[]{
                new HashEntry("name", "David"),
                new HashEntry("age", "27")
            }
        );

        bool succeeded = watchedTrans.Execute();
        Console.WriteLine(succeeded); // >>> true
        // STEP_END
        // REMOVE_START
        Assert.True(succeeded);
        // REMOVE_END

        // STEP_START when_condition
        bool resp7 = db.HashSet("Details", "SerialNumber", "12345");
        Console.WriteLine(resp7); // >>> true

        db.HashSet("Details", "SerialNumber", "12345A", When.NotExists);
        string resp8 = db.HashGet("Details", "SerialNumber");
        Console.WriteLine(resp8); // >>> 12345

        db.HashSet("Details", "SerialNumber", "12345A");
        string resp9 = db.HashGet("Details", "SerialNumber");
        Console.WriteLine(resp9); // >>> 12345A
        // STEP_END
        // REMOVE_START
        Assert.True(resp7);
        Assert.Equal("12345", resp8);
        Assert.Equal("12345A", resp9);
        // REMOVE_END
    }
}