using Xunit;
using NRedisStack.Core;
using NRedisStack;
using static NRedisStack.Auxiliary;
using StackExchange.Redis;
using System.Xml.Linq;
using System.Reflection;
using NRedisStack.RedisStackCommands;


namespace NRedisStack.Tests.Core;

public class CoreTests : AbstractNRedisStackTest, IDisposable
{
    private readonly string key = "CORE_TESTS";
    public CoreTests(RedisFixture redisFixture) : base(redisFixture) { }

    [SkipIfRedis(Comparison.LessThan, "7.1.242")]
    public void TestSetInfoDefaultValue()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase();

        db.Execute("FLUSHALL");
        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack;.NET-{Environment.Version} lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Comparison.LessThan, "7.1.242")]
    public void TestSetInfoWithValue()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase("MyLibraryName;v1.0.0");

        db.Execute("FLUSHALL");
        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"NRedisStack(MyLibraryName;v1.0.0);.NET-{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    // [SkipIfRedis(Comparison.LessThan, "7.1.242")]
    // public void TestSetInfoNull()
    // {
    //     var redis = ConnectionMultiplexer.Connect("localhost");
    //     var db = redis.GetDatabase(null);

    //     db.Execute("FLUSHALL");
    //     db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

    //     var info = db.Execute("CLIENT", "INFO").ToString();
    //     Assert.EndsWith($"lib-name= lib-ver=\n", info);
    // }
}