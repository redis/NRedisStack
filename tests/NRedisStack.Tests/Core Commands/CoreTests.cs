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
    public CoreTests(RedisFixture redisFixture) : base(redisFixture) { }


    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestSimpleSetInfo()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.ClientSetInfo(SetInfoAttr.LibraryName, "TestLibraryName");
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, "1.2.3");

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=TestLibraryName lib-ver=1.2.3\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestSimpleSetInfoAsync()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.ClientSetInfoAsync(SetInfoAttr.LibraryName, "TestLibraryName");
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, "1.2.3");

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=TestLibraryName lib-ver=1.2.3\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestSetInfoDefaultValue()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoDefaultValueAsync()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.ExecuteAsync(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestSetInfoWithValue()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase("MyLibraryName;v1.0.0");
        db.Execute("FLUSHALL");

        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"NRedisStack(MyLibraryName;v1.0.0;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoWithValueAsync()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase("MyLibraryName;v1.0.0");
        db.Execute("FLUSHALL");

        await db.ExecuteAsync(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith($"NRedisStack(MyLibraryName;v1.0.0;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestSetInfoNull()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase(null);

        db.Execute("FLUSHALL");
        var infoBefore = db.Execute("CLIENT", "INFO").ToString();
        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var infoAfter = db.Execute("CLIENT", "INFO").ToString();
        // Find the indices of "lib-name=" in the strings
        int infoAfterLibNameIndex = infoAfter!.IndexOf("lib-name=");
        int infoBeforeLibNameIndex = infoBefore!.IndexOf("lib-name=");

        // Extract the sub-strings starting from "lib-name="
        string infoAfterLibNameToEnd = infoAfter.Substring(infoAfterLibNameIndex);
        string infoBeforeLibNameToEnd = infoBefore.Substring(infoBeforeLibNameIndex);

        // Assert that the extracted sub-strings are equal
        Assert.Equal(infoAfterLibNameToEnd, infoBeforeLibNameToEnd);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoNullAsync()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var redis = ConnectionMultiplexer.Connect("localhost");
        var db = redis.GetDatabase(null);

        db.Execute("FLUSHALL");
        var infoBefore = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        await db.ExecuteAsync(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var infoAfter = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        // Find the indices of "lib-name=" in the strings
        int infoAfterLibNameIndex = infoAfter!.IndexOf("lib-name=");
        int infoBeforeLibNameIndex = infoBefore!.IndexOf("lib-name=");

        // Extract the sub-strings starting from "lib-name="
        string infoAfterLibNameToEnd = infoAfter.Substring(infoAfterLibNameIndex);
        string infoBeforeLibNameToEnd = infoBefore.Substring(infoBeforeLibNameIndex);

        // Assert that the extracted sub-strings are equal
        Assert.Equal(infoAfterLibNameToEnd, infoBeforeLibNameToEnd);
    }
}