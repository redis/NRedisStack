using Xunit;
using NRedisStack.Core;
using static NRedisStack.Auxiliary;
using StackExchange.Redis;
using NRedisStack.Core.DataTypes;
using NRedisStack.RedisStackCommands;


namespace NRedisStack.Tests.Core;

public class CoreTests : AbstractNRedisStackTest, IDisposable
{
    public CoreTests(RedisFixture redisFixture) : base(redisFixture) { }


    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestSimpleSetInfo()
    {
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.ClientSetInfo(SetInfoAttr.LibraryName, "TestLibraryName");
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, "1.2.3");

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=TestLibraryName lib-ver=1.2.3\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestSimpleSetInfoAsync()
    {
        var db = redisFixture.Redis.GetDatabase();
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
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoDefaultValueAsync()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.ExecuteAsync(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestSetInfoWithValue()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase("MyLibraryName;v1.0.0");
        db.Execute("FLUSHALL");

        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"NRedisStack(MyLibraryName;v1.0.0;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoWithValueAsync()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase("MyLibraryName;v1.0.0");
        db.Execute("FLUSHALL");

        await db.ExecuteAsync(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith($"NRedisStack(MyLibraryName;v1.0.0;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.1.242")]
    public void TestSetInfoNull()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase(null);

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
        var db = redisFixture.Redis.GetDatabase(null);

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

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.0.0")]
    public void TestBZMPop()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set";

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);
        db.SortedSetAdd(sortedSetKey, "c", 3.7);
        db.SortedSetAdd(sortedSetKey, "d", 9.4);
        db.SortedSetAdd(sortedSetKey, "e", 7.76);

        // Pop two items with Min modifier, which means it will pop the minimum values.
        var resultWithDefaultOrder = db.BZMPop(0, sortedSetKey, MinMaxModifier.Min, 2);

        Assert.NotNull(resultWithDefaultOrder);
        Assert.Equal(sortedSetKey, resultWithDefaultOrder!.Item1);
        Assert.Equal(2, resultWithDefaultOrder.Item2.Count);
        Assert.Equal("a", resultWithDefaultOrder.Item2[0].Value.ToString());
        Assert.Equal("c", resultWithDefaultOrder.Item2[1].Value.ToString());

        // Pop one more item, with Max modifier, which means it will pop the maximum value.
        var resultWithDescendingOrder = db.BZMPop(0, sortedSetKey, MinMaxModifier.Max, 1);

        Assert.NotNull(resultWithDescendingOrder);
        Assert.Equal(sortedSetKey, resultWithDescendingOrder!.Item1);
        Assert.Single(resultWithDescendingOrder.Item2);
        Assert.Equal("d", resultWithDescendingOrder.Item2[0].Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopNull()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BZMPop(0.5, "my-set", MinMaxModifier.Min, null);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopMultiplexerTimeout()
    {
        var configurationOptions = new ConfigurationOptions();
        configurationOptions.SyncTimeout = 1000;
        configurationOptions.EndPoints.Add("localhost");
        var redis = ConnectionMultiplexer.Connect(configurationOptions);

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Server would wait forever, but the multiplexer times out in 1 second.
        Assert.Throws<RedisTimeoutException>(() => db.BZMPop(0, "my-set", MinMaxModifier.Min));
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopMultipleSets()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.SortedSetAdd("set-one", "a", 1.5);
        db.SortedSetAdd("set-one", "b", 5.1);
        db.SortedSetAdd("set-one", "c", 3.7);
        db.SortedSetAdd("set-two", "d", 9.4);
        db.SortedSetAdd("set-two", "e", 7.76);

        var result = db.BZMPop(0, "set-two", MinMaxModifier.Max);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("d", result.Item2[0].Value.ToString());

        result = db.BZMPop(0, new[] { new RedisKey("set-two"), new RedisKey("set-one") }, MinMaxModifier.Min);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("e", result.Item2[0].Value.ToString());

        result = db.BZMPop(0, new[] { new RedisKey("set-two"), new RedisKey("set-one") }, MinMaxModifier.Max);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("b", result.Item2[0].Value.ToString());

        result = db.BZMPop(0, "set-one", MinMaxModifier.Min, count: 2);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Equal(2, result.Item2.Count);
        Assert.Equal("a", result.Item2[0].Value.ToString());
        Assert.Equal("c", result.Item2[1].Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopNoKeysProvided()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Server would wait forever, but the multiplexer times out in 1 second.
        Assert.Throws<ArgumentException>(() => db.BZMPop(0, Array.Empty<RedisKey>(), MinMaxModifier.Min));
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopWithOrderEnum()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set";

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);
        db.SortedSetAdd(sortedSetKey, "c", 3.7);

        // Pop two items with default order, which means it will pop the minimum values.
        var resultWithDefaultOrder = db.BZMPop(0, sortedSetKey, Order.Ascending.ToMinMax());

        Assert.NotNull(resultWithDefaultOrder);
        Assert.Equal(sortedSetKey, resultWithDefaultOrder!.Item1);
        Assert.Single(resultWithDefaultOrder.Item2);
        Assert.Equal("a", resultWithDefaultOrder.Item2[0].Value.ToString());

        // Pop one more item, with descending order, which means it will pop the maximum value.
        var resultWithDescendingOrder = db.BZMPop(0, sortedSetKey, Order.Descending.ToMinMax());

        Assert.NotNull(resultWithDescendingOrder);
        Assert.Equal(sortedSetKey, resultWithDescendingOrder!.Item1);
        Assert.Single(resultWithDescendingOrder.Item2);
        Assert.Equal("b", resultWithDescendingOrder.Item2[0].Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMin()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set";

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);

        var result = db.BZPopMin(sortedSetKey, 0);

        Assert.NotNull(result);
        Assert.Equal(sortedSetKey, result!.Item1);
        Assert.Equal("a", result.Item2.Value.ToString());
        Assert.Equal(1.5, result.Item2.Score);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMinNull()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BZPopMin("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMinMultipleSets()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.SortedSetAdd("set-one", "a", 1.5);
        db.SortedSetAdd("set-one", "b", 5.1);
        db.SortedSetAdd("set-two", "e", 7.76);

        var result = db.BZPopMin(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Equal("e", result.Item2.Value.ToString());

        result = db.BZPopMin(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Equal("a", result.Item2.Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMax()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set";

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);

        var result = db.BZPopMax(sortedSetKey, 0);

        Assert.NotNull(result);
        Assert.Equal(sortedSetKey, result!.Item1);
        Assert.Equal("b", result.Item2.Value.ToString());
        Assert.Equal(5.1, result.Item2.Score);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMaxNull()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BZPopMax("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMaxMultipleSets()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.SortedSetAdd("set-one", "a", 1.5);
        db.SortedSetAdd("set-one", "b", 5.1);
        db.SortedSetAdd("set-two", "e", 7.76);

        var result = db.BZPopMax(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Equal("e", result.Item2.Value.ToString());

        result = db.BZPopMax(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Equal("b", result.Item2.Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "2.0.0")]
    public void TestBLPop()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");

        var result = db.BLPop("my-list", 0);

        Assert.NotNull(result);
        Assert.Equal("my-list", result!.Item1);
        Assert.Equal("a", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "2.0.0")]
    public void TestBLPopNull()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BLPop("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "2.0.0")]
    public void TestBLPopMultipleLists()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");
        db.ListRightPush("list-two", "e");

        var result = db.BLPop(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Equal("e", result.Item2.ToString());

        result = db.BLPop(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Equal("a", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "2.0.0")]
    public void TestBRPop()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");

        var result = db.BRPop("my-list", 0);

        Assert.NotNull(result);
        Assert.Equal("my-list", result!.Item1);
        Assert.Equal("b", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "2.0.0")]
    public void TestBRPopNull()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BRPop("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Comparison.LessThan, "2.0.0")]
    public void TestBRPopMultipleLists()
    {
        var redis = ConnectionMultiplexer.Connect("localhost");

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");
        db.ListRightPush("list-two", "e");

        var result = db.BRPop(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Equal("e", result.Item2.ToString());

        result = db.BRPop(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Equal("b", result.Item2.ToString());
    }
}