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

    // TODO: understand why this test fails on enterprise
    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public void TestSimpleSetInfo()
    {
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.ClientSetInfo(SetInfoAttr.LibraryName, "TestLibraryName");
        db.ClientSetInfo(SetInfoAttr.LibraryVersion, "1.2.3");

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=TestLibraryName lib-ver=1.2.3\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public async Task TestSimpleSetInfoAsync()
    {
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.ClientSetInfoAsync(SetInfoAttr.LibraryName, "TestLibraryName");
        await db.ClientSetInfoAsync(SetInfoAttr.LibraryVersion, "1.2.3");

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=TestLibraryName lib-ver=1.2.3\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public void TestSetInfoDefaultValue()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoDefaultValueAsync()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase();
        db.Execute("FLUSHALL");

        await db.ExecuteAsync(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public void TestSetInfoWithValue()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase("MyLibraryName;v1.0.0");
        db.Execute("FLUSHALL");

        db.Execute(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.EndsWith($"NRedisStack(MyLibraryName;v1.0.0;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
    public async Task TestSetInfoWithValueAsync()
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = redisFixture.Redis.GetDatabase("MyLibraryName;v1.0.0");
        db.Execute("FLUSHALL");

        await db.ExecuteAsync(new SerializedCommand("PING")); // only the extension method of Execute (which is used for all the commands of Redis Stack) will set the library name and version.

        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.EndsWith($"NRedisStack(MyLibraryName;v1.0.0;.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}\n", info);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.1.242")]
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBZMPop()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public async Task TestBZMPopAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set";

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);
        db.SortedSetAdd(sortedSetKey, "c", 3.7);
        db.SortedSetAdd(sortedSetKey, "d", 9.4);
        db.SortedSetAdd(sortedSetKey, "e", 7.76);

        // Pop two items with Min modifier, which means it will pop the minimum values.
        var resultWithDefaultOrder = await db.BZMPopAsync(0, sortedSetKey, MinMaxModifier.Min, 2);

        Assert.NotNull(resultWithDefaultOrder);
        Assert.Equal(sortedSetKey, resultWithDefaultOrder!.Item1);
        Assert.Equal(2, resultWithDefaultOrder.Item2.Count);
        Assert.Equal("a", resultWithDefaultOrder.Item2[0].Value.ToString());
        Assert.Equal("c", resultWithDefaultOrder.Item2[1].Value.ToString());

        // Pop one more item, with Max modifier, which means it will pop the maximum value.
        var resultWithDescendingOrder = await db.BZMPopAsync(0, sortedSetKey, MinMaxModifier.Max, 1);

        Assert.NotNull(resultWithDescendingOrder);
        Assert.Equal(sortedSetKey, resultWithDescendingOrder!.Item1);
        Assert.Single(resultWithDescendingOrder.Item2);
        Assert.Equal("d", resultWithDescendingOrder.Item2[0].Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopNull()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BZMPop(0.5, "my-set", MinMaxModifier.Min, null);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public async Task TestBZMPopNullAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = await db.BZMPopAsync(0.5, "my-set", MinMaxModifier.Min, null);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopMultiplexerTimeout()
    {
        var configurationOptions = new ConfigurationOptions();
        configurationOptions.SyncTimeout = 1000;

        using var redis = redisFixture.CustomRedis(configurationOptions, out _);

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Server would wait forever, but the multiplexer times out in 1 second.
        Assert.Throws<RedisTimeoutException>(() => db.BZMPop(0, "my-set", MinMaxModifier.Min));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public async Task TestBZMPopMultiplexerTimeoutAsync()
    {
        var configurationOptions = new ConfigurationOptions();
        configurationOptions.SyncTimeout = 1000;

        await using var redis = redisFixture.CustomRedis(configurationOptions, out _);

        var db = redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Server would wait forever, but the multiplexer times out in 1 second.
        await Assert.ThrowsAsync<RedisTimeoutException>(async () => await db.BZMPopAsync(0, "my-set", MinMaxModifier.Min));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopMultipleSets()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "7.0.0")]
    public async Task TestBZMPopMultipleSetsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.SortedSetAdd("set-one", "a", 1.5);
        db.SortedSetAdd("set-one", "b", 5.1);
        db.SortedSetAdd("set-one", "c", 3.7);
        db.SortedSetAdd("set-two", "d", 9.4);
        db.SortedSetAdd("set-two", "e", 7.76);

        var result = await db.BZMPopAsync(0, "set-two", MinMaxModifier.Max);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("d", result.Item2[0].Value.ToString());

        result = await db.BZMPopAsync(0, new[] { new RedisKey("set-two"), new RedisKey("set-one") }, MinMaxModifier.Min);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("e", result.Item2[0].Value.ToString());

        result = await db.BZMPopAsync(0, new[] { new RedisKey("set-two"), new RedisKey("set-one") }, MinMaxModifier.Max);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("b", result.Item2[0].Value.ToString());

        result = await db.BZMPopAsync(0, "set-one", MinMaxModifier.Min, count: 2);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Equal(2, result.Item2.Count);
        Assert.Equal("a", result.Item2[0].Value.ToString());
        Assert.Equal("c", result.Item2[1].Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopNoKeysProvided()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        Assert.Throws<ArgumentException>(() => db.BZMPop(0, Array.Empty<RedisKey>(), MinMaxModifier.Min));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBZMPopWithOrderEnum()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set-" + Guid.NewGuid();

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);
        db.SortedSetAdd(sortedSetKey, "c", 3.7);

        // Pop two items with Ascending order, which means it will pop the minimum values.
        var resultWithDefaultOrder = db.BZMPop(0, sortedSetKey, Order.Ascending.ToMinMax());

        Assert.NotNull(resultWithDefaultOrder);
        Assert.Equal(sortedSetKey, resultWithDefaultOrder!.Item1);
        Assert.Single(resultWithDefaultOrder.Item2);
        Assert.Equal("a", resultWithDefaultOrder.Item2[0].Value.ToString());

        // Pop one more item, with Descending order, which means it will pop the maximum value.
        var resultWithDescendingOrder = db.BZMPop(0, sortedSetKey, Order.Descending.ToMinMax());

        Assert.NotNull(resultWithDescendingOrder);
        Assert.Equal(sortedSetKey, resultWithDescendingOrder!.Item1);
        Assert.Single(resultWithDescendingOrder.Item2);
        Assert.Equal("b", resultWithDescendingOrder.Item2[0].Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMin()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestBZPopMinAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set";

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);

        var result = await db.BZPopMinAsync(sortedSetKey, 0);

        Assert.NotNull(result);
        Assert.Equal(sortedSetKey, result!.Item1);
        Assert.Equal("a", result.Item2.Value.ToString());
        Assert.Equal(1.5, result.Item2.Score);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMinNull()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BZPopMin("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestBZPopMinNullAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = await db.BZPopMinAsync("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMinMultipleSets()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public async Task TestBZPopMinMultipleSetsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.SortedSetAdd("set-one", "a", 1.5);
        db.SortedSetAdd("set-one", "b", 5.1);
        db.SortedSetAdd("set-two", "e", 7.76);

        var result = await db.BZPopMinAsync(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Equal("e", result.Item2.Value.ToString());

        result = await db.BZPopMinAsync(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Equal("a", result.Item2.Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMax()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestBZPopMaxAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var sortedSetKey = "my-set";

        db.SortedSetAdd(sortedSetKey, "a", 1.5);
        db.SortedSetAdd(sortedSetKey, "b", 5.1);

        var result = await db.BZPopMaxAsync(sortedSetKey, 0);

        Assert.NotNull(result);
        Assert.Equal(sortedSetKey, result!.Item1);
        Assert.Equal("b", result.Item2.Value.ToString());
        Assert.Equal(5.1, result.Item2.Score);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMaxNull()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BZPopMax("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestBZPopMaxNullAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = await db.BZPopMaxAsync("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public void TestBZPopMaxMultipleSets()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public async Task TestBZPopMaxMultipleSetsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.SortedSetAdd("set-one", "a", 1.5);
        db.SortedSetAdd("set-one", "b", 5.1);
        db.SortedSetAdd("set-two", "e", 7.76);

        var result = await db.BZPopMaxAsync(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-two", result!.Item1);
        Assert.Equal("e", result.Item2.Value.ToString());

        result = await db.BZPopMaxAsync(new[] { new RedisKey("set-two"), new RedisKey("set-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("set-one", result!.Item1);
        Assert.Equal("b", result.Item2.Value.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBLMPop()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");
        db.ListRightPush("my-list", "c");
        db.ListRightPush("my-list", "d");
        db.ListRightPush("my-list", "e");

        // Pop two items from the left side.
        var resultWithDefaultOrder = db.BLMPop(0, "my-list", ListSide.Left, 2);

        Assert.NotNull(resultWithDefaultOrder);
        Assert.Equal("my-list", resultWithDefaultOrder!.Item1);
        Assert.Equal(2, resultWithDefaultOrder.Item2.Count);
        Assert.Equal("a", resultWithDefaultOrder.Item2[0].ToString());
        Assert.Equal("b", resultWithDefaultOrder.Item2[1].ToString());

        // Pop one more item, from the right side.
        var resultWithDescendingOrder = db.BLMPop(0, "my-list", ListSide.Right, 1);

        Assert.NotNull(resultWithDescendingOrder);
        Assert.Equal("my-list", resultWithDescendingOrder!.Item1);
        Assert.Single(resultWithDescendingOrder.Item2);
        Assert.Equal("e", resultWithDescendingOrder.Item2[0].ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public async Task TestBLMPopAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");
        db.ListRightPush("my-list", "c");
        db.ListRightPush("my-list", "d");
        db.ListRightPush("my-list", "e");

        // Pop two items from the left side.
        var resultWithDefaultOrder = await db.BLMPopAsync(0, "my-list", ListSide.Left, 2);

        Assert.NotNull(resultWithDefaultOrder);
        Assert.Equal("my-list", resultWithDefaultOrder!.Item1);
        Assert.Equal(2, resultWithDefaultOrder.Item2.Count);
        Assert.Equal("a", resultWithDefaultOrder.Item2[0].ToString());
        Assert.Equal("b", resultWithDefaultOrder.Item2[1].ToString());

        // Pop one more item, from the right side.
        var resultWithDescendingOrder = await db.BLMPopAsync(0, "my-list", ListSide.Right, 1);

        Assert.NotNull(resultWithDescendingOrder);
        Assert.Equal("my-list", resultWithDescendingOrder!.Item1);
        Assert.Single(resultWithDescendingOrder.Item2);
        Assert.Equal("e", resultWithDescendingOrder.Item2[0].ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBLMPopNull()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the list, and a short server timeout, which yields null.
        var result = db.BLMPop(0.5, "my-list", ListSide.Left);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public async Task TestBLMPopNullAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the list, and a short server timeout, which yields null.
        var result = await db.BLMPopAsync(0.5, "my-list", ListSide.Left);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBLMPopMultipleLists()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");
        db.ListRightPush("list-one", "c");
        db.ListRightPush("list-two", "d");
        db.ListRightPush("list-two", "e");

        var result = db.BLMPop(0, "list-two", ListSide.Right);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("e", result.Item2[0].ToString());

        result = db.BLMPop(0, new[] { new RedisKey("list-two"), new RedisKey("list-one") }, ListSide.Left);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("d", result.Item2[0].ToString());

        result = db.BLMPop(0, new[] { new RedisKey("list-two"), new RedisKey("list-one") }, ListSide.Right);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("c", result.Item2[0].ToString());

        result = db.BLMPop(0, "list-one", ListSide.Left, count: 2);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Equal(2, result.Item2.Count);
        Assert.Equal("a", result.Item2[0].ToString());
        Assert.Equal("b", result.Item2[1].ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public async Task TestBLMPopMultipleListsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");
        db.ListRightPush("list-one", "c");
        db.ListRightPush("list-two", "d");
        db.ListRightPush("list-two", "e");

        var result = await db.BLMPopAsync(0, "list-two", ListSide.Right);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("e", result.Item2[0].ToString());

        result = await db.BLMPopAsync(0, new[] { new RedisKey("list-two"), new RedisKey("list-one") }, ListSide.Left);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("d", result.Item2[0].ToString());

        result = await db.BLMPopAsync(0, new[] { new RedisKey("list-two"), new RedisKey("list-one") }, ListSide.Right);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Single(result.Item2);
        Assert.Equal("c", result.Item2[0].ToString());

        result = await db.BLMPopAsync(0, "list-one", ListSide.Left, count: 2);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Equal(2, result.Item2.Count);
        Assert.Equal("a", result.Item2[0].ToString());
        Assert.Equal("b", result.Item2[1].ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "7.0.0")]
    public void TestBLMPopNoKeysProvided()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        Assert.Throws<ArgumentException>(() => db.BLMPop(0, Array.Empty<RedisKey>(), ListSide.Left));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public void TestBLPop()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");

        var result = db.BLPop("my-list", 0);

        Assert.NotNull(result);
        Assert.Equal("my-list", result!.Item1);
        Assert.Equal("a", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public async Task TestBLPopAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");

        var result = await db.BLPopAsync("my-list", 0);

        Assert.NotNull(result);
        Assert.Equal("my-list", result!.Item1);
        Assert.Equal("a", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public void TestBLPopNull()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BLPop("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public async Task TestBLPopNullAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = await db.BLPopAsync("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "2.0.0")]
    public void TestBLPopMultipleLists()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "2.0.0")]
    public async Task TestBLPopMultipleListsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");
        db.ListRightPush("list-two", "e");

        var result = await db.BLPopAsync(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Equal("e", result.Item2.ToString());

        result = await db.BLPopAsync(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Equal("a", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public void TestBRPop()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");

        var result = db.BRPop("my-list", 0);

        Assert.NotNull(result);
        Assert.Equal("my-list", result!.Item1);
        Assert.Equal("b", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public async Task TestBRPopAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("my-list", "a");
        db.ListRightPush("my-list", "b");

        var result = await db.BRPopAsync("my-list", 0);

        Assert.NotNull(result);
        Assert.Equal("my-list", result!.Item1);
        Assert.Equal("b", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public void TestBRPopNull()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = db.BRPop("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "2.0.0")]
    public async Task TestBRPopNullAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        // Nothing in the set, and a short server timeout, which yields null.
        var result = await db.BRPopAsync("my-set", 0.5);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "2.0.0")]
    public void TestBRPopMultipleLists()
    {
        var db = redisFixture.Redis.GetDatabase(null);
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

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "2.0.0")]
    public async Task TestBRPopMultipleListsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");
        db.ListRightPush("list-two", "e");

        var result = await db.BRPopAsync(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-two", result!.Item1);
        Assert.Equal("e", result.Item2.ToString());

        result = await db.BRPopAsync(new[] { new RedisKey("list-two"), new RedisKey("list-one") }, 0);

        Assert.NotNull(result);
        Assert.Equal("list-one", result!.Item1);
        Assert.Equal("b", result.Item2.ToString());
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "6.2.0")]
    public void TestBLMove()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");

        db.ListRightPush("list-two", "c");
        db.ListRightPush("list-two", "d");

        var result = db.BLMove("list-one", "list-two", ListSide.Right, ListSide.Left, 0);
        Assert.NotNull(result);
        Assert.Equal("b", result!);

        Assert.Equal(1, db.ListLength("list-one"));
        Assert.Equal("a", db.ListGetByIndex("list-one", 0));
        Assert.Equal(3, db.ListLength("list-two"));
        Assert.Equal("b", db.ListGetByIndex("list-two", 0));
        Assert.Equal("c", db.ListGetByIndex("list-two", 1));
        Assert.Equal("d", db.ListGetByIndex("list-two", 2));

        result = db.BLMove("list-two", "list-one", ListSide.Left, ListSide.Right, 0);
        Assert.NotNull(result);
        Assert.Equal("b", result!);

        Assert.Equal(2, db.ListLength("list-one"));
        Assert.Equal("a", db.ListGetByIndex("list-one", 0));
        Assert.Equal("b", db.ListGetByIndex("list-one", 1));
        Assert.Equal(2, db.ListLength("list-two"));
        Assert.Equal("c", db.ListGetByIndex("list-two", 0));
        Assert.Equal("d", db.ListGetByIndex("list-two", 1));

        result = db.BLMove("list-one", "list-two", ListSide.Left, ListSide.Left, 0);
        Assert.NotNull(result);
        Assert.Equal("a", result!);

        Assert.Equal(1, db.ListLength("list-one"));
        Assert.Equal("b", db.ListGetByIndex("list-one", 0));
        Assert.Equal(3, db.ListLength("list-two"));
        Assert.Equal("a", db.ListGetByIndex("list-two", 0));
        Assert.Equal("c", db.ListGetByIndex("list-two", 1));
        Assert.Equal("d", db.ListGetByIndex("list-two", 2));

        result = db.BLMove("list-two", "list-one", ListSide.Right, ListSide.Right, 0);
        Assert.NotNull(result);
        Assert.Equal("d", result!);

        Assert.Equal(2, db.ListLength("list-one"));
        Assert.Equal("b", db.ListGetByIndex("list-one", 0));
        Assert.Equal("d", db.ListGetByIndex("list-one", 1));
        Assert.Equal(2, db.ListLength("list-two"));
        Assert.Equal("a", db.ListGetByIndex("list-two", 0));
        Assert.Equal("c", db.ListGetByIndex("list-two", 1));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "6.2.0")]
    public async Task TestBLMoveAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");

        db.ListRightPush("list-two", "c");
        db.ListRightPush("list-two", "d");

        var result = await db.BLMoveAsync("list-one", "list-two", ListSide.Right, ListSide.Left, 0);
        Assert.NotNull(result);
        Assert.Equal("b", result!);

        Assert.Equal(1, db.ListLength("list-one"));
        Assert.Equal("a", db.ListGetByIndex("list-one", 0));
        Assert.Equal(3, db.ListLength("list-two"));
        Assert.Equal("b", db.ListGetByIndex("list-two", 0));
        Assert.Equal("c", db.ListGetByIndex("list-two", 1));
        Assert.Equal("d", db.ListGetByIndex("list-two", 2));

        result = await db.BLMoveAsync("list-two", "list-one", ListSide.Left, ListSide.Right, 0);
        Assert.NotNull(result);
        Assert.Equal("b", result!);

        Assert.Equal(2, db.ListLength("list-one"));
        Assert.Equal("a", db.ListGetByIndex("list-one", 0));
        Assert.Equal("b", db.ListGetByIndex("list-one", 1));
        Assert.Equal(2, db.ListLength("list-two"));
        Assert.Equal("c", db.ListGetByIndex("list-two", 0));
        Assert.Equal("d", db.ListGetByIndex("list-two", 1));

        result = await db.BLMoveAsync("list-one", "list-two", ListSide.Left, ListSide.Left, 0);
        Assert.NotNull(result);
        Assert.Equal("a", result!);

        Assert.Equal(1, db.ListLength("list-one"));
        Assert.Equal("b", db.ListGetByIndex("list-one", 0));
        Assert.Equal(3, db.ListLength("list-two"));
        Assert.Equal("a", db.ListGetByIndex("list-two", 0));
        Assert.Equal("c", db.ListGetByIndex("list-two", 1));
        Assert.Equal("d", db.ListGetByIndex("list-two", 2));

        result = await db.BLMoveAsync("list-two", "list-one", ListSide.Right, ListSide.Right, 0);
        Assert.NotNull(result);
        Assert.Equal("d", result!);

        Assert.Equal(2, db.ListLength("list-one"));
        Assert.Equal("b", db.ListGetByIndex("list-one", 0));
        Assert.Equal("d", db.ListGetByIndex("list-one", 1));
        Assert.Equal(2, db.ListLength("list-two"));
        Assert.Equal("a", db.ListGetByIndex("list-two", 0));
        Assert.Equal("c", db.ListGetByIndex("list-two", 1));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "2.2.0")]
    public void TestBRPopLPush()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");

        db.ListRightPush("list-two", "c");
        db.ListRightPush("list-two", "d");

        var result = db.BRPopLPush("list-one", "list-two", 0);
        Assert.NotNull(result);
        Assert.Equal("b", result!);

        Assert.Equal(1, db.ListLength("list-one"));
        Assert.Equal(3, db.ListLength("list-two"));
        Assert.Equal("b", db.ListLeftPop("list-two"));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "2.2.0")]
    public async Task TestBRPopLPushAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.ListRightPush("list-one", "a");
        db.ListRightPush("list-one", "b");

        db.ListRightPush("list-two", "c");
        db.ListRightPush("list-two", "d");

        var result = await db.BRPopLPushAsync("list-one", "list-two", 0);
        Assert.NotNull(result);
        Assert.Equal("b", result!);

        Assert.Equal(1, db.ListLength("list-one"));
        Assert.Equal(3, db.ListLength("list-two"));
        Assert.Equal("b", db.ListLeftPop("list-two"));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXRead()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.StreamAdd("my-stream", "a", 1);
        db.StreamAdd("my-stream", "b", 7);

        var result = db.XRead("my-stream", StreamSpecialIds.AllMessagesId,
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        StreamEntry streamEntry = result![0];
        var lastKey = streamEntry.Id;
        Assert.Single(streamEntry.Values);
        Assert.Equal("a", streamEntry.Values[0].Name);
        Assert.Equal(1, streamEntry.Values[0].Value);

        result = db.XRead("my-stream", lastKey, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        streamEntry = result![0];
        Assert.Single(streamEntry.Values);
        Assert.Equal("b", streamEntry.Values[0].Name);
        Assert.Equal(7, streamEntry.Values[0].Value);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestXReadAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.StreamAdd("my-stream", "a", 1);
        db.StreamAdd("my-stream", "b", 7);

        var result = await db.XReadAsync("my-stream", StreamSpecialIds.AllMessagesId,
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        StreamEntry streamEntry = result![0];
        var lastKey = streamEntry.Id;
        Assert.Single(streamEntry.Values);
        Assert.Equal("a", streamEntry.Values[0].Name);
        Assert.Equal(1, streamEntry.Values[0].Value);

        result = await db.XReadAsync("my-stream", lastKey, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        streamEntry = result![0];
        Assert.Single(streamEntry.Values);
        Assert.Equal("b", streamEntry.Values[0].Name);
        Assert.Equal(7, streamEntry.Values[0].Value);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public void TestXReadMultipleStreams()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.StreamAdd("stream-one", "a", 1);
        db.StreamAdd("stream-one", "b", 7);
        db.StreamAdd("stream-two", "c", "foo");
        db.StreamAdd("stream-two", "d", "bar");

        var result = db.XRead(new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { StreamSpecialIds.AllMessagesId, StreamSpecialIds.AllMessagesId },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Equal("stream-one", result![0].Key);
        Assert.Single(result![0].Entries);
        var lastKeyOne = result![0].Entries[0].Id;
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("a", result![0].Entries[0].Values[0].Name);
        Assert.Equal(1, result![0].Entries[0].Values[0].Value);

        Assert.Equal("stream-two", result![1].Key);
        Assert.Single(result![1].Entries);
        var lastKeyTwo = result![1].Entries[0].Id;
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("c", result![1].Entries[0].Values[0].Name);
        Assert.Equal("foo", result![1].Entries[0].Values[0].Value);

        result = db.XRead(new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { lastKeyOne, lastKeyTwo },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Single(result![0].Entries);
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("b", result![0].Entries[0].Values[0].Name);
        Assert.Equal(7, result![0].Entries[0].Values[0].Value);

        Assert.Single(result![1].Entries);
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("d", result![1].Entries[0].Values[0].Name);
        Assert.Equal("bar", result![1].Entries[0].Values[0].Value);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public async Task TestXReadMultipleStreamsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.StreamAdd("stream-one", "a", 1);
        db.StreamAdd("stream-one", "b", 7);
        db.StreamAdd("stream-two", "c", "foo");
        db.StreamAdd("stream-two", "d", "bar");

        var result = await db.XReadAsync(new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { StreamSpecialIds.AllMessagesId, StreamSpecialIds.AllMessagesId },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Single(result![0].Entries);
        var lastKeyOne = result![0].Entries[0].Id;
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("a", result![0].Entries[0].Values[0].Name);
        Assert.Equal(1, result![0].Entries[0].Values[0].Value);

        Assert.Single(result![1].Entries);
        var lastKeyTwo = result![1].Entries[0].Id;
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("c", result![1].Entries[0].Values[0].Name);
        Assert.Equal("foo", result![1].Entries[0].Values[0].Value);

        result = await db.XReadAsync(new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { lastKeyOne, lastKeyTwo },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Single(result![0].Entries);
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("b", result![0].Entries[0].Values[0].Name);
        Assert.Equal(7, result![0].Entries[0].Values[0].Value);

        Assert.Single(result![1].Entries);
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("d", result![1].Entries[0].Values[0].Name);
        Assert.Equal("bar", result![1].Entries[0].Values[0].Value);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadOnlyNewMessages()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.StreamAdd("my-stream", "a", 1);

        // Reading only new messages will yield null, because we don't add any and the read times out.
        var result = db.XRead("my-stream", StreamSpecialIds.NewMessagesId,
            count: 1, timeoutMilliseconds: 500);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestXReadOnlyNewMessagesAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        db.StreamAdd("my-stream", "a", 1);

        // Reading only new messages will yield null, because we don't add any and the read times out.
        var result = await db.XReadAsync("my-stream", StreamSpecialIds.NewMessagesId,
            count: 1, timeoutMilliseconds: 500);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadNoKeysProvided()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        Assert.Throws<ArgumentException>(() => db.XRead(Array.Empty<RedisKey>(),
            new RedisValue[] { StreamSpecialIds.NewMessagesId }));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadMismatchedKeysAndPositionsCountsProvided()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        Assert.Throws<ArgumentException>(() => db.XRead(new RedisKey[] { "my-stream" },
            new RedisValue[] { StreamSpecialIds.NewMessagesId, StreamSpecialIds.NewMessagesId }));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadGroup()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var groupCreationResult = db.StreamCreateConsumerGroup("my-stream", "my-group");
        Assert.True(groupCreationResult);

        db.StreamAdd("my-stream", "a", 1);
        db.StreamAdd("my-stream", "b", 7);
        db.StreamAdd("my-stream", "c", 11);
        db.StreamAdd("my-stream", "d", 12);

        // Read one message by each consumer.
        var result = db.XReadGroup("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerAIdOne = result![0].Id;
        Assert.Single(result[0].Values);
        Assert.Equal("a", result![0].Values[0].Name);
        Assert.Equal(1, result![0].Values[0].Value);

        result = db.XReadGroup("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerBIdOne = result![0].Id;
        Assert.Single(result[0].Values);
        Assert.Equal("b", result![0].Values[0].Name);
        Assert.Equal(7, result![0].Values[0].Value);

        // Read another message from each consumer, don't ACK anything.
        result = db.XReadGroup("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerAIdTwo = result![0].Id;
        Assert.Single(result![0].Values);
        Assert.Equal("c", result![0].Values[0].Name);
        Assert.Equal(11, result![0].Values[0].Value);

        result = db.XReadGroup("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerBIdTwo = result![0].Id;
        Assert.Single(result![0].Values);
        Assert.Equal("d", result![0].Values[0].Name);
        Assert.Equal(12, result![0].Values[0].Value);

        // Since we didn't ACK anything, the pending messages can be re-read with the right ID.
        result = db.XReadGroup("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        Assert.Single(result![0].Values);
        Assert.Equal("a", result![0].Values[0].Name);
        Assert.Equal(1, result![0].Values[0].Value);

        result = db.XReadGroup("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        Assert.Single(result![0].Values);
        Assert.Equal("b", result![0].Values[0].Name);
        Assert.Equal(7, result![0].Values[0].Value);

        // ACK the messages.
        var ackedMessagesCount = db.StreamAcknowledge("my-stream", "my-group",
            new[] { consumerAIdOne, consumerAIdTwo, consumerBIdOne, consumerBIdTwo });
        Assert.Equal(4, ackedMessagesCount);

        // After ACK we don't see anything pending.
        result = db.XReadGroup("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Empty(result);

        result = db.XReadGroup("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestXReadGroupAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var groupCreationResult = db.StreamCreateConsumerGroup("my-stream", "my-group");
        Assert.True(groupCreationResult);

        db.StreamAdd("my-stream", "a", 1);
        db.StreamAdd("my-stream", "b", 7);
        db.StreamAdd("my-stream", "c", 11);
        db.StreamAdd("my-stream", "d", 12);

        // Read one message by each consumer.
        var result = await db.XReadGroupAsync("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerAIdOne = result![0].Id;
        Assert.Single(result[0].Values);
        Assert.Equal("a", result![0].Values[0].Name);
        Assert.Equal(1, result![0].Values[0].Value);

        result = await db.XReadGroupAsync("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerBIdOne = result![0].Id;
        Assert.Single(result[0].Values);
        Assert.Equal("b", result![0].Values[0].Name);
        Assert.Equal(7, result![0].Values[0].Value);

        // Read another message from each consumer, don't ACK anything.
        result = await db.XReadGroupAsync("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerAIdTwo = result![0].Id;
        Assert.Single(result![0].Values);
        Assert.Equal("c", result![0].Values[0].Name);
        Assert.Equal(11, result![0].Values[0].Value);

        result = await db.XReadGroupAsync("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        var consumerBIdTwo = result![0].Id;
        Assert.Single(result![0].Values);
        Assert.Equal("d", result![0].Values[0].Name);
        Assert.Equal(12, result![0].Values[0].Value);

        // Since we didn't ACK anything, the pending messages can be re-read with the right ID.
        result = await db.XReadGroupAsync("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        Assert.Single(result![0].Values);
        Assert.Equal("a", result![0].Values[0].Name);
        Assert.Equal(1, result![0].Values[0].Value);

        result = await db.XReadGroupAsync("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Single(result);

        Assert.Single(result![0].Values);
        Assert.Equal("b", result![0].Values[0].Name);
        Assert.Equal(7, result![0].Values[0].Value);

        // ACK the messages.
        var ackedMessagesCount = db.StreamAcknowledge("my-stream", "my-group",
            new[] { consumerAIdOne, consumerAIdTwo, consumerBIdOne, consumerBIdTwo });
        Assert.Equal(4, ackedMessagesCount);

        // After ACK we don't see anything pending.
        result = await db.XReadGroupAsync("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Empty(result);

        result = await db.XReadGroupAsync("my-group", "consumer-b",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadGroupNoAck()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var groupCreationResult = db.StreamCreateConsumerGroup("my-stream", "my-group");
        Assert.True(groupCreationResult);

        db.StreamAdd("my-stream", "a", 1);
        db.StreamAdd("my-stream", "b", 7);
        db.StreamAdd("my-stream", "c", 11);
        db.StreamAdd("my-stream", "d", 12);

        var result = db.XReadGroup("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId,
            count: 1, timeoutMilliseconds: 1000, noAck: true);

        Assert.NotNull(result);
        Assert.Single(result);

        Assert.Single(result![0].Values);
        Assert.Equal("a", result![0].Values[0].Name);
        Assert.Equal(1, result![0].Values[0].Value);

        // We don't see anything pending because of the NOACK.
        result = db.XReadGroup("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.AllMessagesId, count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Empty(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public void TestXReadGroupMultipleStreams()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var groupCreationResult = db.StreamCreateConsumerGroup("stream-one", "my-group");
        Assert.True(groupCreationResult);

        groupCreationResult = db.StreamCreateConsumerGroup("stream-two", "my-group");
        Assert.True(groupCreationResult);

        db.StreamAdd("stream-one", "a", 1);
        db.StreamAdd("stream-two", "b", 7);
        db.StreamAdd("stream-one", "c", 11);
        db.StreamAdd("stream-two", "d", 17);

        var result = db.XReadGroup("my-group", "consumer-a",
            new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { StreamSpecialIds.UndeliveredMessagesId, StreamSpecialIds.UndeliveredMessagesId },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Single(result![0].Entries);
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("a", result![0].Entries[0].Values[0].Name);
        Assert.Equal(1, result![0].Entries[0].Values[0].Value);

        Assert.Single(result![1].Entries);
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("b", result![1].Entries[0].Values[0].Name);
        Assert.Equal(7, result![1].Entries[0].Values[0].Value);

        result = db.XReadGroup("my-group", "consumer-b",
            new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { StreamSpecialIds.UndeliveredMessagesId, StreamSpecialIds.UndeliveredMessagesId },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Single(result![0].Entries);
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("c", result![0].Entries[0].Values[0].Name);
        Assert.Equal(11, result![0].Entries[0].Values[0].Value);

        Assert.Single(result![1].Entries);
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("d", result![1].Entries[0].Values[0].Name);
        Assert.Equal(17, result![1].Entries[0].Values[0].Value);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Is.EnterpriseOssCluster, Comparison.LessThan, "5.0.0")]
    public async Task TestXReadGroupMultipleStreamsAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var groupCreationResult = db.StreamCreateConsumerGroup("stream-one", "my-group");
        Assert.True(groupCreationResult);

        groupCreationResult = db.StreamCreateConsumerGroup("stream-two", "my-group");
        Assert.True(groupCreationResult);

        db.StreamAdd("stream-one", "a", 1);
        db.StreamAdd("stream-two", "b", 7);
        db.StreamAdd("stream-one", "c", 11);
        db.StreamAdd("stream-two", "d", 17);

        var result = await db.XReadGroupAsync("my-group", "consumer-a",
            new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { StreamSpecialIds.UndeliveredMessagesId, StreamSpecialIds.UndeliveredMessagesId },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Single(result![0].Entries);
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("a", result![0].Entries[0].Values[0].Name);
        Assert.Equal(1, result![0].Entries[0].Values[0].Value);

        Assert.Single(result![1].Entries);
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("b", result![1].Entries[0].Values[0].Name);
        Assert.Equal(7, result![1].Entries[0].Values[0].Value);

        result = await db.XReadGroupAsync("my-group", "consumer-b",
            new RedisKey[] { "stream-one", "stream-two" },
            new RedisValue[] { StreamSpecialIds.UndeliveredMessagesId, StreamSpecialIds.UndeliveredMessagesId },
            count: 1, timeoutMilliseconds: 1000);

        Assert.NotNull(result);
        Assert.Equal(2, result!.Length);

        Assert.Single(result![0].Entries);
        Assert.Single(result![0].Entries[0].Values);
        Assert.Equal("c", result![0].Entries[0].Values[0].Name);
        Assert.Equal(11, result![0].Entries[0].Values[0].Value);

        Assert.Single(result![1].Entries);
        Assert.Single(result![1].Entries[0].Values);
        Assert.Equal("d", result![1].Entries[0].Values[0].Name);
        Assert.Equal(17, result![1].Entries[0].Values[0].Value);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadGroupNull()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var groupCreationResult = db.StreamCreateConsumerGroup("my-stream", "my-group");
        Assert.True(groupCreationResult);

        var result = db.XReadGroup("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId,
            count: 1, timeoutMilliseconds: 500);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public async Task TestXReadGroupNullAsync()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        var groupCreationResult = db.StreamCreateConsumerGroup("my-stream", "my-group");
        Assert.True(groupCreationResult);

        var result = await db.XReadGroupAsync("my-group", "consumer-a",
            "my-stream", StreamSpecialIds.UndeliveredMessagesId,
            count: 1, timeoutMilliseconds: 500);

        Assert.Null(result);
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadGroupNoKeysProvided()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        Assert.Throws<ArgumentException>(() => db.XReadGroup("my-group", "consumer",
            Array.Empty<RedisKey>(), new RedisValue[] { StreamSpecialIds.NewMessagesId }));
    }

    [SkipIfRedis(Is.OSSCluster, Is.Enterprise, Comparison.LessThan, "5.0.0")]
    public void TestXReadGroupMismatchedKeysAndPositionsCountsProvided()
    {
        var db = redisFixture.Redis.GetDatabase(null);
        db.Execute("FLUSHALL");

        Assert.Throws<ArgumentException>(() => db.XReadGroup("my-group", "consumer",
            new RedisKey[] { "my-stream" }, new RedisValue[] { StreamSpecialIds.NewMessagesId, StreamSpecialIds.NewMessagesId }));
    }
}