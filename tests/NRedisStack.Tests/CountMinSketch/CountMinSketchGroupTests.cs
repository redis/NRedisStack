using Xunit;
using StackExchange.Redis;

namespace NRedisStack.Tests.CountMinSketch;

/// <summary>
/// The <c>db.CountMinSketch</c> command group - the v4 surface, as opposed to <see cref="CmsTests"/> which
/// exercises the original <c>db.CMS()</c> surface (now a proxy over this one).
/// </summary>
[RunPerProtocol(RunProtocol.Resp2 | RunProtocol.Resp3)]
public class CountMinSketchGroupTests(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task InitByDimAndInfo(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var key = CreateKeyName();

        await db.CountMinSketch.InitByDimAsync(key, 16, 4);
        var info = await db.CountMinSketch.InfoAsync(key);

        Assert.Equal(16, info.Width);
        Assert.Equal(4, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task InitByProbAndInfo(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var key = CreateKeyName();

        await db.CountMinSketch.InitByProbAsync(key, 0.01, 0.01);
        var info = await db.CountMinSketch.InfoAsync(key);

        Assert.Equal(200, info.Width);
        Assert.Equal(7, info.Depth);
        Assert.Equal(0, info.Count);
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task KeyAlreadyExistsIsAServerError(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var key = CreateKeyName();

        await db.CountMinSketch.InitByDimAsync(key, 16, 4);
        await Assert.ThrowsAsync<RedisServerException>(async () => await db.CountMinSketch.InitByDimAsync(key, 8, 6));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task IncrByAndQuery(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var key = CreateKeyName();
        var cms = db.CountMinSketch;

        await cms.InitByDimAsync(key, 1000, 5);
        Assert.Equal(5, await cms.IncrByAsync(key, "foo", 5));

        // xunit's span-based Assert.Equal overloads bind to collection expressions, and a span cannot live
        // across an await, so the results are hoisted and the expectations typed as arrays
        long[] counts = await cms.IncrByAsync(key, [("foo", 5), ("bar", 15)]);
        Assert.Equal<long[]>([10, 15], counts);

        long[] queried = await cms.QueryAsync(key, ["foo", "bar", "nope"]);
        Assert.Equal<long[]>([10, 15, 0], queried);

        var info = await cms.InfoAsync(key);
        Assert.Equal(1000, info.Width);
        Assert.Equal(5, info.Depth);
        Assert.Equal(25, info.Count);
    }

    [Fact]
    public async Task EmptyArgumentsAreRejectedBeforeSending()
    {
        var db = GetCleanDatabase();
        var key = CreateKeyName();
        var cms = db.CountMinSketch;

        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await cms.QueryAsync(key, []));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await cms.IncrByAsync(key, []));
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(async () => await cms.MergeAsync(key, []));
        await Assert.ThrowsAsync<ArgumentException>(async () => await cms.MergeAsync(key, ["a", "b"], [1]));
    }

    [Theory]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public async Task Merge(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        // same slot, so this is cluster-safe
        var keys = CreateKeyNames(3);
        RedisKey a = "{m}" + keys[0], b = "{m}" + keys[1], dest = "{m}" + keys[2];
        var cms = db.CountMinSketch;

        await cms.InitByDimAsync(a, 1000, 5);
        await cms.InitByDimAsync(b, 1000, 5);
        await cms.InitByDimAsync(dest, 1000, 5);
        await cms.IncrByAsync(a, "foo", 5);
        await cms.IncrByAsync(b, "foo", 15);

        await cms.MergeAsync(dest, [a, b]);
        long[] unweighted = await cms.QueryAsync(dest, ["foo"]);
        Assert.Equal<long[]>([20], unweighted);

        await cms.MergeAsync(dest, [a, b], [1, 2]);
        long[] weighted = await cms.QueryAsync(dest, ["foo"]);
        Assert.Equal<long[]>([35], weighted);
    }

    [Fact]
    public async Task KeysAreKeys_APrefixedContextPrefixesThem()
    {
        // the test Extending.md says every extending library should have: run a command through a prefixed
        // context and check the server saw the prefix
        var db = GetCleanDatabase();
        var key = CreateKeyName();

        var prefixed = db.Context.AppendKeyPrefix("p:");
        await prefixed.CountMinSketch.InitByDimAsync(key, 16, 4);
        await prefixed.CountMinSketch.IncrByAsync(key, "foo", 3);

        Assert.True(await db.KeyExistsAsync("p:" + key));
        Assert.False(await db.KeyExistsAsync(key));
        long[] viaFullKey = await db.CountMinSketch.QueryAsync("p:" + key, ["foo"]);
        long[] viaPrefix = await prefixed.CountMinSketch.QueryAsync(key, ["foo"]);
        Assert.Equal<long[]>([3], viaFullKey);
        Assert.Equal<long[]>([3], viaPrefix);
    }

    [Fact]
    public async Task TheGroupHangsOffABatchToo()
    {
        var db = GetCleanDatabase();
        var key = CreateKeyName();

        var batch = db.CreateBatch();
        var init = batch.CountMinSketch.InitByDimAsync(key, 16, 4);
        var incr = batch.CountMinSketch.IncrByAsync(key, "foo", 7);
        var info = batch.CountMinSketch.InfoAsync(key);
        batch.Execute();

        await init;
        Assert.Equal(7, await incr);
        Assert.Equal(7, (await info).Count);
    }
}
