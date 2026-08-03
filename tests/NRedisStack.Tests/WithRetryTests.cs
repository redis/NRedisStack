using NRedisStack.Core;
using NRedisStack.RedisStackCommands;
using StackExchange.Redis;
using StackExchange.Redis.Availability;
using Xunit;
using static NRedisStack.Auxiliary;

namespace NRedisStack.Tests;

/// <summary>
/// The retry-wrapped database from <c>WithRetry</c> is <see cref="IDatabaseAsync"/> only - it does not
/// implement <see cref="IDatabase"/> - so anything on our async path that reaches for the sync interface
/// breaks every command issued through it, before the command is even sent.
/// </summary>
public class WithRetryTests(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task ModuleCommandsWorkOnRetryWrappedDatabase(string endpointId)
    {
        ResetInfoDefaults(); // demonstrate first connection: the announcement used to demand IDatabase
        var db = GetCleanDatabase(endpointId);

        var json = new JsonCommandsAsync(db.WithRetry(RetryPolicy.Default));

        Assert.True(await json.SetAsync("retry-json", "$", "{\"Name\":\"Shachar\",\"Age\":23}"));
        Assert.Equal("{\"Name\":\"Shachar\",\"Age\":23}", (await json.GetAsync("retry-json")).ToString());
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task LibraryInfoIsAnnouncedThroughAnAsyncOnlyDatabase(string endpointId)
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = GetCleanDatabase(endpointId);
        IDatabaseAsync retrying = db.WithRetry(RetryPolicy.Default);

        // the announcement is fire-and-forget, but it is enqueued ahead of this command, on this connection
        await retrying.ExecuteAsync(new SerializedCommand(CommandCategories.Always, "PING"));

        var info = db.Execute("CLIENT", "INFO").ToString();
        Assert.Contains($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}", info);
    }

    // the announcement relies on this: fire-and-forget must not leave the caller's first command waiting on
    // a reply, so the task it hands back is expected to be complete before we ever look at it
    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void FireAndForgetClientSetInfoCompletesSynchronously(string endpointId)
    {
        IDatabaseAsync retrying = GetDatabase(endpointId).WithRetry(RetryPolicy.Default);

        var pending = retrying.ClientSetInfoAsync(SetInfoAttr.LibraryName, "TestLibraryName", CommandFlags.FireAndForget);

        Assert.Equal(TaskStatus.RanToCompletion, pending.Status);
        Assert.True(pending.Result); // fire-and-forget: nothing to report, so never a false negative
    }
}
