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

    // a retry-wrapped transaction is the same trap again: statically just an ITransactionAsync, and not an
    // IDatabase however it is tested. Kept alongside the announcement test below, because this half - the
    // command working at all - must stay green regardless of what the guard there recognizes.
    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task ModuleCommandsWorkOnRetryWrappedTransaction(string endpointId)
    {
        ResetInfoDefaults(); // demonstrate first connection
        var db = GetCleanDatabase(endpointId);

        var transaction = db.WithRetry(RetryPolicy.Default).CreateTransaction();
        var json = new JsonCommandsAsync(transaction);

        var pending = json.SetAsync("retry-tran-json", "$", "{\"Age\":23}");
        Assert.True(await transaction.ExecuteAsync());
        Assert.True(await pending);
    }

    // the announcement must not be queued into someone else's MULTI/EXEC: that changes what their transaction
    // contains, and anything waiting on a queued reply before EXEC deadlocks
    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public Task TransactionsDoNotCarryTheAnnouncement(string endpointId)
    {
        var db = FirstUseDatabase(endpointId);
        return AssertTransactionCarriesNoAnnouncement(db, db.CreateTransaction(), "tran-json");
    }

    // the same standard for the retry-wrapped shape, which the guard recognizes as an ITransactionAsync -
    // the form CreateTransaction() actually advertises there, and the one that cannot stop being true.
    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public Task RetryWrappedTransactionsDoNotCarryTheAnnouncement(string endpointId)
    {
        var db = FirstUseDatabase(endpointId);
        return AssertTransactionCarriesNoAnnouncement(db,
            db.WithRetry(RetryPolicy.Default).CreateTransaction(), "retry-tran-announce-json");
    }

    // ITransaction and the retry-wrapped transaction have ITransactionAsync in common, so both shapes are
    // held to the same standard here
    private async Task AssertTransactionCarriesNoAnnouncement(IDatabase db, ITransactionAsync transaction, RedisKey key)
    {
        var pending = new JsonCommandsAsync(transaction).SetAsync(key, "$", "{\"Age\":23}");
        Assert.True(await transaction.ExecuteAsync());
        Assert.True(await pending);

        var info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.DoesNotContain("lib-name=NRedisStack", info);

        // the announcement is still owed, so the next command outside a transaction performs it
        await db.ExecuteAsync(new SerializedCommand(CommandCategories.Always, "PING"));

        info = (await db.ExecuteAsync("CLIENT", "INFO")).ToString();
        Assert.Contains($"lib-name=NRedisStack(.NET_v{Environment.Version}) lib-ver={GetNRedisStackVersion()}", info);
    }

    // a connection of our own, with the announcement still owed: these tests assert on the *absence* of our
    // lib-name, so the connection must not be a shared one that has already announced
    private IDatabase FirstUseDatabase(string endpointId)
    {
        ResetInfoDefaults(); // demonstrate first connection
        return GetConnection(endpointId, shareConnection: false).GetDatabase();
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
