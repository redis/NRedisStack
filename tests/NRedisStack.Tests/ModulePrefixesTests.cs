using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using StackExchange.Redis;
using StackExchange.Redis.Availability;
using Xunit;

namespace NRedisStack.Tests;

/// <summary>
/// The module prefixes (<c>JSON()</c>, <c>FT()</c>, ...) exist for both <see cref="IDatabase"/> and
/// <see cref="IDatabaseAsync"/>: the latter for databases that are async-only - a retry-wrapped database, a
/// batch, a transaction - while an <see cref="IDatabase"/> keeps binding to the sync-capable commands.
/// </summary>
public class ModulePrefixesTests(EndpointsFixture endpointsFixture) : AbstractNRedisStackTest(endpointsFixture)
{
    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public void EachReceiverBindsToItsOwnPrefixes(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        IDatabaseAsync asyncOnly = db;
        var retrying = db.WithRetry(RetryPolicy.Default);
        var batch = db.CreateBatch();
        var transaction = db.CreateTransaction();
        var retryingTransaction = retrying.CreateTransaction();

        // an IDatabase is the more specific receiver, so it keeps the sync-capable commands
        Assert.IsType<JsonCommands>(db.JSON());
        Assert.IsType<SearchCommands>(db.FT());
        Assert.IsType<BloomCommands>(db.BF());
        Assert.IsType<CuckooCommands>(db.CF());
        Assert.IsType<CmsCommands>(db.CMS());
        Assert.IsType<TopKCommands>(db.TOPK());
        Assert.IsType<TdigestCommands>(db.TDIGEST());
        Assert.IsType<TimeSeriesCommands>(db.TS());

        // anything else that's an IDatabaseAsync gets the async-only ones; each prefix call is made on the
        // variable's own static type so overload resolution is exercised for every receiver shape
        AssertAsyncPrefixes(asyncOnly.JSON(), asyncOnly.FT(), asyncOnly.BF(), asyncOnly.CF(),
            asyncOnly.CMS(), asyncOnly.TOPK(), asyncOnly.TDIGEST(), asyncOnly.TS());
        AssertAsyncPrefixes(retrying.JSON(), retrying.FT(), retrying.BF(), retrying.CF(),
            retrying.CMS(), retrying.TOPK(), retrying.TDIGEST(), retrying.TS());
        AssertAsyncPrefixes(batch.JSON(), batch.FT(), batch.BF(), batch.CF(),
            batch.CMS(), batch.TOPK(), batch.TDIGEST(), batch.TS());
        AssertAsyncPrefixes(transaction.JSON(), transaction.FT(), transaction.BF(), transaction.CF(),
            transaction.CMS(), transaction.TOPK(), transaction.TDIGEST(), transaction.TS());
        AssertAsyncPrefixes(retryingTransaction.JSON(), retryingTransaction.FT(), retryingTransaction.BF(),
            retryingTransaction.CF(), retryingTransaction.CMS(), retryingTransaction.TOPK(),
            retryingTransaction.TDIGEST(), retryingTransaction.TS());

        static void AssertAsyncPrefixes(
            JsonCommandsAsync json,
            SearchCommandsAsync ft,
            BloomCommandsAsync bf,
            CuckooCommandsAsync cf,
            CmsCommandsAsync cms,
            TopKCommandsAsync topk,
            TdigestCommandsAsync tdigest,
            TimeSeriesCommandsAsync ts)
        {
            Assert.IsType<JsonCommandsAsync>(json);
            Assert.IsType<SearchCommandsAsync>(ft);
            Assert.IsType<BloomCommandsAsync>(bf);
            Assert.IsType<CuckooCommandsAsync>(cf);
            Assert.IsType<CmsCommandsAsync>(cms);
            Assert.IsType<TopKCommandsAsync>(topk);
            Assert.IsType<TdigestCommandsAsync>(tdigest);
            Assert.IsType<TimeSeriesCommandsAsync>(ts);
        }
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task MultiCommandHelpersRejectDeferredReceivers(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        IDatabaseAsync[] deferred =
        [
            db.CreateBatch(),
            db.CreateTransaction(),
            db.WithRetry(RetryPolicy.Default).CreateTransaction(),
        ];

        foreach (var receiver in deferred)
        {
            var jsonEx = await Assert.ThrowsAsync<NotSupportedException>(() =>
                receiver.JSON().SetFromDirectoryAsync("$", Path.GetTempPath()));
            Assert.Contains(nameof(IJsonCommandsAsync.SetFromDirectoryAsync), jsonEx.Message);

            var searchEx = Assert.Throws<NotSupportedException>(() =>
                receiver.FT().AggregateAsyncEnumerable("idx", new AggregationRequest("*")));
            Assert.Contains(nameof(ISearchCommandsAsync.AggregateAsyncEnumerable), searchEx.Message);

            var tsEx = Assert.Throws<NotSupportedException>(() =>
                receiver.TS().ReadAsyncEnumerable("ts", "-"));
            Assert.Contains(nameof(ITimeSeriesCommandsAsync.ReadAsyncEnumerable), tsEx.Message);
        }
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task PrefixesWorkOnRetryWrappedDatabase(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var retrying = db.WithRetry(RetryPolicy.Default);

        Assert.True(await retrying.JSON().SetAsync("prefix-retry-json", "$", "{\"Age\":23}"));
        Assert.Equal("{\"Age\":23}", (await retrying.JSON().GetAsync("prefix-retry-json")).ToString());
    }

    [SkipIfRedisTheory(Is.Enterprise, Comparison.LessThan, "7.1.242")]
    [MemberData(nameof(EndpointsFixture.Env.StandaloneOnly), MemberType = typeof(EndpointsFixture.Env))]
    public async Task PrefixesWorkOnBatch(string endpointId)
    {
        var db = GetCleanDatabase(endpointId);
        var batch = db.CreateBatch();

        var set = batch.JSON().SetAsync("prefix-batch-json", "$", "{\"Age\":23}");
        batch.Execute();

        Assert.True(await set);
        Assert.Equal("{\"Age\":23}", db.JSON().Get("prefix-batch-json").ToString());
    }
}
