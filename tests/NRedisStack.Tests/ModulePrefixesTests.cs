using NRedisStack.RedisStackCommands;
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

        // an IDatabase is the more specific receiver, so it keeps the sync-capable commands
        Assert.IsType<JsonCommands>(db.JSON());
        Assert.IsType<SearchCommands>(db.FT());
        Assert.IsType<BloomCommands>(db.BF());
        Assert.IsType<CuckooCommands>(db.CF());
        Assert.IsType<CmsCommands>(db.CMS());
        Assert.IsType<TopKCommands>(db.TOPK());
        Assert.IsType<TdigestCommands>(db.TDIGEST());
        Assert.IsType<TimeSeriesCommands>(db.TS());

        // anything else that's an IDatabaseAsync gets the async-only ones
        foreach (var receiver in new[] { asyncOnly, retrying, batch, transaction })
        {
            Assert.IsType<JsonCommandsAsync>(receiver.JSON());
            Assert.IsType<SearchCommandsAsync>(receiver.FT());
            Assert.IsType<BloomCommandsAsync>(receiver.BF());
            Assert.IsType<CuckooCommandsAsync>(receiver.CF());
            Assert.IsType<CmsCommandsAsync>(receiver.CMS());
            Assert.IsType<TopKCommandsAsync>(receiver.TOPK());
            Assert.IsType<TdigestCommandsAsync>(receiver.TDIGEST());
            Assert.IsType<TimeSeriesCommandsAsync>(receiver.TS());
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
