using StackExchange.Redis;
using Xunit;

namespace NRedisStack.Tests;

public class CommunityEditionUpdatesTests : AbstractNRedisStackTest, IDisposable
{
    public CommunityEditionUpdatesTests(EndpointsFixture endpointsFixture) : base(endpointsFixture) { }

    private IServer getAnyPrimary(IConnectionMultiplexer muxer)
    {
        foreach (var endpoint in muxer.GetEndPoints())
        {
            var server = muxer.GetServer(endpoint);
            if (!server.IsReplica) return server;
        }
        throw new InvalidOperationException("Requires a primary endpoint (found none)");
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void ConfigSearchSettings(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        IConnectionMultiplexer muxer = db.Multiplexer;
        IServer server = getAnyPrimary(muxer);

        server.ConfigSet("search-on-timeout", "fail");

        Assert.Equal("fail", server.ConfigGet("search-on-timeout").First().Value);

        server.ConfigSet("search-on-timeout", "return");

        Assert.Single(server.ConfigGet("search-min-prefix"));

        Assert.Single(server.ConfigGet("search-max-prefix-expansions"));

        Assert.Single(server.ConfigGet("search-max-search-results"));

        Assert.Single(server.ConfigGet("search-max-aggregate-results"));

        Assert.Single(server.ConfigGet("search-default-dialect"));
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void ConfigTimeSeriesSettings(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        IConnectionMultiplexer muxer = db.Multiplexer;
        IServer server = getAnyPrimary(muxer);

        Assert.Single(server.ConfigGet("ts-compaction-policy"));

        Assert.Single(server.ConfigGet("ts-retention-policy"));

        Assert.Single(server.ConfigGet("ts-duplicate-policy"));

        Assert.Single(server.ConfigGet("ts-encoding"));

        Assert.Single(server.ConfigGet("ts-chunk-size-bytes"));

        Assert.Single(server.ConfigGet("ts-ignore-max-time-diff"));

        Assert.Single(server.ConfigGet("ts-ignore-max-val-diff"));
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void ConfigProbabilisticSettings(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        IConnectionMultiplexer muxer = db.Multiplexer;
        IServer server = getAnyPrimary(muxer);

        server.ConfigSet("bf-error-rate", "0.02");

        Assert.Single(server.ConfigGet("bf-error-rate"));

        Assert.Equal("0.02", server.ConfigGet("bf-error-rate").First().Value);

        Assert.Single(server.ConfigGet("bf-initial-size"));

        Assert.Single(server.ConfigGet("cf-max-expansions"));

        Assert.Single(server.ConfigGet("bf-expansion-factor"));

        Assert.Single(server.ConfigGet("cf-expansion-factor"));

        Assert.Single(server.ConfigGet("cf-initial-size"));

        Assert.Single(server.ConfigGet("cf-bucket-size"));

        Assert.Single(server.ConfigGet("cf-max-iterations"));
    }

    [SkipIfRedisTheory(Comparison.LessThan, "7.9.0")]
    [MemberData(nameof(EndpointsFixture.Env.AllEnvironments), MemberType = typeof(EndpointsFixture.Env))]
    public void InfoSearchSection(string endpointId)
    {
        IDatabase db = GetCleanDatabase(endpointId);
        IConnectionMultiplexer muxer = db.Multiplexer;
        IServer server = getAnyPrimary(muxer);

        var searchInfo = server.Info("search");
        CustomAssertions.GreaterThan(9, searchInfo.Length);
    }

}