using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using Xunit;

namespace NRedisStack.Tests.Search;

/// <summary>
/// Unit tests for the <c>WITHCOUNT</c> / <c>WITHOUTCOUNT</c> options of <see cref="Query"/> and
/// <see cref="AggregationRequest"/> — verify where the builders put them. These tests do not require a
/// running server.
/// </summary>
public class CountOptionTests
{
    private static string[] SearchArgs(Query query) =>
        SearchCommandBuilder.Search("idx", query).Args.Select(arg => arg.ToString()!).ToArray();

    [Fact]
    public void QuerySendsNoCountOptionByDefault()
    {
        var args = SearchArgs(new Query("foo"));

        Assert.DoesNotContain("WITHCOUNT", args);
        Assert.DoesNotContain("WITHOUTCOUNT", args);
    }

    [Fact]
    public void QueryWithCount()
    {
        var args = SearchArgs(new Query("foo").SetWithCount());

        Assert.Contains("WITHCOUNT", args);
        Assert.DoesNotContain("WITHOUTCOUNT", args);
    }

    [Fact]
    public void QueryWithoutCount()
    {
        var args = SearchArgs(new Query("foo").SetWithoutCount());

        Assert.Contains("WITHOUTCOUNT", args);
        Assert.DoesNotContain("WITHCOUNT", args);
    }

    [Fact]
    public void AggregationCountOptionFollowsQueryWhenSetLast()
    {
        // The server rejects the option after a pipeline step such as LOAD, so it has to follow the
        // query even when it is set last.
        var request = new AggregationRequest("@n:[0 10]")
            .Load(FieldName.Of("@n"))
            .SortBy(10, SortedField.Asc("@n"))
            .WithoutCount();

        object[] expected =
        [
            "@n:[0 10]", "WITHOUTCOUNT",
            "LOAD", 1, "@n",
            "SORTBY", 2, "@n", "ASC", "MAX", 10
        ];

        Assert.Equal(expected, request.GetArgs());
    }

    [Fact]
    public void AggregationLaterCountOptionReplacesEarlierOne()
    {
        var request = new AggregationRequest("*")
            .WithoutCount()
            .SortBy(SortedField.Asc("@n"))
            .WithCount();

        object[] expected = ["*", "WITHCOUNT", "SORTBY", 2, "@n", "ASC"];

        Assert.Equal(expected, request.GetArgs());
    }
}
