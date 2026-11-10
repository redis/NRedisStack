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

    private static object[] AggregateArgs(AggregationRequest request) =>
        SearchCommandBuilder.Aggregate("idx", request).Args.Skip(1).ToArray();

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
        var args = SearchArgs(new Query("foo").SetWithCount(false));

        Assert.Contains("WITHOUTCOUNT", args);
        Assert.DoesNotContain("WITHCOUNT", args);
    }

    [Fact]
    public void QueryCountOptionCanBeCleared()
    {
        var args = SearchArgs(new Query("foo").SetWithCount(false).SetWithCount(null));

        Assert.DoesNotContain("WITHCOUNT", args);
        Assert.DoesNotContain("WITHOUTCOUNT", args);
    }

    [Fact]
    public void AggregationCountOptionFollowsQueryWhenSetLast()
    {
        // The server rejects the option after a pipeline step such as LOAD, so it has to follow the
        // query even when it is set last.
        var request = new AggregationRequest("@n:[0 10]")
            .Load(FieldName.Of("@n"))
            .SortBy(10, SortedField.Asc("@n"))
            .WithCount(false);

        object[] expected =
        [
            "@n:[0 10]", "WITHOUTCOUNT",
            "LOAD", 1, "@n",
            "SORTBY", 2, "@n", "ASC", "MAX", 10
        ];

        Assert.Equal(expected, AggregateArgs(request));
    }

    [Fact]
    public void AggregationLaterCountOptionReplacesEarlierOne()
    {
        var request = new AggregationRequest("*")
            .WithCount(false)
            .SortBy(SortedField.Asc("@n"))
            .WithCount();

        object[] expected = ["*", "WITHCOUNT", "SORTBY", 2, "@n", "ASC"];

        Assert.Equal(expected, AggregateArgs(request));
    }

    [Fact]
    public void AggregationCountOptionCanBeCleared()
    {
        var request = new AggregationRequest("*")
            .WithCount(false)
            .SortBy(SortedField.Asc("@n"))
            .WithCount(null);

        object[] expected = ["*", "SORTBY", 2, "@n", "ASC"];

        Assert.Equal(expected, AggregateArgs(request));
    }

    [Fact]
    public void AggregationCountOptionLeavesCallerInsertedArgsIntact()
    {
        var request = new AggregationRequest("*").WithCount();
        request.GetArgs().Insert(1, "VERBATIM");
        request.WithCount(false);

        object[] expected = ["*", "WITHOUTCOUNT", "VERBATIM"];

        Assert.Equal(expected, AggregateArgs(request));
    }
}
