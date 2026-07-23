using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using Xunit;

namespace NRedisStack.Tests.Search;

/// <summary>
/// Unit tests for <see cref="CollectReducer"/> — verify that the builder produces the exact token layout that the Redis
/// Search COLLECT grammar expects. These tests do not require a running server.
/// </summary>
public class CollectReducerTests
{
    [Fact]
    public void CollectExplicitFieldsSortByLimitAndAlias()
    {
        var request = new AggregationRequest("*").GroupBy("@color", Reducers.Collect()
            .Fields("fruit", "sweetness")
            .SortBy(SortedField.Desc("sweetness"))
            .Limit(0, 2)
            .As("top"));

        // narg = FIELDS 2 @fruit @sweetness SORTBY 2 @sweetness DESC LIMIT 0 2 = 11
        object[] expected =
        [
            "*", "GROUPBY", 1, "@color",
            "REDUCE", "COLLECT", 11,
            "FIELDS", 2, "@fruit", "@sweetness",
            "SORTBY", 2, "@sweetness", "DESC",
            "LIMIT", 0, 2,
            "AS", "top"
        ];

        Assert.Equal(expected, request.GetArgs());
    }

    [Fact]
    public void CollectFieldsAllSortByDescAndLimitWithoutAlias()
    {
        var request = new AggregationRequest("*").GroupBy("@color", Reducers.Collect()
            .FieldsAll()
            .SortByDesc("sweetness")
            .Limit(2));

        // narg = FIELDS * SORTBY 2 @sweetness DESC LIMIT 0 2 = 9
        object[] expected =
        [
            "*", "GROUPBY", 1, "@color",
            "REDUCE", "COLLECT", 9,
            "FIELDS", "*",
            "SORTBY", 2, "@sweetness", "DESC",
            "LIMIT", 0, 2
        ];

        Assert.Equal(expected, request.GetArgs());
    }

    [Fact]
    public void CollectMultipleSortKeysNormalizeAtPrefix()
    {
        var request = new AggregationRequest("*").GroupBy("@color", Reducers.Collect()
            .Fields("@__key", "fruit")
            .SortBy(SortedField.Desc("@sweetness"), SortedField.Asc("__key"))
            .As("top"));

        // narg = FIELDS 2 @__key @fruit SORTBY 4 @sweetness DESC @__key ASC = 10
        object[] expected =
        [
            "*", "GROUPBY", 1, "@color",
            "REDUCE", "COLLECT", 10,
            "FIELDS", 2, "@__key", "@fruit",
            "SORTBY", 4, "@sweetness", "DESC", "@__key", "ASC",
            "AS", "top"
        ];

        Assert.Equal(expected, request.GetArgs());
    }

    [Fact]
    public void CollectRejectsInvalidLocalUsage()
    {
        Assert.Throws<InvalidOperationException>(() => Reducers.Collect().Fields("a").FieldsAll());
        Assert.Throws<InvalidOperationException>(() => Reducers.Collect().FieldsAll().Fields("a"));
        Assert.Throws<ArgumentException>(() => Reducers.Collect().Limit(-1, 5));

        // Serialization (which happens eagerly when the reducer is attached to a GROUPBY) fails when neither
        // Fields(...) nor FieldsAll() was configured.
        Assert.Throws<InvalidOperationException>(
            () => new AggregationRequest("*").GroupBy("@color", Reducers.Collect().As("top")));
    }

    [Fact]
    public void CollectRejectsMutationAfterAttachingToGroupBy()
    {
        // GroupBy serializes the reducer eagerly, so builder calls made afterwards could never
        // reach the wire; they must throw rather than be silently dropped.
        var collect = Reducers.Collect().Fields("fruit");
        var request = new AggregationRequest("*").GroupBy("@color", collect);

        Assert.Throws<InvalidOperationException>(() => collect.Fields("sweetness"));
        Assert.Throws<InvalidOperationException>(() => collect.SortByDesc("sweetness"));
        Assert.Throws<InvalidOperationException>(() => collect.Limit(0, 2));

        // The serialized request is unchanged by the rejected calls.
        object[] expected =
        [
            "*", "GROUPBY", 1, "@color",
            "REDUCE", "COLLECT", 3,
            "FIELDS", 1, "@fruit"
        ];
        Assert.Equal(expected, request.GetArgs());
    }
}
