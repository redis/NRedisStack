using System.Buffers;
using System.Diagnostics;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace NRedisStack.Tests.Search;

public class HybridSearchUnitTests(ITestOutputHelper log)
{
    private string Index { get; } = "myindex";

    private ICollection<object> GetArgs(HybridSearchQuery query, IReadOnlyDictionary<string, object>? parameters = null)
    {
        Assert.Equal("FT.HYBRID", query.Command);
        var args = query.GetArgs(Index, parameters).ToArray();
        byte[] buffer = ArrayPool<byte>.Shared.Rent(128);
        for (int i = 0; i < args.Length; i++)
        {
            var val = args[i];
            if (val is RedisValue v)
            {
                // force non-readable values to an ASCII-printable string
                var bytes = v.GetByteCount();
                if (bytes > buffer.Length)
                {
                    ArrayPool<byte>.Shared.Return(buffer);
                    buffer = ArrayPool<byte>.Shared.Rent(bytes);
                }

                var span = buffer.AsSpan(0, v.CopyTo(buffer));
                Debug.Assert(span.Length == bytes, $"expected {bytes}, wrote {span.Length}");
                var format = false;
                foreach (var b in span)
                {
                    if (b < 32 | b >= 127)
                    {
                        format = true;
                        break;
                    }
                }

                if (format) // not how resp-cli works, but: useful enough for unit tests
                {
                    args[i] = Convert.ToBase64String(buffer, 0, bytes);
                }
            }
        }
        ArrayPool<byte>.Shared.Return(buffer);
        log.WriteLine(query.Command + " " + string.Join(" ", args));
        return args;
    }

    [Fact]
    public void EmptySearch()
    {
        HybridSearchQuery query = new();
        object[] expected = [Index];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void BasicSearch()
    {
        HybridSearchQuery query = new();
        query.Search("foo");

        object[] expected = [Index, "SEARCH", "foo"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BasicSearch_WithNullScorer(bool withAlias) // test: no SCORER added
    {
        HybridSearchQuery query = new();
        HybridSearchQuery.SearchConfig queryConfig = "foo";
        if (withAlias) queryConfig = queryConfig.WithScoreAlias("score_alias");
        query.Search(queryConfig);

        object[] expected = [Index, "SEARCH", "foo"];
        if (withAlias)
        {
            expected = [.. expected, "YIELD_SCORE_AS", "score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BasicSearch_WithSimpleScorer(bool withAlias)
    {
        HybridSearchQuery query = new();
        HybridSearchQuery.SearchConfig queryConfig = "foo";
        queryConfig = queryConfig.WithScorer(Scorer.TfIdf);
        if (withAlias) queryConfig = queryConfig.WithScoreAlias("score_alias");
        query.Search(queryConfig);

        object[] expected = [Index, "SEARCH", "foo", "SCORER", "TFIDF"];
        if (withAlias)
        {
            expected = [.. expected, "YIELD_SCORE_AS", "score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData("TFIDF")]
    [InlineData("TFIDF.DOCNORM")]
    [InlineData("BM25STD")]
    [InlineData("BM25STD.NORM")]
    [InlineData("DISMAX")]
    [InlineData("DOCSCORE")]
    [InlineData("HAMMING")]
    public void BasicSearch_WithKnownSimpleScorers(string scenario)
    {
        HybridSearchQuery query = new();
        HybridSearchQuery.SearchConfig queryConfig = "foo";
        queryConfig = queryConfig.WithScorer(scenario switch
        {
            "TFIDF" => Scorer.TfIdf,
            "TFIDF.DOCNORM" => Scorer.TfIdfDocNorm,
            "BM25STD" => Scorer.BM25Std,
            "BM25STD.NORM" => Scorer.BM25StdNorm,
            "DISMAX" => Scorer.DisMax,
            "DOCSCORE" => Scorer.DocScore,
            "HAMMING" => Scorer.Hamming,
            _ => throw new NotImplementedException(),
        });
        query.Search(queryConfig);

        object[] expected = [Index, "SEARCH", "foo", "SCORER", scenario];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void BasicSearch_WithBM25StdTanh()
    {
        HybridSearchQuery query = new();
        query.Search(new("foo", scorer: Scorer.BM25StdTanh(5)));

        object[] expected = [Index, "SEARCH", "foo", "SCORER", "BM25STD.TANH", "BM25STD_TANH_FACTOR", 5];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void BasicVectorSearch()
    {
        HybridSearchQuery query = new();
        byte[] blob = [];
        query.VectorSearch("vfield", VectorData.Raw(blob));

        object[] expected = [Index, "VSIM", "vfield", ""];
        Assert.Equivalent(expected, GetArgs(query));
    }

    private static readonly VectorData<float> SomeRandomDataHere = VectorData.LeaseWithValues<float>(1, 2, 3, 4 );

    private const string SomeRandomVectorValue = "AACAPwAAAEAAAEBAAACAQA==";

    [Fact]
    public void BasicNonZeroLengthVectorSearch()
    {
        HybridSearchQuery query = new();
        query.VectorSearch("vfield", SomeRandomDataHere);

        object[] expected = [Index, "VSIM", "vfield", SomeRandomVectorValue];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void BasicVectorSearch_WithKNN(bool withScoreAlias, bool withDistanceAlias)
    {
        HybridSearchQuery query = new();
        var searchConfig = new HybridSearchQuery.VectorSearchConfig("vField", SomeRandomDataHere);
        if (withScoreAlias) searchConfig = searchConfig.WithScoreAlias("my_score_alias");
        searchConfig = searchConfig.WithMethod(VectorSearchMethod.NearestNeighbour(null, null,
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSearch(searchConfig);

        object[] expected =
            [Index, "VSIM", "vField", SomeRandomVectorValue, "KNN", withDistanceAlias ? 4 : 2, "K", 10];
        if (withDistanceAlias)
        {
            expected = [.. expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [.. expected, "YIELD_SCORE_AS", "my_score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void BasicVectorSearch_WithKNN_WithEF(bool withScoreAlias, bool withDistanceAlias)
    {
        HybridSearchQuery query = new();
        var searchConfig = new HybridSearchQuery.VectorSearchConfig("vfield", SomeRandomDataHere);
        if (withScoreAlias) searchConfig = searchConfig.WithScoreAlias("my_score_alias");
        searchConfig = searchConfig.WithMethod(VectorSearchMethod.NearestNeighbour(
            16,
            maxTopCandidates: 100,
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSearch(searchConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", SomeRandomVectorValue, "KNN", withDistanceAlias ? 6 : 4, "K", 16,
            "EF_RUNTIME", 100
        ];
        if (withDistanceAlias)
        {
            expected = [.. expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [.. expected, "YIELD_SCORE_AS", "my_score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void BasicVectorSearch_WithRange(bool withScoreAlias, bool withDistanceAlias)
    {
        HybridSearchQuery query = new();
        var searchConfig = new HybridSearchQuery.VectorSearchConfig("vfield", SomeRandomDataHere);
        if (withScoreAlias) searchConfig = searchConfig.WithScoreAlias("my_score_alias");
        searchConfig = searchConfig.WithMethod(VectorSearchMethod.Range(4.2, null,
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSearch(searchConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", SomeRandomVectorValue, "RANGE", withDistanceAlias ? 4 : 2, "RADIUS",
            4.2
        ];
        if (withDistanceAlias)
        {
            expected = [.. expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [.. expected, "YIELD_SCORE_AS", "my_score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void BasicVectorSearch_WithRange_WithEpsilon(bool withScoreAlias, bool withDistanceAlias)
    {
        HybridSearchQuery query = new();
        HybridSearchQuery.VectorSearchConfig vsimConfig = new("vfield", SomeRandomDataHere);
        if (withScoreAlias) vsimConfig = vsimConfig.WithScoreAlias("my_score_alias");
        vsimConfig = vsimConfig.WithMethod(VectorSearchMethod.Range(4.2,
            epsilon: 0.06,
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSearch(vsimConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", SomeRandomVectorValue, "RANGE", withDistanceAlias ? 6 : 4, "RADIUS",
            4.2, "EPSILON", 0.06
        ];
        if (withDistanceAlias)
        {
            expected = [.. expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [.. expected, "YIELD_SCORE_AS", "my_score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void BasicVectorSearch_WithFilter_NoPolicy()
    {
        HybridSearchQuery query = new();
        query.VectorSearch(new("vfield", SomeRandomDataHere, filter: "@foo:bar"));

        object[] expected =
        [
            Index, "VSIM", "vfield", SomeRandomVectorValue, "FILTER", "@foo:bar"
        ];

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Combine_DefaultLinear()
    {
        HybridSearchQuery query = new();
        query.Combine(HybridSearchQuery.Combiner.Linear());
        object[] expected = [Index, "COMBINE", "LINEAR", 4, "ALPHA", 0.3, "BETA", 0.7];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Combine_Linear_EqualSplit_WithAlias()
    {
        HybridSearchQuery query = new();
        query.Combine(HybridSearchQuery.Combiner.Linear(0.5, 0.5), "my_combined_alias");
        object[] expected =
            [Index, "COMBINE", "LINEAR", 4, "ALPHA", 0.5, "BETA", 0.5, "YIELD_SCORE_AS", "my_combined_alias"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Combine_DefaultRrf_WithAlias()
    {
        HybridSearchQuery query = new();
        query.Combine(HybridSearchQuery.Combiner.ReciprocalRankFusion(), "my_combined_alias");
        object[] expected = [Index, "COMBINE", "RRF", 2, "WINDOW", 20, "YIELD_SCORE_AS", "my_combined_alias"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(null, null)]
    [InlineData(42, null)]
    [InlineData(null, 12.1)]
    [InlineData(42, 12.1)]
    public void Combine_NonDefaultRrf(int? window, double? constant)
    {
        HybridSearchQuery query = new();
        query.Combine(HybridSearchQuery.Combiner.ReciprocalRankFusion(window, constant));
        object[] expected = [Index, "COMBINE", "RRF", 2 + (constant is not null ? 2 : 0), "WINDOW", window ?? 20];

        if (constant is not null)
        {
            expected = [.. expected, "CONSTANT", constant];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void LoadField()
    {
        HybridSearchQuery query = new();
        query.ReturnFields("field1");
        object[] expected = [Index, "LOAD", 1, "field1"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void LoadFields()
    {
        HybridSearchQuery query = new();
        query.ReturnFields("field1", "field2");
        object[] expected = [Index, "LOAD", 2, "field1", "field2"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void LoadEmptyFields()
    {
        HybridSearchQuery query = new();
        query.ReturnFields([]);
        object[] expected = [Index];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void GroupBy_SingleField()
    {
        HybridSearchQuery query = new();
        query.GroupBy("field1");
        object[] expected = [Index, "GROUPBY", 1, "field1"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void GroupBy_SingleField_WithReducer_NoAlias()
    {
        HybridSearchQuery query = new();
        query.GroupBy("field1")
            .Reduce(Reducers.Count().As(null!)); // workaround https://github.com/redis/NRedisStack/issues/453
        object[] expected = [Index, "GROUPBY", 1, "field1", "REDUCE", "COUNT", 0];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void GroupBy_SingleField_WithReducer_WithAlias()
    {
        HybridSearchQuery query = new();
        query.GroupBy("field1").Reduce(Reducers.Count().As("qty"));
        object[] expected = [Index, "GROUPBY", 1, "field1", "REDUCE", "COUNT", 0, "AS", "qty"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void GroupBy_SingleField_WithReducers_WithAlias()
    {
        HybridSearchQuery query = new();
        query.GroupBy("field1").Reduce(
            Reducers.Min("@field2").As("min"),
            Reducers.Max("@field2").As("max"),
            Reducers.Count().As("qty"));
        object[] expected =
        [
            Index, "GROUPBY", 1, "field1",
            "REDUCE", "MIN", 1, "@field2", "AS", "min",
            "REDUCE", "MAX", 1, "@field2", "AS", "max",
            "REDUCE", "COUNT", 0, "AS", "qty"
        ];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void GroupBy_MultipleFields()
    {
        HybridSearchQuery query = new();
        query.GroupBy(["field1", "field2"]);
        object[] expected = [Index, "GROUPBY", 2, "field1", "field2"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void GroupBy_MultipleFields_WithReducer()
    {
        HybridSearchQuery query = new();
        query.GroupBy(["field1", "field2"]).Reduce(Reducers.Quantile("@field3", 0.5));
        object[] expected = [Index, "GROUPBY", 2, "field1", "field2", "REDUCE", "QUANTILE", 2, "@field3", 0.5];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Apply()
    {
        HybridSearchQuery query = new();
        query.Apply(new("@field1 + @field2", "sum"));
        object[] expected = [Index, "APPLY", "@field1 + @field2", "AS", "sum"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Apply_Multi()
    {
        HybridSearchQuery query = new();
        query.Apply(new("@field1 + @field2", "sum"), "@field3 + @field4");
        object[] expected = [Index, "APPLY", "@field1 + @field2", "AS", "sum", "APPLY", "@field3 + @field4"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_EmptyStrings()
    {
        HybridSearchQuery query = new();
        string[] sortBy = [];
        query.SortBy(sortBy);
        object[] expected = [Index];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_EmptySortedFields()
    {
        HybridSearchQuery query = new();
        SortedField[] sortBy = [];
        query.SortBy(sortBy);
        object[] expected = [Index];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void NoSort()
    {
        HybridSearchQuery query = new();
        query.NoSort();
        object[] expected = [Index, "NOSORT"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_SingleString()
    {
        HybridSearchQuery query = new();
        query.SortBy("field1");
        object[] expected = [Index, "SORTBY", 1, "field1"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_SingleSortedFieldAsc()
    {
        HybridSearchQuery query = new();
        query.SortBy(SortedField.Asc("field1"));
        object[] expected = [Index, "SORTBY", 1, "field1"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_SingleSortedFieldDesc()
    {
        HybridSearchQuery query = new();
        query.SortBy(SortedField.Desc("field1"));
        object[] expected = [Index, "SORTBY", 2, "field1", "DESC"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_MultiString()
    {
        HybridSearchQuery query = new();
        query.SortBy("field1", "field2");
        object[] expected = [Index, "SORTBY", 2, "field1", "field2"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_MultiSortedFieldAsc()
    {
        HybridSearchQuery query = new();
        query.SortBy(SortedField.Asc("field1"), SortedField.Asc("field2"));
        object[] expected = [Index, "SORTBY", 2, "field1", "field2"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_MultiSortedFieldDesc()
    {
        HybridSearchQuery query = new();
        query.SortBy(SortedField.Desc("field1"), SortedField.Desc("field2"));
        object[] expected = [Index, "SORTBY", 4, "field1", "DESC", "field2", "DESC"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SortBy_MultiSortedFieldMixed()
    {
        HybridSearchQuery query = new();
        query.SortBy(SortedField.Desc("field1"), SortedField.Asc("field2"));
        object[] expected = [Index, "SORTBY", 3, "field1", "DESC", "field2"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Filter()
    {
        HybridSearchQuery query = new();
        query.Filter("@field1:bar");
        object[] expected = [Index, "FILTER", "@field1:bar"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Limit()
    {
        HybridSearchQuery query = new();
        query.Limit(12, 54);
        object[] expected = [Index, "LIMIT", 12, 54];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void ExplainScoreImplicit()
    {
        HybridSearchQuery query = new();
        query.ExplainScore();
        object[] expected = [Index, "EXPLAINSCORE"];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ExplainScoreExplicit(bool enabled)
    {
        HybridSearchQuery query = new();
        query.ExplainScore(enabled);
        object[] expected = enabled ? [Index, "EXPLAINSCORE"] : [Index];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Timeout()
    {
        HybridSearchQuery query = new();
        query.Timeout(TimeSpan.FromSeconds(1));
        object[] expected = [Index, "TIMEOUT", 1000];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void SimpleCursor()
    {
        HybridSearchQuery query = new();
        query.WithCursor(10);
        object[] expected = [Index, "WITHCURSOR"];
        Assert.Equivalent(expected, GetArgs(query));
    }


    [Fact]
    public void CusorWithCount()
    {
        HybridSearchQuery query = new();
        query.WithCursor(15);
        object[] expected = [Index, "WITHCURSOR", "COUNT", 15];
        Assert.Equivalent(expected, GetArgs(query));
    }


    [Fact]
    public void CursorWithMaxIdle()
    {
        HybridSearchQuery query = new();
        query.WithCursor(maxIdle: TimeSpan.FromSeconds(10));
        object[] expected = [Index, "WITHCURSOR", "MAXIDLE", 10000];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void CursorWithCountAndMaxIdle()
    {
        HybridSearchQuery query = new();
        query.WithCursor(15, maxIdle: TimeSpan.FromSeconds(10));
        object[] expected = [Index, "WITHCURSOR", "COUNT", 15, "MAXIDLE", 10000];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void ParameterizedQuery()
    {
        HybridSearchQuery query = new();
        query.Search("$s").VectorSearch("@field", "$v");

        // issue that query, with parameter values from a dictionary
        IReadOnlyDictionary<string, object> args = new Dictionary<string, object>
        {
            { "s", "abc"},
            { "v", SomeRandomDataHere }
        };
        object[] expected = [Index, "SEARCH", "$s", "VSIM", "@field", "$v", "PARAMS", 4, "s", "abc", "v", SomeRandomVectorValue];
        var idx = Array.IndexOf(expected, "abc");
        Assert.True(idx >= 0);
        Assert.Equivalent(expected, GetArgs(query, args));

        // issue a second query against the same "query" instance, with different parameter values, this time from an object
        args = Parameters.From(new { s = "def", v = SomeRandomDataHere });

        expected[idx] = "def"; // update our expectations
        expected[idx + 2] = SomeRandomVectorValue;
        Assert.Equivalent(expected, GetArgs(query, args));
    }

    [Fact]
    public void MakeMeOneWithEverything()
    {
        HybridSearchQuery query = new();
        var args = new Dictionary<string, object>
        {
            ["x"] = 42,
            ["y"] = "abc"
        };
        using var vector = VectorData.LeaseWithValues<float>(1, 2, 3);
        query.Search(new("foo", Scorer.BM25StdTanh(5), "text_score_alias"))
            .VectorSearch(new HybridSearchQuery.VectorSearchConfig("bar", vector,
                    VectorSearchMethod.NearestNeighbour(10, 100, "vector_distance_alias"))
                .WithFilter("@foo:bar").WithScoreAlias("vector_score_alias"))
            .Combine(HybridSearchQuery.Combiner.ReciprocalRankFusion(10, 0.5), "my_combined_alias")
            .ReturnFields("field1", "field2")
            .GroupBy("field1").Reduce(Reducers.Quantile("@field3", 0.5).As("reducer_alias"))
            .Apply(new("@field1 + @field2", "apply_alias"))
            .SortBy(SortedField.Asc("field1"), SortedField.Desc("field2"))
            .Filter("@field1:bar")
            .Limit(12, 54)
            .ExplainScore()
            .Timeout(TimeSpan.FromSeconds(1))
            .WithCursor(10, TimeSpan.FromSeconds(10));
        object[] expected =
        [
            Index, "SEARCH", "foo", "SCORER", "BM25STD.TANH", "BM25STD_TANH_FACTOR", 5, "YIELD_SCORE_AS",
            "text_score_alias", "VSIM", "bar",
            "AACAPwAAAEAAAEBA", "KNN", 6, "K", 10, "EF_RUNTIME", 100, "YIELD_DISTANCE_AS", "vector_distance_alias", "FILTER",
            "@foo:bar", "YIELD_SCORE_AS", "vector_score_alias", "COMBINE", "RRF", 4, "WINDOW", 10, "CONSTANT", 0.5,
            "YIELD_SCORE_AS", "my_combined_alias", "LOAD", 2, "field1", "field2", "GROUPBY", 1, "field1", "REDUCE",
            "QUANTILE", 2, "@field3", 0.5, "AS", "reducer_alias", "APPLY", "@field1 + @field2", "AS", "apply_alias",
            "SORTBY", 3, "field1", "field2", "DESC", "FILTER", "@field1:bar", "LIMIT", 12, 54,
            "PARAMS", 4, "x", 42, "y", "abc",
            "EXPLAINSCORE", "TIMEOUT", 1000,
            "WITHCURSOR", "COUNT", 10, "MAXIDLE", 10000
        ];

        log.WriteLine(query.Command + " " + string.Join(" ", expected));
        log.WriteLine("vs");
        Assert.Equivalent(expected, GetArgs(query, args));
    }

    [Fact]
    public void Freezing()
    {
        HybridSearchQuery query = new();
        query.Limit(3, 4); // fine
        query.Limit(5, 6); // fine

        query.GetArgs("abc", null); // indirect freeze
        Assert.Throws<InvalidOperationException>(() => query.Limit(7, 8));

        query.AllowModification();
        query.Limit(9, 10); // fine
        query.Limit(11, 12); // fine
    }
}