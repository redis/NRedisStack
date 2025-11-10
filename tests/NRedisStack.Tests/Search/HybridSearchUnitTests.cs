using System.Reflection;
using System.Text;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using StackExchange.Redis;
using Xunit;
using Xunit.Abstractions;

namespace NRedisStack.Tests.Search;

public class HybridSearchUnitTests(ITestOutputHelper log)
{
    private readonly RedisKey _index = "myindex";
    private ref readonly RedisKey Index => ref _index;

    private ICollection<object> GetArgs(HybridSearchQuery query, Dictionary<string, object>? parameters = null)
    {
        Assert.Equal("FT.HYBRID", query.Command);
        var args = query.GetArgs(Index, parameters);
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
        HybridSearchQuery.QueryConfig queryConfig = new();
        if (withAlias) queryConfig.ScoreAlias("score_alias");
        query.Search("foo", queryConfig);

        object[] expected = [Index, "SEARCH", "foo"];
        if (withAlias)
        {
            expected = [..expected, "YIELD_SCORE_AS", "score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BasicSearch_WithSimpleScorer(bool withAlias)
    {
        HybridSearchQuery query = new();
        HybridSearchQuery.QueryConfig queryConfig = new();
        queryConfig.Scorer(Scorer.TfIdf);
        if (withAlias) queryConfig.ScoreAlias("score_alias");
        query.Search("foo", queryConfig);

        object[] expected = [Index, "SEARCH", "foo", "SCORER", "TFIDF"];
        if (withAlias)
        {
            expected = [..expected, "YIELD_SCORE_AS", "score_alias"];
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
        HybridSearchQuery.QueryConfig queryConfig = new();
        queryConfig.Scorer(scenario switch
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
        query.Search("foo", queryConfig);

        object[] expected = [Index, "SEARCH", "foo", "SCORER", scenario];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void BasicSearch_WithBM25StdTanh()
    {
        HybridSearchQuery query = new();
        query.Search("foo", new HybridSearchQuery.QueryConfig().Scorer(Scorer.BM25StdTanh(5)));

        object[] expected = [Index, "SEARCH", "foo", "SCORER", "BM25STD.TANH", "BM25STD_TANH_FACTOR", 5];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void BasicZeroLengthVectorSearch(bool withConfig)
    {
        HybridSearchQuery query = new();
        if (withConfig)
        {
            HybridSearchQuery.VectorSearchConfig config = new();
            query.VectorSimilaritySearch("vfield", Array.Empty<byte>(), config);
        }
        else
        {
            query.VectorSimilaritySearch("vfield", Array.Empty<byte>());
        }

        object[] expected = [Index, "VSIM", "vfield", ""];
        Assert.Equivalent(expected, GetArgs(query));
    }

    private static readonly ReadOnlyMemory<byte> SomeRandomDataHere = Encoding.UTF8.GetBytes("some random data here!");

    [Fact]
    public void BasicNonZeroLengthVectorSearch()
    {
        HybridSearchQuery query = new();
        query.VectorSimilaritySearch("vfield", SomeRandomDataHere);

        object[] expected = [Index, "VSIM", "vfield", "c29tZSByYW5kb20gZGF0YSBoZXJlIQ=="];
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
        var searchConfig = new HybridSearchQuery.VectorSearchConfig();
        if (withScoreAlias) searchConfig.ScoreAlias("my_score_alias");
        searchConfig.Method(HybridSearchQuery.VectorSearchMethod.NearestNeighbour(
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSimilaritySearch("vfield", SomeRandomDataHere, searchConfig);

        object[] expected =
            [Index, "VSIM", "vfield", "c29tZSByYW5kb20gZGF0YSBoZXJlIQ==", "KNN", withDistanceAlias ? 4 : 2, "K", 10];
        if (withDistanceAlias)
        {
            expected = [..expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [..expected, "YIELD_SCORE_AS", "my_score_alias"];
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
        var searchConfig = new HybridSearchQuery.VectorSearchConfig();
        if (withScoreAlias) searchConfig.ScoreAlias("my_score_alias");
        searchConfig.Method(HybridSearchQuery.VectorSearchMethod.NearestNeighbour(
            16,
            maxTopCandidates: 100,
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSimilaritySearch("vfield", SomeRandomDataHere, searchConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", "c29tZSByYW5kb20gZGF0YSBoZXJlIQ==", "KNN", withDistanceAlias ? 6 : 4, "K", 16,
            "EF_RUNTIME", 100
        ];
        if (withDistanceAlias)
        {
            expected = [..expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [..expected, "YIELD_SCORE_AS", "my_score_alias"];
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
        var searchConfig = new HybridSearchQuery.VectorSearchConfig();
        if (withScoreAlias) searchConfig.ScoreAlias("my_score_alias");
        searchConfig.Method(HybridSearchQuery.VectorSearchMethod.Range(4.2,
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSimilaritySearch("vfield", SomeRandomDataHere, searchConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", "c29tZSByYW5kb20gZGF0YSBoZXJlIQ==", "RANGE", withDistanceAlias ? 4 : 2, "RADIUS",
            4.2
        ];
        if (withDistanceAlias)
        {
            expected = [..expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [..expected, "YIELD_SCORE_AS", "my_score_alias"];
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
        var searchConfig = new HybridSearchQuery.VectorSearchConfig();
        if (withScoreAlias) searchConfig.ScoreAlias("my_score_alias");
        searchConfig.Method(HybridSearchQuery.VectorSearchMethod.Range(4.2,
            epsilon: 0.06,
            distanceAlias: withDistanceAlias ? "my_distance_alias" : null));
        query.VectorSimilaritySearch("vfield", SomeRandomDataHere, searchConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", "c29tZSByYW5kb20gZGF0YSBoZXJlIQ==", "RANGE", withDistanceAlias ? 6 : 4, "RADIUS",
            4.2, "EPSILON", 0.06
        ];
        if (withDistanceAlias)
        {
            expected = [..expected, "YIELD_DISTANCE_AS", "my_distance_alias"];
        }

        if (withScoreAlias)
        {
            expected = [..expected, "YIELD_SCORE_AS", "my_score_alias"];
        }

        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void BasicVectorSearch_WithFilter_NoPolicy()
    {
        HybridSearchQuery query = new();
        var searchConfig = new HybridSearchQuery.VectorSearchConfig();
        searchConfig.Filter("@foo:bar");
        query.VectorSimilaritySearch("vfield", SomeRandomDataHere, searchConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", "c29tZSByYW5kb20gZGF0YSBoZXJlIQ==", @"FILTER", "@foo:bar"
        ];

        Assert.Equivalent(expected, GetArgs(query));
    }
    
    [Theory]
    [InlineData(HybridSearchQuery.VectorSearchConfig.VectorFilterPolicy.AdHoc)]
    [InlineData(HybridSearchQuery.VectorSearchConfig.VectorFilterPolicy.Batches)]
    [InlineData(HybridSearchQuery.VectorSearchConfig.VectorFilterPolicy.Batches, 100)]
    [InlineData(HybridSearchQuery.VectorSearchConfig.VectorFilterPolicy.Acorn)]
    public void BasicVectorSearch_WithFilter_WithPolicy(HybridSearchQuery.VectorSearchConfig.VectorFilterPolicy policy, int? batchSize = null)
    {
        HybridSearchQuery query = new();
        var searchConfig = new HybridSearchQuery.VectorSearchConfig();
        searchConfig.Filter("@foo:bar", policy, batchSize);
        query.VectorSimilaritySearch("vfield", SomeRandomDataHere, searchConfig);

        object[] expected =
        [
            Index, "VSIM", "vfield", "c29tZSByYW5kb20gZGF0YSBoZXJlIQ==", @"FILTER", "@foo:bar", "POLICY", policy.ToString().ToUpper()
        ];
        if (batchSize != null)
        {
            expected = [..expected, "BATCH_SIZE", batchSize];
        }
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Combine_DefaultLinear()
    {
        HybridSearchQuery query = new();
        query.Combine(HybridSearchQuery.Combiner.Linear());
        object[] expected = [Index, "COMBINE", "LINEAR", 0];
        Assert.Equivalent(expected, GetArgs(query));
    }
    
    [Fact]
    public void Combine_Linear_EqualSplit_WithAlias()
    {
        HybridSearchQuery query = new();
        query.Combine(HybridSearchQuery.Combiner.Linear(0.5, 0.5), "my_combined_alias");
        object[] expected = [Index, "COMBINE", "LINEAR", 4, "ALPHA", 0.5, "BETA", 0.5, "YIELD_SCORE_AS", "my_combined_alias"];
        Assert.Equivalent(expected, GetArgs(query));
    }
    
    [Fact]
    public void Combine_DefaultRrf_WithAlias()
    {
        HybridSearchQuery query = new();
        query.Combine(HybridSearchQuery.Combiner.ReciprocalRankFusion(), "my_combined_alias");
        object[] expected = [Index, "COMBINE", "RRF", 0, "YIELD_SCORE_AS", "my_combined_alias"];
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
        object[] expected = [Index, "COMBINE", "RRF", (window is not null ? 2 : 0) + (constant is not null ? 2 : 0)];
        if (window is not null)
        {
            expected = [..expected, "WINDOW", window];
        }

        if (constant is not null)
        {
            expected = [..expected, "CONSTANT", constant];
        }
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void LoadFields()
    {
        HybridSearchQuery query = new();
        query.Load("field1", "field2");
        object[] expected = [Index, "LOAD", 2, "field1", "field2"];
        Assert.Equivalent(expected, GetArgs(query));
    }
    
    [Fact]
    public void LoadEmptyFields()
    {
        HybridSearchQuery query = new();
        query.Load([]);
        object[] expected = [Index, "LOAD", 0];
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
    public void GroupBy_SingleField_WithReducer()
    {
        HybridSearchQuery query = new();
        query.GroupBy("field1", Reducers.Count());
        object[] expected = [Index, "GROUPBY", 1, "field1", "REDUCE", "COUNT", 0];
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
        query.GroupBy(["field1", "field2"], Reducers.Quantile("@field3", 0.5));
        object[] expected = [Index, "GROUPBY", 2, "field1", "field2", "REDUCE", "QUANTILE", 2, "@field3", 0.5];
        Assert.Equivalent(expected, GetArgs(query));
    }

    [Fact]
    public void Apply()
    {
        HybridSearchQuery query = new();
        query.Apply("@field1 + @field2", "sum");
        object[] expected = [Index, "APPLY", "@field1 + @field2", "AS", "sum"];
        Assert.Equivalent(expected, GetArgs(query));
    }
}