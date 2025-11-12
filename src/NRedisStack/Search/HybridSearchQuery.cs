using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using NRedisStack.Search.Aggregation;

namespace NRedisStack.Search;

[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public sealed partial class HybridSearchQuery
{
    private SearchConfig _search;
    private VectorSearchConfig _vsim;

    /// <summary>
    /// Specify the textual search portion of the query.
    /// </summary>
    public HybridSearchQuery Search(SearchConfig query)
    {
        _search = query;
        return this;
    }

    /// <summary>
    /// Specify the vector search portion of the query.
    /// </summary>
    public HybridSearchQuery VectorSearch(string fieldName, VectorData vectorData)
        => VectorSearch(new VectorSearchConfig(fieldName, vectorData));

    /// <summary>
    /// Specify the vector search portion of the query.
    /// </summary>
    public HybridSearchQuery VectorSearch(VectorSearchConfig config)
    {
        _vsim = config;
        return this;
    }

    private Combiner? _combiner;
    private string? _combineScoreAlias;

    /// <summary>
    /// Configure the score fusion method (optional). If not provided, Reciprocal Rank Fusion (RRF) is used with server-side default parameters.
    /// </summary>
    public HybridSearchQuery Combine(Combiner combiner, string? scoreAlias = null)
    {
        _combiner = combiner;
        _combineScoreAlias = scoreAlias;
        return this;
    }

    private object? _loadFieldOrFields;

    /// <summary>
    /// Add the list of fields to return in the results.
    /// </summary>
    public HybridSearchQuery ReturnFields(params string[] fields) // naming for consistency with SearchQuery
    {
        _loadFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Add the list of fields to return in the results.
    /// </summary>
    public HybridSearchQuery ReturnFields(string field) // naming for consistency with SearchQuery
    {
        _loadFieldOrFields = field;
        return this;
    }

    private object? _groupByFieldOrFields;
    private object? _reducerOrReducers;

    /// <summary>
    /// Perform a group by operation on the results.
    /// </summary>
    public HybridSearchQuery GroupBy(string field)
    {
        _groupByFieldOrFields = field;
        return this;
    }

    /// <summary>
    /// Perform a group by operation on the results.
    /// </summary>
    public HybridSearchQuery GroupBy(params string[] fields)
    {
        _groupByFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Perform a reduce operation on the results, after grouping.
    /// </summary>
    public HybridSearchQuery Reduce(Reducer reducer)
    {
        _reducerOrReducers = reducer;
        return this;
    }

    /// <summary>
    /// Perform a reduce operation on the results, after grouping.
    /// </summary>
    public HybridSearchQuery Reduce(params Reducer[] reducers)
    {
        _reducerOrReducers = NullIfEmpty(reducers);
        return this;
    }

    private static T[]? NullIfEmpty<T>(T[]? array) => array?.Length > 0 ? array : null;

    private object? _applyExpressionOrExpressions;

    /// <summary>
    /// Apply a field transformation expression to the results.
    /// </summary>
    [OverloadResolutionPriority(1)] // allow Apply(new("expr", "alias")) to resolve correctly
    public HybridSearchQuery Apply(ApplyExpression applyExpression)
    {
        if (applyExpression.Alias is null)
        {
            _applyExpressionOrExpressions = applyExpression.Expression;
        }
        else
        {
            _applyExpressionOrExpressions = applyExpression; // pay for the box
        }

        return this;
    }

    /// <summary>
    /// Apply field transformation expressions to the results.
    /// </summary>
    public HybridSearchQuery Apply(params ApplyExpression[] applyExpression)
    {
        _applyExpressionOrExpressions = NullIfEmpty(applyExpression);
        return this;
    }

    private object? _sortByFieldOrFields;

    /// <summary>
    /// Sort the final results by the specified fields.
    /// </summary>
    public HybridSearchQuery SortBy(params SortedField[] fields)
    {
        _sortByFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Sort the final results by the specified fields.
    /// </summary>
    public HybridSearchQuery SortBy(params string[] fields)
    {
        _sortByFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Sort the final results by the specified field.
    /// </summary>
    public HybridSearchQuery SortBy(SortedField field)
    {
        _sortByFieldOrFields = field;
        return this;
    }

    /// <summary>
    /// Sort the final results by the specified field.
    /// </summary>
    public HybridSearchQuery SortBy(string field)
    {
        _sortByFieldOrFields = field;
        return this;
    }

    private string? _filter;

    /// <summary>
    /// Final result filtering
    /// </summary>
    public HybridSearchQuery Filter(string expression)
    {
        _filter = expression;
        return this;
    }

    private int _pagingOffset = -1, _pagingCount = -1;

    public HybridSearchQuery Limit(int offset, int count)
    {
        if (offset < 0) throw new ArgumentOutOfRangeException(nameof(offset));
        if (count < 0) throw new ArgumentOutOfRangeException(nameof(count));
        _pagingOffset = offset;
        _pagingCount = count;
        return this;
    }

    private bool _explainScore;

    /// <summary>
    /// Include score explanations
    /// </summary>
    public HybridSearchQuery ExplainScore(bool explainScore = true)
    {
        _explainScore = explainScore;
        return this;
    }

    private bool _timeout;

    /// <summary>
    /// Apply the global timeout setting.
    /// </summary>
    public HybridSearchQuery Timeout(bool timeout = true)
    {
        _timeout = timeout;
        return this;
    }

    private int _cursorCount = -1; // -1: no cursor; 0: default count
    private TimeSpan _cursorMaxIdle;

    /// <summary>
    /// Use a cursor for result iteration.
    /// </summary>
    internal HybridSearchQuery WithCursor(int count = 0, TimeSpan maxIdle = default)
    {
        // not currently exposed, while I figure out the API
        _cursorCount = count;
        _cursorMaxIdle = maxIdle;
        return this;
    }

    private IReadOnlyDictionary<string, object>? _parameters;

    /// <summary>
    /// Supply parameters for the query.
    /// </summary>
    public HybridSearchQuery Parameters(IReadOnlyDictionary<string, object> parameters)
    {
        _parameters = parameters is { Count: 0 } ? null : parameters; // ignore empty
        return this;
    }
}