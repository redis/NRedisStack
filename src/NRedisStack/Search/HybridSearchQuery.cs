using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using NRedisStack.Search.Aggregation;

namespace NRedisStack.Search;

/// <summary>
/// Represents a hybrid search (FT.HYBRID) operation. Note that <see cref="HybridSearchQuery"/> instances can be reused for
/// common queries, by passing the search operands as named parameters.
/// </summary>
[Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
public sealed partial class HybridSearchQuery
{
    /// <summary>
    /// Well-known fields for use with <see cref="ReturnFields(string[])"/>
    /// </summary>
    public static class Fields
    {
        // ReSharper disable InconsistentNaming

        /// <summary>
        /// The key of the indexed item in the database.
        /// </summary>
        public const string Key = "@__key";

        /// <summary>
        /// The score from the query. 
        /// </summary>
        public const string Score = "@__score";

        // ReSharper restore InconsistentNaming
    }
    private bool _frozen;
    private SearchConfig _search;
    private VectorSearchConfig _vsim;

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private HybridSearchQuery ThrowIfFrozen() // GetArgs freezes
    {
        if (_frozen) Throw();
        return this;

        [MethodImpl(MethodImplOptions.NoInlining)]
        static void Throw() => throw new InvalidOperationException(
            "By default, the query cannot be mutated after being issued (to allow safe parameterized reuse from concurrent callers). If you are using the query sequentially rather than concurrently, you can use " + nameof(AllowModification) + " to re-enable changes.");
    }
    /// <summary>
    /// Specify the textual search portion of the query.
    /// For a parameterized query, a search like <c>"$key"</c> will search using the parameter named <c>key</c>.
    /// </summary>
    public HybridSearchQuery Search(SearchConfig query)
    {
        ThrowIfFrozen();
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
        ThrowIfFrozen();
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
        ThrowIfFrozen();
        _combiner = combiner;
        _combineScoreAlias = scoreAlias;
        return this;
    }

    private object? _loadFieldOrFields;

    /// <summary>
    /// Add the list of fields to return in the results. Well-known fields are available via <see cref="Fields"/>.
    /// </summary>
    public HybridSearchQuery ReturnFields(params string[] fields) // naming for consistency with SearchQuery
    {
        ThrowIfFrozen();
        _loadFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Add the list of fields to return in the results. Well-known fields are available via <see cref="Fields"/>.
    /// </summary>
    public HybridSearchQuery ReturnFields(string field) // naming for consistency with SearchQuery
    {
        ThrowIfFrozen();
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
        ThrowIfFrozen();
        _groupByFieldOrFields = field;
        return this;
    }

    /// <summary>
    /// Perform a group by operation on the results.
    /// </summary>
    public HybridSearchQuery GroupBy(params string[] fields)
    {
        ThrowIfFrozen();
        _groupByFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Perform a reduce operation on the results, after grouping.
    /// </summary>
    public HybridSearchQuery Reduce(Reducer reducer)
    {
        ThrowIfFrozen();
        _reducerOrReducers = reducer;
        return this;
    }

    /// <summary>
    /// Perform a reduce operation on the results, after grouping.
    /// </summary>
    public HybridSearchQuery Reduce(params Reducer[] reducers)
    {
        ThrowIfFrozen();
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
        ThrowIfFrozen();
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
        ThrowIfFrozen();
        _applyExpressionOrExpressions = NullIfEmpty(applyExpression);
        return this;
    }

    private object? _sortByFieldOrFields;

    /// <summary>
    /// Sort the final results by the specified fields.
    /// </summary>
    /// <remarks>The default sort order is by score, unless overridden or disabled.</remarks>
    public HybridSearchQuery SortBy(params SortedField[] fields)
    {
        ThrowIfFrozen();
        _sortByFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Do not sort the final results. This disables the default sort by score.
    /// </summary>
    public HybridSearchQuery NoSort()
    {
        ThrowIfFrozen();
        _sortByFieldOrFields = s_NoSortSentinel;
        return this;
    }

    private static readonly object s_NoSortSentinel = new();

    /// <summary>
    /// Sort the final results by the specified fields.
    /// </summary>
    public HybridSearchQuery SortBy(params string[] fields)
    {
        ThrowIfFrozen();
        _sortByFieldOrFields = NullIfEmpty(fields);
        return this;
    }

    /// <summary>
    /// Sort the final results by the specified field.
    /// </summary>
    public HybridSearchQuery SortBy(SortedField field)
    {
        ThrowIfFrozen();
        _sortByFieldOrFields = field;
        return this;
    }

    /// <summary>
    /// Sort the final results by the specified field.
    /// </summary>
    public HybridSearchQuery SortBy(string field)
    {
        ThrowIfFrozen();
        _sortByFieldOrFields = field;
        return this;
    }

    private string? _filter;

    /// <summary>
    /// Final result filtering
    /// </summary>
    public HybridSearchQuery Filter(string expression)
    {
        ThrowIfFrozen();
        _filter = expression;
        return this;
    }

    private int _pagingOffset = -1, _pagingCount = -1;

    public HybridSearchQuery Limit(int offset, int count)
    {
        ThrowIfFrozen();
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
        ThrowIfFrozen();
        _explainScore = explainScore;
        return this;
    }

    private bool _timeout;

    /// <summary>
    /// Apply the global timeout setting.
    /// </summary>
    public HybridSearchQuery Timeout(bool timeout = true)
    {
        ThrowIfFrozen();
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
        ThrowIfFrozen();
        // not currently exposed, while I figure out the API
        _cursorCount = count;
        _cursorMaxIdle = maxIdle;
        return this;
    }

    /// <summary>
    /// By default, queries are frozen when issued, to allow safe re-use of prepared queries from different callers.
    /// If you instead want to make sequential use of a query in a <i>single</i> caller, you can use this method
    /// to re-enable modification after issuing each query.
    /// </summary>
    /// <returns></returns>
    public HybridSearchQuery AllowModification()
    {
        _frozen = false;
        return this;
    }
}