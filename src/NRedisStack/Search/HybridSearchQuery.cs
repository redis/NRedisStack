using NRedisStack.Search.Aggregation;

namespace NRedisStack.Search;

public sealed partial class HybridSearchQuery
{
    private string? _query;
    QueryConfig? _queryConfig;

    /// <summary>
    /// Specify the textual search portion of the query.
    /// </summary>
    public HybridSearchQuery Search(string query, QueryConfig? config = null)
    {
        _query = query;
        _queryConfig = config;
        return this;
    }

    private string? _vectorField;
    private ReadOnlyMemory<byte> _vectorData;
    private VectorSearchConfig? _vectorConfig;

    /// <summary>
    /// Specify the vector search portion of the query.
    /// </summary>
    public HybridSearchQuery VectorSimilaritySearch(string field, ReadOnlyMemory<byte> data, VectorSearchConfig? config = null)
    {
        _vectorField = field;
        _vectorData = data;
        _vectorConfig = config;
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

    private string[]? _loadFields;

    /// <summary>
    /// Add the list of fields to return in the results.
    /// </summary>
    public HybridSearchQuery Load(params string[] fields)
    {
        _loadFields = fields;
        return this;
    }

    private int _rerankTop;
    private string? _rerankExpression;

    /// <summary>
    /// Specify the re-rank configuration.
    /// </summary>
    public HybridSearchQuery ReRank(int top, string expression)
    {
        _rerankTop = top;
        _rerankExpression = expression;
        return this;
    }

    private object? _groupByFieldOrFields;
    private Reducer? _groupByReducer;

    /// <summary>
    /// Perform a group by operation on the results.
    /// </summary>
    public HybridSearchQuery GroupBy(string field, Reducer? reducer = null)
    {
        _groupByFieldOrFields = field;
        _groupByReducer = reducer;
        return this;
    }

    /// <summary>
    /// Perform a group by operation on the results.
    /// </summary>
    public HybridSearchQuery GroupBy(string[] fields, Reducer? reducer = null)
    {
        _groupByFieldOrFields = fields;
        _groupByReducer = reducer;
        return this;
    }

    private string? _applyExpression, _applyAlias;

    /// <summary>
    /// Apply a field transformation expression to the results.
    /// </summary>
    public HybridSearchQuery Apply(string expression, string alias)
    {
        _applyExpression = expression;
        _applyAlias = alias;
        return this;
    }

    private object? _sortByFieldOrFields;

    /// <summary>
    /// Sort the final results by the specified fields.
    /// </summary>
    public HybridSearchQuery SortBy(params SortedField[] fields)
    {
        _sortByFieldOrFields = fields;
        return this;
    }
    
    /// <summary>
    /// Sort the final results by the specified fields.
    /// </summary>
    public HybridSearchQuery SortBy(params string[] fields)
    {
        _sortByFieldOrFields = fields;
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

    private string? _language;
    /// <summary>
    /// The language to use for search queries. 
    /// </summary>
    public HybridSearchQuery Language(string language)
    {
        _language = language;
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
    public HybridSearchQuery WithCursor(int count = 0, TimeSpan maxIdle = default)
    {
        _cursorCount = count;
        _cursorMaxIdle = maxIdle;
        return this;
    }
}