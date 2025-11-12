using System.ComponentModel;
using System.Diagnostics.CodeAnalysis;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.DataTypes;
using StackExchange.Redis;
namespace NRedisStack;

public class SearchCommandsAsync : ISearchCommandsAsync
{
    private readonly IDatabaseAsync _db;
    protected int? defaultDialect;

    public SearchCommandsAsync(IDatabaseAsync db, int? defaultDialect = 2)
    {
        _db = db;
        SetDefaultDialect(defaultDialect);
    }

    internal void SetDefaultDialectIfUnset(IDialectAwareParam? param)
    {
        if (param != null && param.Dialect == null && defaultDialect != null)
        {
            param.Dialect = defaultDialect;
        }
    }

    internal int? CheckAndGetDefaultDialect(int? dialect) =>
        (dialect == null && defaultDialect != null) ? defaultDialect : dialect;

    public void SetDefaultDialect(int? defaultDialect)
    {
        if (defaultDialect == 0)
        {
            throw new ArgumentOutOfRangeException("DIALECT=0 cannot be set.");
        }
        this.defaultDialect = defaultDialect;
    }

    /// <inheritdoc/>
    public async Task<RedisResult[]> _ListAsync()
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder._List())).ToArray();
    }

    internal static IServer? GetRandomServerForCluster(IDatabaseAsync db, out int? database)
    {
        var server = db.Multiplexer.GetServer(key: default(RedisKey));
        // ReSharper disable once ConditionIsAlwaysTrueOrFalseAccordingToNullableAPIContract
        if (server is null || server.ServerType != ServerType.Cluster)
        {
            database = null;
            return null;
        }
        // This is vexingly misplaced, but: it doesn't actually matter for cluster
        database = db is IDatabase nonAsync ? nonAsync.Database : null;
        return server;
    }

    /// <inheritdoc/>
    public async Task<AggregationResult> AggregateAsync(string index, AggregationRequest query)
    {
        SetDefaultDialectIfUnset(query);
        IServer? server = null;
        int? database = null;

        var command = SearchCommandBuilder.Aggregate(index, query);
        if (query.IsWithCursor())
        {
            // we can issue this anywhere, but follow-up calls need to be on the same server
            server = GetRandomServerForCluster(_db, out database);
        }

        RedisResult result;
        if (server is not null)
        {
            result = await server.ExecuteAsync(database, command);
        }
        else
        {
            result = await _db.ExecuteAsync(command);
        }

        return result.ToAggregationResult(index, query, server, database);
    }

    public async IAsyncEnumerable<Row> AggregateAsyncEnumerable(string index, AggregationRequest query)
    {
        if (!query.IsWithCursor()) query.Cursor();

        var result = await AggregateAsync(index, query);
        try
        {
            while (true)
            {
                var count = checked((int)result.TotalResults);
                for (int i = 0; i < count; i++)
                {
                    yield return result.GetRow(i);
                }
                if (result.CursorId == 0) break;
                result = await CursorReadAsync(result, query.Count);
            }
        }
        finally
        {
            if (result.CursorId != 0)
            {
                await CursorDelAsync(result);
            }
        }
    }

    /// <inheritdoc/>
    public async Task<bool> AliasAddAsync(string alias, string index)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.AliasAdd(alias, index))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> AliasDelAsync(string alias)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.AliasDel(alias))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> AliasUpdateAsync(string alias, string index)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.AliasUpdate(alias, index))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> AlterAsync(string index, Schema schema, bool skipInitialScan = false)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.Alter(index, schema, skipInitialScan))).OKtoBoolean();
    }

    /// <inheritdoc/>
    [Obsolete("Starting from Redis 8.0, use db.ConfigGetAsync instead")]
    public async Task<Dictionary<string, string>> ConfigGetAsync(string option)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.ConfigGet(option))).ToConfigDictionary();
    }

    /// <inheritdoc/>
    [Obsolete("Starting from Redis 8.0, use db.ConfigSetAsync instead")]
    public async Task<bool> ConfigSetAsync(string option, string value)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.ConfigSet(option, value))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> CreateAsync(string indexName, FTCreateParams parameters, Schema schema)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.Create(indexName, parameters, schema))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<bool> CreateAsync(string indexName, Schema schema)
    {
        return (await CreateAsync(indexName, new(), schema));
    }

    /// <inheritdoc/>
    [Obsolete("When possible, use CursorDelAsync(AggregationResult, int?) instead. This legacy API will not work correctly on CLUSTER environments, but will continue to work for single-node deployments.")]
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public async Task<bool> CursorDelAsync(string indexName, long cursorId)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.CursorDel(indexName, cursorId))).OKtoBoolean();
    }

    public async Task<bool> CursorDelAsync(AggregationResult result)
    {
        if (result is not AggregationResult.WithCursorAggregationResult withCursor)
        {
            throw new ArgumentException(
                message: $"{nameof(CursorDelAsync)} must be called with a value returned from a previous call to {nameof(AggregateAsync)} with a cursor.",
                paramName: nameof(result));
        }

        var command = SearchCommandBuilder.CursorDel(withCursor.IndexName, withCursor.CursorId);
        var pending = withCursor.Server is { } server
            ? server.ExecuteAsync(withCursor.Database, command)
            : _db.ExecuteAsync(command);
        return (await pending).OKtoBoolean();
    }

    /// <inheritdoc/>
    [Obsolete("When possible, use AggregateAsyncEnumerable or CursorReadAsync(AggregationResult, int?) instead. This legacy API will not work correctly on CLUSTER environments, but will continue to work for single-node deployments.")]
    [Browsable(false), EditorBrowsable(EditorBrowsableState.Never)]
    public async Task<AggregationResult> CursorReadAsync(string indexName, long cursorId, int? count = null)
    {
        var resp = (await _db.ExecuteAsync(SearchCommandBuilder.CursorRead(indexName, cursorId, count))).ToArray();
        return new(resp[0], (long)resp[1]);
    }

    public async Task<AggregationResult> CursorReadAsync(AggregationResult result, int? count = null)
    {
        if (result is not AggregationResult.WithCursorAggregationResult withCursor)
        {
            throw new ArgumentException(message: $"{nameof(CursorReadAsync)} must be called with a value returned from a previous call to {nameof(AggregateAsync)} with a cursor.", paramName: nameof(result));
        }
        var command = SearchCommandBuilder.CursorRead(withCursor.IndexName, withCursor.CursorId, count);
        var pending = withCursor.Server is { } server
            ? server.ExecuteAsync(withCursor.Database, command)
            : _db.ExecuteAsync(command);
        var resp = (await pending).ToArray();
        return new AggregationResult.WithCursorAggregationResult(withCursor.IndexName, resp[0], (long)resp[1], withCursor.Server, withCursor.Database);
    }

    /// <inheritdoc/>
    public async Task<long> DictAddAsync(string dict, params string[] terms)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.DictAdd(dict, terms))).ToLong();
    }

    /// <inheritdoc/>
    public async Task<long> DictDelAsync(string dict, params string[] terms)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.DictDel(dict, terms))).ToLong();
    }

    /// <inheritdoc/>
    public async Task<RedisResult[]> DictDumpAsync(string dict)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.DictDump(dict))).ToArray();
    }

    /// <inheritdoc/>
    public async Task<bool> DropIndexAsync(string indexName, bool dd = false)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.DropIndex(indexName, dd))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<string> ExplainAsync(string indexName, string query, int? dialect = null)
    {
        dialect = CheckAndGetDefaultDialect(dialect);
        return (await _db.ExecuteAsync(SearchCommandBuilder.Explain(indexName, query, dialect))).ToString();
    }

    /// <inheritdoc/>
    public async Task<RedisResult[]> ExplainCliAsync(string indexName, string query, int? dialect = null)
    {
        dialect = CheckAndGetDefaultDialect(dialect);
        return (await _db.ExecuteAsync(SearchCommandBuilder.ExplainCli(indexName, query, dialect))).ToArray();
    }

    /// <inheritdoc/>
    public async Task<InfoResult> InfoAsync(RedisValue index) =>
        new(await _db.ExecuteAsync(SearchCommandBuilder.Info(index)));

    /// <inheritdoc/>
    [Obsolete("Consider using ProfileOnSearchAsync with Redis CE 8.0 and later")]
    public async Task<Tuple<SearchResult, Dictionary<string, RedisResult>>> ProfileSearchAsync(string indexName, Query q, bool limited = false)
    {
        SetDefaultDialectIfUnset(q);
        return (await _db.ExecuteAsync(SearchCommandBuilder.ProfileSearch(indexName, q, limited)))
            .ToProfileSearchResult(q);
    }

    /// <inheritdoc/>
    public async Task<Tuple<SearchResult, ProfilingInformation>> ProfileOnSearchAsync(string indexName, Query q, bool limited = false)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.ProfileSearch(indexName, q, limited)))
            .ParseProfileSearchResult(q);
    }
    /// <inheritdoc/>
    [Obsolete("Consider using ProfileOnSearchAsync with Redis CE 8.0 and later")]
    public async Task<Tuple<AggregationResult, Dictionary<string, RedisResult>>> ProfileAggregateAsync(string indexName, AggregationRequest query, bool limited = false)
    {
        SetDefaultDialectIfUnset(query);
        return (await _db.ExecuteAsync(SearchCommandBuilder.ProfileAggregate(indexName, query, limited)))
            .ToProfileAggregateResult(query);
    }
    /// <inheritdoc/>
    public async Task<Tuple<AggregationResult, ProfilingInformation>> ProfileOnAggregateAsync(string indexName, AggregationRequest query, bool limited = false)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.ProfileAggregate(indexName, query, limited)))
            .ParseProfileAggregateResult(query);
    }

    /// <inheritdoc/>
    public async Task<SearchResult> SearchAsync(string indexName, Query q)
    {
        SetDefaultDialectIfUnset(q);
        return (await _db.ExecuteAsync(SearchCommandBuilder.Search(indexName, q))).ToSearchResult(q);
    }

    /// <inheritdoc/>
    public async Task<Dictionary<string, Dictionary<string, double>>> SpellCheckAsync(string indexName, string query, FTSpellCheckParams? spellCheckParams = null)
    {
        SetDefaultDialectIfUnset(spellCheckParams);
        return (await _db.ExecuteAsync(SearchCommandBuilder.SpellCheck(indexName, query, spellCheckParams))).ToFtSpellCheckResult();
    }

    /// <inheritdoc/>
    public async Task<long> SugAddAsync(string key, string str, double score, bool increment = false, string? payload = null)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.SugAdd(key, str, score, increment, payload))).ToLong();
    }


    /// <inheritdoc/>
    public async Task<bool> SugDelAsync(string key, string str)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.SugDel(key, str))).ToString() == "1";

    }


    /// <inheritdoc/>
    public async Task<List<string>> SugGetAsync(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.SugGet(key, prefix, fuzzy, withScores: false, withPayloads, max))).ToStringList();

    }

    /// <inheritdoc/>
    public async Task<List<Tuple<string, double>>> SugGetWithScoresAsync(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.SugGet(key, prefix, fuzzy, withScores: true, withPayloads, max))).ToStringDoubleTupleList();
    }


    /// <inheritdoc/>
    public async Task<long> SugLenAsync(string key)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.SugLen(key))).ToLong();

    }

    /// <inheritdoc/>
    public async Task<Dictionary<string, List<string>>> SynDumpAsync(string indexName)
    {
        var resp = (await _db.ExecuteAsync(SearchCommandBuilder.SynDump(indexName))).ToArray();
        var result = new Dictionary<string, List<string>>();
        for (int i = 0; i < resp.Length; i += 2)
        {
            var term = resp[i].ToString();
            var synonyms = (resp[i + 1]).ToArray().Select(x => x.ToString()).ToList(); // TODO: consider leave synonyms as RedisValue[]
            result.Add(term, synonyms);
        }
        return result;
    }

    /// <inheritdoc/>
    public async Task<bool> SynUpdateAsync(string indexName, string synonymGroupId, bool skipInitialScan = false, params string[] terms)
    {
        return (await _db.ExecuteAsync(SearchCommandBuilder.SynUpdate(indexName, synonymGroupId, skipInitialScan, terms))).OKtoBoolean();
    }

    /// <inheritdoc/>
    public async Task<RedisResult[]> TagValsAsync(string indexName, string fieldName) => //TODO: consider return Set
        (await _db.ExecuteAsync(SearchCommandBuilder.TagVals(indexName, fieldName))).ToArray();

    /// <inheritdoc/>
    [Experimental(Experiments.Server_8_4, UrlFormat = Experiments.UrlFormat)]
    public async Task<HybridSearchResult> HybridSearchAsync(string indexName, HybridSearchQuery query)
    {
        query.Validate();
        var args = query.GetArgs(indexName);
        return HybridSearchResult.Parse(await _db.ExecuteAsync(query.Command, args));
    }
}