using NRedisStack.Search;
using NRedisStack.Search.DataTypes;
using StackExchange.Redis;
namespace NRedisStack;

public class SearchCommands(IDatabase db, int? defaultDialect = 2)
    : SearchCommandsAsync(db, defaultDialect), ISearchCommands
{
    /// <inheritdoc/>
    public RedisResult[] _List()
    {
        return db.Execute(SearchCommandBuilder._List()).ToArray();
    }

    /// <inheritdoc/>
    public AggregationResult Aggregate(string index, AggregationRequest query)
    {
        SetDefaultDialectIfUnset(query);
        var result = db.Execute(SearchCommandBuilder.Aggregate(index, query));
        return result.ToAggregationResult(query);
    }

    /// <inheritdoc/>
    public bool AliasAdd(string alias, string index)
    {
        return db.Execute(SearchCommandBuilder.AliasAdd(alias, index)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool AliasDel(string alias)
    {
        return db.Execute(SearchCommandBuilder.AliasDel(alias)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool AliasUpdate(string alias, string index)
    {
        return db.Execute(SearchCommandBuilder.AliasUpdate(alias, index)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Alter(string index, Schema schema, bool skipInitialScan = false)
    {
        return db.Execute(SearchCommandBuilder.Alter(index, schema, skipInitialScan)).OKtoBoolean();
    }

    /// <inheritdoc/>
    [Obsolete("Starting from Redis 8.0, use db.ConfigGet instead")]
    public Dictionary<string, string> ConfigGet(string option)
    {
        return db.Execute(SearchCommandBuilder.ConfigGet(option)).ToConfigDictionary();
    }

    /// <inheritdoc/>
    [Obsolete("Starting from Redis 8.0, use db.ConfigSet instead")]
    public bool ConfigSet(string option, string value)
    {
        return db.Execute(SearchCommandBuilder.ConfigSet(option, value)).OKtoBoolean();
    }

    // TODO: Add an ability to add fildes like that: TextField.Of("name")
    /// <inheritdoc/>
    public bool Create(string indexName, FTCreateParams parameters, Schema schema)
    {
        return db.Execute(SearchCommandBuilder.Create(indexName, parameters, schema)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public bool Create(string indexName, Schema schema)
    {
        return Create(indexName, new(), schema);
    }

    /// <inheritdoc/>
    public bool CursorDel(string indexName, long cursorId)
    {
        return db.Execute(SearchCommandBuilder.CursorDel(indexName, cursorId)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public AggregationResult CursorRead(string indexName, long cursorId, int? count = null)
    {
        var resp = db.Execute(SearchCommandBuilder.CursorRead(indexName, cursorId, count)).ToArray();
        return new(resp[0], (long)resp[1]);
    }

    /// <inheritdoc/>
    public long DictAdd(string dict, params string[] terms)
    {
        return db.Execute(SearchCommandBuilder.DictAdd(dict, terms)).ToLong();
    }

    /// <inheritdoc/>
    public long DictDel(string dict, params string[] terms)
    {
        return db.Execute(SearchCommandBuilder.DictDel(dict, terms)).ToLong();
    }

    /// <inheritdoc/>
    public RedisResult[] DictDump(string dict)
    {
        return db.Execute(SearchCommandBuilder.DictDump(dict)).ToArray();
    }

    /// <inheritdoc/>
    public bool DropIndex(string indexName, bool dd = false)
    {
        return db.Execute(SearchCommandBuilder.DropIndex(indexName, dd)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public string Explain(string indexName, string query, int? dialect = null)
    {
        dialect = CheckAndGetDefaultDialect(dialect);
        return db.Execute(SearchCommandBuilder.Explain(indexName, query, dialect)).ToString();
    }

    /// <inheritdoc/>
    public RedisResult[] ExplainCli(string indexName, string query, int? dialect = null)
    {
        dialect = CheckAndGetDefaultDialect(dialect);
        return db.Execute(SearchCommandBuilder.ExplainCli(indexName, query, dialect)).ToArray();
    }

    /// <inheritdoc/>
    public InfoResult Info(RedisValue index) =>
        new(db.Execute(SearchCommandBuilder.Info(index)));

    /// <inheritdoc/>
    [Obsolete("Consider using ProfileOnSearch with Redis CE 8.0 and later")]
    public Tuple<SearchResult, Dictionary<string, RedisResult>> ProfileSearch(string indexName, Query q, bool limited = false)
    {
        SetDefaultDialectIfUnset(q);
        return db.Execute(SearchCommandBuilder.ProfileSearch(indexName, q, limited))
            .ToProfileSearchResult(q);
    }

    /// <inheritdoc/>
    public Tuple<SearchResult, ProfilingInformation> ProfileOnSearch(string indexName, Query q, bool limited = false)
    {
        return db.Execute(SearchCommandBuilder.ProfileSearch(indexName, q, limited))
            .ParseProfileSearchResult(q);
    }

    /// <inheritdoc/>
    [Obsolete("Consider using ProfileOnAggregate with Redis CE 8.0 and later")]
    public Tuple<AggregationResult, Dictionary<string, RedisResult>> ProfileAggregate(string indexName, AggregationRequest query, bool limited = false)
    {
        SetDefaultDialectIfUnset(query);
        return db.Execute(SearchCommandBuilder.ProfileAggregate(indexName, query, limited))
            .ToProfileAggregateResult(query);
    }

    /// <inheritdoc/>
    public Tuple<AggregationResult, ProfilingInformation> ProfileOnAggregate(string indexName, AggregationRequest query, bool limited = false)
    {
        SetDefaultDialectIfUnset(query);
        return db.Execute(SearchCommandBuilder.ProfileAggregate(indexName, query, limited))
            .ParseProfileAggregateResult(query);
    }

    /// <inheritdoc/>
    public SearchResult Search(string indexName, Query q)
    {
        SetDefaultDialectIfUnset(q);
        return db.Execute(SearchCommandBuilder.Search(indexName, q)).ToSearchResult(q);
    }

    /// <inheritdoc/>
    public Dictionary<string, Dictionary<string, double>> SpellCheck(string indexName, string query, FTSpellCheckParams? spellCheckParams = null)
    {
        SetDefaultDialectIfUnset(spellCheckParams);
        return db.Execute(SearchCommandBuilder.SpellCheck(indexName, query, spellCheckParams)).ToFtSpellCheckResult();
    }

    /// <inheritdoc/>
    public long SugAdd(string key, string str, double score, bool increment = false, string? payload = null)
    {
        return db.Execute(SearchCommandBuilder.SugAdd(key, str, score, increment, payload)).ToLong();
    }


    /// <inheritdoc/>
    public bool SugDel(string key, string str)
    {
        return db.Execute(SearchCommandBuilder.SugDel(key, str)).ToString() == "1";
    }


    /// <inheritdoc/>
    public List<string> SugGet(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null)
    {
        return db.Execute(SearchCommandBuilder.SugGet(key, prefix, fuzzy, false, withPayloads, max)).ToStringList();
    }

    /// <inheritdoc/>
    public List<Tuple<string, double>> SugGetWithScores(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null)
    {
        return db.Execute(SearchCommandBuilder.SugGet(key, prefix, fuzzy, true, withPayloads, max)).ToStringDoubleTupleList();
    }


    /// <inheritdoc/>
    public long SugLen(string key)
    {
        return db.Execute(SearchCommandBuilder.SugLen(key)).ToLong();
    }


    /// <inheritdoc/>
    public Dictionary<string, List<string>> SynDump(string indexName)
    {
        var resp = db.Execute(SearchCommandBuilder.SynDump(indexName)).ToArray();
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
    public bool SynUpdate(string indexName, string synonymGroupId, bool skipInitialScan = false, params string[] terms)
    {
        return db.Execute(SearchCommandBuilder.SynUpdate(indexName, synonymGroupId, skipInitialScan, terms)).OKtoBoolean();
    }

    /// <inheritdoc/>
    public RedisResult[] TagVals(string indexName, string fieldName) => //TODO: consider return Set
        db.Execute(SearchCommandBuilder.TagVals(indexName, fieldName)).ToArray();
}