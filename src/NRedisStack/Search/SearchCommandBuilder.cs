using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using NRedisStack.Search.Literals;
using StackExchange.Redis;
namespace NRedisStack;

public static class SearchCommandBuilder
{
    public static SerializedCommand _List()
    {
        return new(FT._LIST);
    }

    public static SerializedCommand Aggregate(string index, AggregationRequest query)
    {
        List<object> args = [index];
        query.SerializeRedisArgs();
        args.AddRange(query.GetArgs());
        return new(FT.AGGREGATE, args);
    }

    public static SerializedCommand AliasAdd(string alias, string index)
    {
        return new(FT.ALIASADD, alias, index);
    }

    public static SerializedCommand AliasDel(string alias)
    {
        return new(FT.ALIASDEL, alias);
    }

    public static SerializedCommand AliasUpdate(string alias, string index)
    {
        return new(FT.ALIASUPDATE, alias, index);
    }
    public static SerializedCommand Alter(string index, Schema schema, bool skipInitialScan = false)
    {
        List<object> args = [index];
        if (skipInitialScan) args.Add("SKIPINITIALSCAN");
        args.Add("SCHEMA");
        args.Add("ADD");
        foreach (var f in schema.Fields)
        {
            f.AddSchemaArgs(args);
        }
        return new(FT.ALTER, args);
    }

    [Obsolete("Starting from Redis 8.0, use db.ConfigGet instead")]
    public static SerializedCommand ConfigGet(string option)
    {
        return new(FT.CONFIG, "GET", option);
    }

    [Obsolete("Starting from Redis 8.0, use db.ConfigSet instead")]
    public static SerializedCommand ConfigSet(string option, string value)
    {
        return new(FT.CONFIG, "SET", option, value);
    }

    public static SerializedCommand Create(string indexName, FTCreateParams parameters, Schema schema)
    {
        var args = new List<object>() { indexName };
        parameters.AddParams(args); // TODO: Think of a better implementation

        args.Add("SCHEMA");

        foreach (var f in schema.Fields)
        {
            f.AddSchemaArgs(args);
        }

        return new(FT.CREATE, args);
    }

    public static SerializedCommand CursorDel(string indexName, long cursorId)
    {
        return new(FT.CURSOR, "DEL", indexName, cursorId);
    }

    public static SerializedCommand CursorRead(string indexName, long cursorId, int? count = null)
    {
        return ((count == null) ? new(FT.CURSOR, "READ", indexName, cursorId)
            : new SerializedCommand(FT.CURSOR, "READ", indexName, cursorId, "COUNT", count));
    }

    public static SerializedCommand DictAdd(string dict, params string[] terms)
    {
        if (terms.Length < 1)
        {
            throw new ArgumentOutOfRangeException("At least one term must be provided");
        }

        var args = new List<object>(terms.Length + 1) { dict };
        foreach (var t in terms)
        {
            args.Add(t);
        }

        return new(FT.DICTADD, args);
    }

    public static SerializedCommand DictDel(string dict, params string[] terms)
    {
        if (terms.Length < 1)
        {
            throw new ArgumentOutOfRangeException("At least one term must be provided");
        }

        var args = new List<object>(terms.Length + 1) { dict };
        foreach (var t in terms)
        {
            args.Add(t);
        }

        return new(FT.DICTDEL, args);
    }

    public static SerializedCommand DictDump(string dict)
    {
        return new(FT.DICTDUMP, dict);
    }

    public static SerializedCommand DropIndex(string indexName, bool dd = false)
    {
        return ((dd) ? new(FT.DROPINDEX, indexName, "DD")
            : new SerializedCommand(FT.DROPINDEX, indexName));
    }

    public static SerializedCommand Explain(string indexName, string query, int? dialect)
    {
        var args = new List<object> { indexName, query };
        if (dialect != null)
        {
            args.Add("DIALECT");
            args.Add(dialect);
        }
        return new(FT.EXPLAIN, args);
    }

    public static SerializedCommand ExplainCli(string indexName, string query, int? dialect)
    {
        var args = new List<object> { indexName, query };
        if (dialect != null)
        {
            args.Add("DIALECT");
            args.Add(dialect);
        }
        return new(FT.EXPLAINCLI, args);
    }

    public static SerializedCommand Info(RedisValue index) => new(FT.INFO, index);

    public static SerializedCommand Search(string indexName, Query q)
    {
        var args = new List<object> { indexName };
        q.SerializeRedisArgs(args);

        return new(FT.SEARCH, args);
    }

    public static SerializedCommand ProfileSearch(string IndexName, Query q, bool limited = false)
    {
        var args =
            (limited)
                ? new() { IndexName, SearchArgs.SEARCH, SearchArgs.LIMITED, SearchArgs.QUERY }
                : new List<object>() { IndexName, SearchArgs.SEARCH, SearchArgs.QUERY };

        q.SerializeRedisArgs(args);
        return new(FT.PROFILE, args);
    }

    public static SerializedCommand ProfileAggregate(string IndexName, AggregationRequest query, bool limited = false)
    {
        var args = (limited)
            ? new() { IndexName, SearchArgs.AGGREGATE, SearchArgs.LIMITED, SearchArgs.QUERY }
            : new List<object> { IndexName, SearchArgs.AGGREGATE, SearchArgs.QUERY };

        query.SerializeRedisArgs();
        args.AddRange(query.GetArgs());
        return new(FT.PROFILE, args);
    }

    public static SerializedCommand SpellCheck(string indexName, string query, FTSpellCheckParams? spellCheckParams = null)
    {
        if (spellCheckParams != null)
        {
            spellCheckParams.SerializeRedisArgs();
            var args = new List<object>(spellCheckParams.GetArgs().Count + 2) { indexName, query }; // TODO: check if this improves performance (create a list with exact size)
            args.AddRange(spellCheckParams.GetArgs());
            return new(FT.SPELLCHECK, args);
        }

        return new(FT.SPELLCHECK, indexName, query);
    }

    public static SerializedCommand SugAdd(string key, string str, double score, bool increment = false, string? payload = null)
    {
        var args = new List<object> { key, str, score };
        if (increment) { args.Add(SearchArgs.INCR); }
        if (payload != null) { args.Add(SearchArgs.PAYLOAD); args.Add(payload); }
        return new(FT.SUGADD, args);
    }

    public static SerializedCommand SugDel(string key, string str)
    {
        return new(FT.SUGDEL, key, str);
    }

    public static SerializedCommand SugGet(string key, string prefix, bool fuzzy = false, bool withScores = false, bool withPayloads = false, int? max = null)
    {
        var args = new List<object> { key, prefix };
        if (fuzzy) { args.Add(SearchArgs.FUZZY); }
        if (withScores) { args.Add(SearchArgs.WITHSCORES); }
        if (withPayloads) { args.Add(SearchArgs.WITHPAYLOADS); }
        if (max != null) { args.Add(SearchArgs.MAX); args.Add(max); }
        return new(FT.SUGGET, args);
    }

    public static SerializedCommand SugLen(string key)
    {
        return new(FT.SUGLEN, key);
    }

    public static SerializedCommand SynDump(string indexName)
    {
        return new(FT.SYNDUMP, indexName);
    }

    public static SerializedCommand SynUpdate(string indexName, string synonymGroupId, bool skipInitialScan = false, params string[] terms)
    {
        if (terms.Length < 1)
        {
            throw new ArgumentOutOfRangeException("terms must have at least one element");
        }
        var args = new List<object> { indexName, synonymGroupId };
        if (skipInitialScan) { args.Add(SearchArgs.SKIPINITIALSCAN); }
        args.AddRange(terms);
        return new(FT.SYNUPDATE, args);
    }

    public static SerializedCommand TagVals(string indexName, string fieldName) => //TODO: consider return Set
        new(FT.TAGVALS, indexName, fieldName);
}