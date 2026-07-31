using NRedisStack.Search.Literals;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search;
using StackExchange.Redis;
namespace NRedisStack;

public static class SearchCommandBuilder
{
    public static SerializedCommand _List()
    {
        return new(CommandCategories.ReadOnly, FT._LIST);
    }

    public static SerializedCommand Aggregate(string index, AggregationRequest query)
    {
        List<object> args = [index];
        query.SerializeRedisArgs();
        args.AddRange(query.GetArgs());
        return new(AggregateCategory(query), FT.AGGREGATE, args);
    }

    /// <summary>
    /// A plain aggregate is a pure read, but WITHCURSOR allocates cursor state on the serving node, so
    /// every replay leaks another cursor (held until its idle timeout) and returns an id the caller's
    /// follow-up FT.CURSOR calls will not be using.
    /// </summary>
    /// <remarks>
    /// Never, not merely a high rung. ServerSpecific is not enough, because it only strips failover and a
    /// same-server replay orphans a cursor just as effectively. Nor is WriteAccumulating: that relies on
    /// the policy's default ceiling, and a caller may legitimately raise MaxCommandRetryCategory - which
    /// says "I accept double-applied writes", not "I accept leaked cursors". Never is the only category
    /// no policy can override, since the ceiling itself may be set as high as Never.
    /// </remarks>
    private static CommandFlags AggregateCategory(AggregationRequest query)
        => query.IsWithCursor() ? CommandCategories.Never : CommandCategories.ReadOnly;

    public static SerializedCommand AliasAdd(string alias, string index)
    {
        return new(CommandCategories.WriteAccumulating, FT.ALIASADD, alias, index);
    }

    public static SerializedCommand AliasDel(string alias)
    {
        return new(CommandCategories.WriteAccumulating, FT.ALIASDEL, alias);
    }

    public static SerializedCommand AliasUpdate(string alias, string index)
    {
        return new(CommandCategories.WriteLastWins, FT.ALIASUPDATE, alias, index);
    }

    public static SerializedCommand AliasList(string index)
    {
        return new(CommandCategories.ReadOnly, FT.ALIASLIST, index);
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
        return new(CommandCategories.WriteAccumulating, FT.ALTER, args);
    }

    [Obsolete("Starting from Redis 8.0, use db.ConfigGet instead")]
    public static SerializedCommand ConfigGet(string option)
    {
        // matches how SE.Redis categorizes plain CONFIG: node-local, and GET is not meaningfully safer,
        // since a different member can be configured differently
        return new(CommandCategories.ServerAdmin | CommandCategories.ServerSpecific, FT.CONFIG, "GET", option);
    }

    [Obsolete("Starting from Redis 8.0, use db.ConfigSet instead")]
    public static SerializedCommand ConfigSet(string option, string value)
    {
        return new(CommandCategories.ServerAdmin | CommandCategories.ServerSpecific, FT.CONFIG, "SET", option, value);
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

        return new(CommandCategories.WriteAccumulating, FT.CREATE, args);
    }

    public static SerializedCommand CursorDel(string indexName, long cursorId)
    {
        // RediSearch errors ("Cursor does not exist") rather than treating a repeat delete as an OK no-op,
        // so replaying a delete whose reply was merely lost reports a failure for cleanup that actually
        // succeeded. Never rather than a high rung, since the policy ceiling is caller-settable and
        // forgoing retry costs little here - an undeleted cursor expires by idle timeout anyway.
        return new(CommandCategories.Never, FT.CURSOR, "DEL", indexName, cursorId);
    }

    public static SerializedCommand CursorRead(string indexName, long cursorId, int? count = null)
    {
        // Reading advances the cursor, so a replay silently skips a page rather than re-reading one - i.e.
        // data loss, not just a wasted round trip. Never for the same reason as CursorDel: no lower rung
        // survives a caller-raised MaxCommandRetryCategory.
        const CommandFlags Category = CommandCategories.Never;
        return ((count == null) ? new(Category, FT.CURSOR, "READ", indexName, cursorId)
            : new SerializedCommand(Category, FT.CURSOR, "READ", indexName, cursorId, "COUNT", count));
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

        return new(CommandCategories.WriteLastWins, FT.DICTADD, args);
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

        return new(CommandCategories.WriteLastWins, FT.DICTDEL, args);
    }

    public static SerializedCommand DictDump(string dict)
    {
        return new(CommandCategories.ReadOnly, FT.DICTDUMP, dict);
    }

    public static SerializedCommand DropIndex(string indexName, bool dd = false)
    {
        return ((dd) ? new(CommandCategories.WriteAccumulating, FT.DROPINDEX, indexName, "DD")
            : new SerializedCommand(CommandCategories.WriteAccumulating, FT.DROPINDEX, indexName));
    }

    public static SerializedCommand Explain(string indexName, string query, int? dialect)
    {
        var args = new List<object> { indexName, query };
        if (dialect != null)
        {
            args.Add("DIALECT");
            args.Add(dialect);
        }
        return new(CommandCategories.ReadOnly, FT.EXPLAIN, args);
    }

    public static SerializedCommand ExplainCli(string indexName, string query, int? dialect)
    {
        var args = new List<object> { indexName, query };
        if (dialect != null)
        {
            args.Add("DIALECT");
            args.Add(dialect);
        }
        return new(CommandCategories.ReadOnly, FT.EXPLAINCLI, args);
    }

    public static SerializedCommand Info(RedisValue index) => new(CommandCategories.ReadOnly, FT.INFO, index);

    public static SerializedCommand Search(string indexName, Query q)
    {
        var args = new List<object> { indexName };
        q.SerializeRedisArgs(args);

        return new(CommandCategories.ReadOnly, FT.SEARCH, args);
    }

    public static SerializedCommand ProfileSearch(string IndexName, Query q, bool limited = false)
    {
        var args =
            (limited)
                ? new() { IndexName, SearchArgs.SEARCH, SearchArgs.LIMITED, SearchArgs.QUERY }
                : new List<object>() { IndexName, SearchArgs.SEARCH, SearchArgs.QUERY };

        q.SerializeRedisArgs(args);
        return new(CommandCategories.ReadOnly, FT.PROFILE, args);
    }

    public static SerializedCommand ProfileAggregate(string IndexName, AggregationRequest query, bool limited = false)
    {
        var args = (limited)
            ? new() { IndexName, SearchArgs.AGGREGATE, SearchArgs.LIMITED, SearchArgs.QUERY }
            : new List<object> { IndexName, SearchArgs.AGGREGATE, SearchArgs.QUERY };

        query.SerializeRedisArgs();
        args.AddRange(query.GetArgs());
        // profiling a cursored aggregate still allocates the cursor, so it carries the same category
        return new(AggregateCategory(query), FT.PROFILE, args);
    }

    public static SerializedCommand SpellCheck(string indexName, string query, FTSpellCheckParams? spellCheckParams = null)
    {
        if (spellCheckParams != null)
        {
            spellCheckParams.SerializeRedisArgs();
            var args = new List<object>(spellCheckParams.GetArgs().Count + 2) { indexName, query }; // TODO: check if this improves performance (create a list with exact size)
            args.AddRange(spellCheckParams.GetArgs());
            return new(CommandCategories.ReadOnly, FT.SPELLCHECK, args);
        }

        return new(CommandCategories.ReadOnly, FT.SPELLCHECK, indexName, query);
    }

    public static SerializedCommand SugAdd(string key, string str, double score, bool increment = false, string? payload = null)
    {
        var args = new List<object> { (RedisKey)key, str, score };
        if (increment) { args.Add(SearchArgs.INCR); }
        if (payload != null) { args.Add(SearchArgs.PAYLOAD); args.Add(payload); }
        // INCR adds to the existing score, so a replay inflates it; otherwise the score is just set
        return new(increment ? CommandCategories.WriteAccumulating : CommandCategories.WriteLastWins, FT.SUGADD, args);
    }

    public static SerializedCommand SugDel(string key, string str)
    {
        return new(CommandCategories.WriteLastWins, FT.SUGDEL, (RedisKey)key, str);
    }

    public static SerializedCommand SugGet(string key, string prefix, bool fuzzy = false, bool withScores = false, bool withPayloads = false, int? max = null)
    {
        var args = new List<object> { (RedisKey)key, prefix };
        if (fuzzy) { args.Add(SearchArgs.FUZZY); }
        if (withScores) { args.Add(SearchArgs.WITHSCORES); }
        if (withPayloads) { args.Add(SearchArgs.WITHPAYLOADS); }
        if (max != null) { args.Add(SearchArgs.MAX); args.Add(max); }
        return new(CommandCategories.ReadOnly, FT.SUGGET, args);
    }

    public static SerializedCommand SugLen(string key)
    {
        return new(CommandCategories.ReadOnly, FT.SUGLEN, (RedisKey)key);
    }

    public static SerializedCommand SynDump(string indexName)
    {
        return new(CommandCategories.ReadOnly, FT.SYNDUMP, indexName);
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
        return new(CommandCategories.WriteLastWins, FT.SYNUPDATE, args);
    }

    public static SerializedCommand TagVals(string indexName, string fieldName) => //TODO: consider return Set
        new(CommandCategories.ReadOnly, FT.TAGVALS, indexName, fieldName);
}