using NRedisStack.Literals;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.DataTypes;
using NRedisStack.Search.FT.CREATE;
using StackExchange.Redis;
namespace NRedisStack
{
    public class SearchCommands : ISearchCommands
    {
        IDatabase _db;
        public SearchCommands(IDatabase db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public RedisResult[] _List()
        {
            return _db.Execute(FT._LIST).ToArray();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> _ListAsync()
        {
            return (await _db.ExecuteAsync(FT._LIST)).ToArray();
        }

        /// <inheritdoc/>
        public AggregationResult Aggregate(string index, AggregationRequest query)
        {
            List<object> args = new List<object> { index };
            //query.SerializeRedisArgs(args);
            foreach (var arg in query.GetArgs())
            {
                args.Add(arg.ToString());
            }
            var result = _db.Execute(FT.AGGREGATE, args);
            if (query.IsWithCursor())
            {
                var results = (RedisResult[])result;

                return new AggregationResult(results[0], (long)results[1]);
            }
            else
            {
                return new AggregationResult(result);
            }
        }

        /// <inheritdoc/>
        public async Task<AggregationResult> AggregateAsync(string index, AggregationRequest query)
        {
            List<object> args = new List<object> { index };
            //query.SerializeRedisArgs(args);
            foreach (var arg in query.GetArgs())
            {
                args.Add(arg);
            }
            var result = await _db.ExecuteAsync(FT.AGGREGATE, args);
            if (query.IsWithCursor())
            {
                var results = (RedisResult[])result;

                return new AggregationResult(results[0], (long)results[1]);
            }
            else
            {
                return new AggregationResult(result);
            }
        }

        /// <inheritdoc/>
        public bool AliasAdd(string alias, string index)
        {
            return _db.Execute(FT.ALIASADD, alias, index).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> AliasAddAsync(string alias, string index)
        {
            return (await _db.ExecuteAsync(FT.ALIASADD, alias, index)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool AliasDel(string alias)
        {
            return _db.Execute(FT.ALIASDEL, alias).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> AliasDelAsync(string alias)
        {
            return (await _db.ExecuteAsync(FT.ALIASDEL, alias)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool AliasUpdate(string alias, string index)
        {
            return _db.Execute(FT.ALIASUPDATE, alias, index).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> AliasUpdateAsync(string alias, string index)
        {
            return (await _db.ExecuteAsync(FT.ALIASUPDATE, alias, index)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool Alter(string index, Schema schema, bool skipInitialScan = false)
        {
            List<object> args = new List<object>() { index };
            if (skipInitialScan) args.Add("SKIPINITIALSCAN");
            args.Add("SCHEMA");
            args.Add("ADD");
            foreach (var f in schema.Fields)
            {
                f.AddSchemaArgs(args);
            }
            return _db.Execute(FT.ALTER, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> AlterAsync(string index, Schema schema, bool skipInitialScan = false)
        {
            List<object> args = new List<object>() { index };
            if (skipInitialScan) args.Add("SKIPINITIALSCAN");
            args.Add("SCHEMA");
            args.Add("ADD");
            foreach (var f in schema.Fields)
            {
                f.AddSchemaArgs(args);
            }
            return (await _db.ExecuteAsync(FT.ALTER, args)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public Dictionary<string, string> ConfigGet(string option)
        {
            var result = _db.Execute(FT.CONFIG, "GET", option);
            return result.ToConfigDictionary();
        }

        /// <inheritdoc/>
        public async Task<Dictionary<string, string>> ConfigGetAsync(string option)
        {
            return (await _db.ExecuteAsync(FT.CONFIG, "GET", option)).ToConfigDictionary();
        }

        /// <inheritdoc/>
        public bool ConfigSet(string option, string value)
        {
            return _db.Execute(FT.CONFIG, "SET", option, value).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> ConfigSetAsync(string option, string value)
        {
            return (await _db.ExecuteAsync(FT.CONFIG, "SET", option, value)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool Create(string indexName, FTCreateParams parameters, Schema schema)
        {
            var args = new List<object>() { indexName };
            parameters.AddParams(args); // TODO: Think of a better implementation

            args.Add("SCHEMA");

            foreach (var f in schema.Fields)
            {
                f.AddSchemaArgs(args);
            }

            return _db.Execute(FT.CREATE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> CreateAsync(string indexName, FTCreateParams parameters, Schema schema)
        {
            var args = new List<object>() { indexName };
            parameters.AddParams(args); // TODO: Think of a better implementation
            args.Add("SCHEMA");
            foreach (var f in schema.Fields)
            {
                f.AddSchemaArgs(args);
            }
            return (await _db.ExecuteAsync(FT.CREATE, args)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool CursorDel(string indexName, long cursorId)
        {
            return _db.Execute(FT.CURSOR, "DEL", indexName, cursorId).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> CursorDelAsync(string indexName, long cursorId)
        {
            return (await _db.ExecuteAsync(FT.CURSOR, "DEL", indexName, cursorId)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public AggregationResult CursorRead(string indexName, long cursorId, int? count = null)
        {
            RedisResult[] resp = ((count == null) ? _db.Execute(FT.CURSOR, "READ", indexName, cursorId)
                                                  : _db.Execute(FT.CURSOR, "READ", indexName, cursorId, "COUNT", count))
                                                  .ToArray();

            return new AggregationResult(resp[0], (long)resp[1]);
        }

        /// <inheritdoc/>
        public async Task<AggregationResult> CursorReadAsync(string indexName, long cursorId, int? count = null)
        {
            RedisResult[] resp = (await ((count == null) ? _db.ExecuteAsync(FT.CURSOR, "READ", indexName, cursorId)
                                                         : _db.ExecuteAsync(FT.CURSOR, "READ", indexName, cursorId, "COUNT", count)))
                                                         .ToArray();

            return new AggregationResult(resp[0], (long)resp[1]);
        }

        /// <inheritdoc/>
        public long DictAdd(string dict, params string[] terms)
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

            return _db.Execute(FT.DICTADD, args).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> DictAddAsync(string dict, params string[] terms)
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

            return (await _db.ExecuteAsync(FT.DICTADD, args)).ToLong();
        }

        /// <inheritdoc/>
        public long DictDel(string dict, params string[] terms)
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

            return _db.Execute(FT.DICTDEL, args).ToLong();
        }

        /// <inheritdoc/>
        public async Task<long> DictDelAsync(string dict, params string[] terms)
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

            return (await _db.ExecuteAsync(FT.DICTDEL, args)).ToLong();
        }

        /// <inheritdoc/>
        public RedisResult[] DictDump(string dict)
        {
            return _db.Execute(FT.DICTDUMP, dict).ToArray();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> DictDumpAsync(string dict)
        {
            return (await _db.ExecuteAsync(FT.DICTDUMP, dict)).ToArray();
        }

        /// <inheritdoc/>
        public bool DropIndex(string indexName, bool dd = false)
        {
            return ((dd) ? _db.Execute(FT.DROPINDEX, indexName, "DD")
                         : _db.Execute(FT.DROPINDEX, indexName))
                        .OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> DropIndexAsync(string indexName, bool dd = false)
        {
            return (await ((dd) ? _db.ExecuteAsync(FT.DROPINDEX, indexName, "DD")
                                : _db.ExecuteAsync(FT.DROPINDEX, indexName)))
                                .OKtoBoolean();
        }

        /// <inheritdoc/>
        public string Explain(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return _db.Execute(FT.EXPLAIN, args).ToString();
        }

        /// <inheritdoc/>
        public async Task<string> ExplainAsync(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return (await _db.ExecuteAsync(FT.EXPLAIN, args)).ToString();
        }

        /// <inheritdoc/>
        public RedisResult[] ExplainCli(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return _db.Execute(FT.EXPLAINCLI, args).ToArray();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> ExplainCliAsync(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return (await _db.ExecuteAsync(FT.EXPLAINCLI, args)).ToArray();
        }

        /// <inheritdoc/>
        public InfoResult Info(RedisValue index) =>
        new InfoResult(_db.Execute("FT.INFO", index));

        /// <inheritdoc/>
        public async Task<InfoResult> InfoAsync(RedisValue index) =>
        new InfoResult(await _db.ExecuteAsync("FT.INFO", index));

        // TODO: FT.PROFILE (jedis doesn't have it)

        /// <inheritdoc/>
        public SearchResult Search(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);

            var resp = _db.Execute("FT.SEARCH", args).ToArray();
            return new SearchResult(resp, !q.NoContent, q.WithScores, q.WithPayloads, q.ExplainScore);
        }

        /// <inheritdoc/>
        public async Task<SearchResult> SearchAsync(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            var resp = (await _db.ExecuteAsync("FT.SEARCH", args)).ToArray();
            return new SearchResult(resp, !q.NoContent, q.WithScores, q.WithPayloads, q.ExplainScore);
        }

        /// <inheritdoc/>
        public Dictionary<string, List<string>> SynDump(string indexName)
        {
            var resp = _db.Execute(FT.SYNDUMP, indexName).ToArray();
            var result = new Dictionary<string, List<string>>();
            for (int i = 0; i < resp.Length; i += 2)
            {
                var term = resp[i].ToString();
                var synonyms = (resp[i + 1]).ToArray().Select(x => x.ToString()).ToList(); // TODO: consider leave synonyms as RedisValue[]
                result.Add(term, synonyms);
            }
            return result;
        }

        // TODO: FT.SPELLCHECK (jedis doesn't have it)

        /// <inheritdoc/>
        public async Task<Dictionary<string, List<string>>> SynDumpAsync(string indexName)
        {
            var resp = (await _db.ExecuteAsync(FT.SYNDUMP, indexName)).ToArray();
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
            if (terms.Length < 1)
            {
                throw new ArgumentOutOfRangeException("terms must have at least one element");
            }
            var args = new List<object> { indexName, synonymGroupId };
            if (skipInitialScan) { args.Add(SearchArgs.SKIPINITIALSCAN); }
            args.AddRange(terms);
            return _db.Execute(FT.SYNUPDATE, args).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<bool> SynUpdateAsync(string indexName, string synonymGroupId, bool skipInitialScan = false, params string[] terms)
        {
            if (terms.Length < 1)
            {
                throw new ArgumentOutOfRangeException("terms must have at least one element");
            }
            var args = new List<object> { indexName, synonymGroupId };
            if (skipInitialScan) { args.Add(SearchArgs.SKIPINITIALSCAN); }
            args.AddRange(terms);
            return (await _db.ExecuteAsync(FT.SYNUPDATE, args)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public RedisResult[] TagVals(string indexName, string fieldName) => //TODO: consider return Set
        _db.Execute(FT.TAGVALS, indexName, fieldName).ToArray();

        /// <inheritdoc/>
        public async Task<RedisResult[]> TagValsAsync(string indexName, string fieldName) => //TODO: consider return Set
        (await _db.ExecuteAsync(FT.TAGVALS, indexName, fieldName)).ToArray();
    }
}