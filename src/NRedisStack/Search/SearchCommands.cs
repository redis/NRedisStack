using NRedisStack.Literals;
using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.DataTypes;
using NRedisStack.Search.FT.CREATE;
using StackExchange.Redis;
namespace NRedisStack
{
    public class SearchCommands
    {
        IDatabase _db;
        public SearchCommands(IDatabase db)
        {
            _db = db;
        }

        /// <summary>
        /// Returns a list of all existing indexes.
        /// </summary>
        /// <returns>Array with index names.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft._list"/></remarks>
        public RedisResult[] _List()
        {
            return _db.Execute(FT._LIST).ToArray();
        }

        /// <summary>
        /// Returns a list of all existing indexes.
        /// </summary>
        /// <returns>Array with index names.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft._list"/></remarks>
        public async Task<RedisResult[]> _ListAsync()
        {
            return (await _db.ExecuteAsync(FT._LIST)).ToArray();
        }

        /// <summary>
        /// Run a search query on an index, and perform aggregate transformations on the results.
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="query">The query</param>
        /// <returns>An <see langword="AggregationResult"/> object</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aggregate"/></remarks>
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

        /// <summary>
        /// Run a search query on an index, and perform aggregate transformations on the results.
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="query">The query</param>
        /// <returns>An <see langword="AggregationResult"/> object</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aggregate"/></remarks>
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

        /// <summary>
        /// Add an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be added to an index.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasadd"/></remarks>
        public bool AliasAdd(string alias, string index)
        {
            return _db.Execute(FT.ALIASADD, alias, index).OKtoBoolean();
        }

        /// <summary>
        /// Add an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be added to an index.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasadd"/></remarks>
        public async Task<bool> AliasAddAsync(string alias, string index)
        {
            return (await _db.ExecuteAsync(FT.ALIASADD, alias, index)).OKtoBoolean();
        }

        /// <summary>
        /// Remove an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public bool AliasDel(string alias)
        {
            return _db.Execute(FT.ALIASDEL, alias).OKtoBoolean();
        }

        /// <summary>
        /// Remove an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public async Task<bool> AliasDelAsync(string alias)
        {
            return (await _db.ExecuteAsync(FT.ALIASDEL, alias)).OKtoBoolean();
        }

        /// <summary>
        /// Add an alias to an index. If the alias is already associated with another index,
        /// FT.ALIASUPDATE removes the alias association with the previous index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public bool AliasUpdate(string alias, string index)
        {
            return _db.Execute(FT.ALIASUPDATE, alias, index).OKtoBoolean();
        }

        /// <summary>
        /// Add an alias to an index. If the alias is already associated with another index,
        /// FT.ALIASUPDATE removes the alias association with the previous index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        public async Task<bool> AliasUpdateAsync(string alias, string index)
        {
            return (await _db.ExecuteAsync(FT.ALIASUPDATE, alias, index)).OKtoBoolean();
        }

        /// <summary>
        /// Add a new attribute to the index
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="skipInitialScan">If set, does not scan and index.</param>
        /// <param name="schema">the schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.alter"/></remarks>
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

        /// <summary>
        /// Add a new attribute to the index
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="skipInitialScan">If set, does not scan and index.</param>
        /// <param name="schema">the schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.alter"/></remarks>
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

        /// <summary>
        /// Retrieve configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <returns>An array reply of the configuration name and value.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-get"/></remarks>
        public Dictionary<string, string> ConfigGet(string option)
        {
            var result = _db.Execute(FT.CONFIG, "GET", option);
            return result.ToConfigDictionary();
        }

        /// <summary>
        /// Retrieve configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <returns>An array reply of the configuration name and value.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-get"/></remarks>
        public async Task<Dictionary<string, string>> ConfigGetAsync(string option)
        {
            return (await _db.ExecuteAsync(FT.CONFIG, "GET", option)).ToConfigDictionary();
        }

        /// <summary>
        /// Describe configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <param name="value">is value of the configuration option.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-set"/></remarks>
        public bool ConfigSet(string option, string value)
        {
            return _db.Execute(FT.CONFIG, "SET", option, value).OKtoBoolean();
        }

        /// <summary>
        /// Describe configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <param name="value">is value of the configuration option.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-set"/></remarks>
        public async Task<bool> ConfigSetAsync(string option, string value)
        {
            return (await _db.ExecuteAsync(FT.CONFIG, "SET", option, value)).OKtoBoolean();
        }

        /// <summary>
        /// Create an index with the given specification.
        /// </summary>
        /// <param name="indexName">The index name.</param>
        /// <param name="parameters">Command's parameters.</param>
        /// <param name="schema">The index schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.create"/></remarks>
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

        /// <summary>
        /// Create an index with the given specification.
        /// </summary>
        /// <param name="indexName">The index name.</param>
        /// <param name="parameters">Command's parameters.</param>
        /// <param name="schema">The index schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.create"/></remarks>
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

        /// <summary>
        /// Delete a cursor from the index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <returns><see langword="true"/> if it has been deleted, <see langword="false"/> if it did not exist.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-del/"/></remarks>
        public bool CursorDel(string indexName, long cursorId)
        {
            return _db.Execute(FT.CURSOR, "DEL", indexName, cursorId).OKtoBoolean();
        }

        /// <summary>
        /// Delete a cursor from the index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <returns><see langword="true"/> if it has been deleted, <see langword="false"/> if it did not exist.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-del/"/></remarks>
        public async Task<bool> CursorDelAsync(string indexName, long cursorId)
        {
            return (await _db.ExecuteAsync(FT.CURSOR, "DEL", indexName, cursorId)).OKtoBoolean();
        }

        /// <summary>
        /// Read next results from an existing cursor.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <param name="count">Limit the amount of returned results.</param>
        /// <returns>A AggregationResult object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-read/"/></remarks>
        public AggregationResult CursorRead(string indexName, long cursorId, int? count = null)
        {
            RedisResult[] resp = ((count == null) ? _db.Execute(FT.CURSOR, "READ", indexName, cursorId)
                                                  : _db.Execute(FT.CURSOR, "READ", indexName, cursorId, "COUNT", count))
                                                  .ToArray();

            return new AggregationResult(resp[0], (long)resp[1]);
        }

        /// <summary>
        /// Read next results from an existing cursor.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <param name="count">Limit the amount of returned results.</param>
        /// <returns>A AggregationResult object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-read/"/></remarks>
        public async Task<AggregationResult> CursorReadAsync(string indexName, long cursorId, int? count = null)
        {
            RedisResult[] resp = (await ((count == null) ? _db.ExecuteAsync(FT.CURSOR, "READ", indexName, cursorId)
                                                         : _db.ExecuteAsync(FT.CURSOR, "READ", indexName, cursorId, "COUNT", count)))
                                                         .ToArray();

            return new AggregationResult(resp[0], (long)resp[1]);
        }

        /// <summary>
        /// Add terms to a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to add to the dictionary..</param>
        /// <returns>The number of new terms that were added.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictadd/"/></remarks>
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

        /// <summary>
        /// Add terms to a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to add to the dictionary..</param>
        /// <returns>The number of new terms that were added.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictadd/"/></remarks>
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

        /// <summary>
        /// Delete terms from a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to delete to the dictionary..</param>
        /// <returns>The number of new terms that were deleted.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdel/"/></remarks>
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

        /// <summary>
        /// Delete terms from a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to delete to the dictionary..</param>
        /// <returns>The number of new terms that were deleted.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdel/"/></remarks>
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

        /// <summary>
        /// Dump all terms in the given dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <returns>An array, where each element is term.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdump/"/></remarks>
        public RedisResult[] DictDump(string dict)
        {
            return _db.Execute(FT.DICTDUMP, dict).ToArray();
        }

        /// <summary>
        /// Dump all terms in the given dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <returns>An array, where each element is term.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdump/"/></remarks>
        public async Task<RedisResult[]> DictDumpAsync(string dict)
        {
            return (await _db.ExecuteAsync(FT.DICTDUMP, dict)).ToArray();
        }

        /// <summary>
        /// Delete an index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="dd">If set, deletes the actual document hashes.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dropindex/"/></remarks>
        public bool DropIndex(string indexName, bool dd = false)
        {
            return ((dd) ? _db.Execute(FT.DROPINDEX, indexName, "DD")
                         : _db.Execute(FT.DROPINDEX, indexName))
                        .OKtoBoolean();
        }

        /// <summary>
        /// Delete an index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="dd">If set, deletes the actual document hashes.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dropindex/"/></remarks>
        public async Task<bool> DropIndexAsync(string indexName, bool dd = false)
        {
            return (await ((dd) ? _db.ExecuteAsync(FT.DROPINDEX, indexName, "DD")
                                : _db.ExecuteAsync(FT.DROPINDEX, indexName)))
                                .OKtoBoolean();
        }

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">The query to explain</param>
        /// <returns>String that representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explain/"/></remarks>
        public string Explain(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return _db.Execute(FT.EXPLAIN, args).ToString();
        }

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">The query to explain</param>
        /// <returns>String that representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explain/"/></remarks>
        public async Task<string> ExplainAsync(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return (await _db.ExecuteAsync(FT.EXPLAIN, args)).ToString();
        }

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">The query to explain</param>
        /// <returns>An array reply with a string representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explaincli/"/></remarks>
        public RedisResult[] ExplainCli(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return _db.Execute(FT.EXPLAINCLI, args).ToArray();
        }

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">The query to explain</param>
        /// <returns>An array reply with a string representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explaincli/"/></remarks>
        public async Task<RedisResult[]> ExplainCliAsync(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            return (await _db.ExecuteAsync(FT.EXPLAINCLI, args)).ToArray();
        }

        // /// <summary>
        // /// Return information and statistics on the index.
        // /// </summary>
        // /// <param name="key">The name of the index.</param>
        // /// <returns>Dictionary of key and value with information about the index</returns>
        // /// <remarks><seealso href="https://redis.io/commands/ft.info"/></remarks>
        // public Dictionary<string, RedisValue> Info(RedisValue index)
        // {
        //     return _db.Execute(FT.INFO, index).ToFtInfoAsDictionary();
        // }

        /// <summary>
        /// Return information and statistics on the index.
        /// </summary>
        /// <param name="key">The name of the index.</param>
        /// <returns>Dictionary of key and value with information about the index</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.info"/></remarks>
        public InfoResult Info(RedisValue index) =>
            new InfoResult(_db.Execute("FT.INFO", index));

        /// <summary>
        /// Return information and statistics on the index.
        /// </summary>
        /// <param name="key">The name of the index.</param>
        /// <returns>Dictionary of key and value with information about the index</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.info"/></remarks>
        public async Task<InfoResult> InfoAsync(RedisValue index) =>
            new InfoResult(await _db.ExecuteAsync("FT.INFO", index));

        // TODO: FT.PROFILE (jedis doesn't have it)

        /// <summary>
        /// Search the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">a <see cref="Query"/> object with the query string and optional parameters</param>
        /// <returns>a <see cref="SearchResult"/> object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.search"/></remarks>
        public SearchResult Search(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);

            var resp = _db.Execute("FT.SEARCH", args).ToArray();
            return new SearchResult(resp, !q.NoContent, q.WithScores, q.WithPayloads, q.ExplainScore);
        }

        /// <summary>
        /// Search the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">a <see cref="Query"/> object with the query string and optional parameters</param>
        /// <returns>a <see cref="SearchResult"/> object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.search"/></remarks>
        public async Task<SearchResult> SearchAsync(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            var resp = (await _db.ExecuteAsync("FT.SEARCH", args)).ToArray();
            return new SearchResult(resp, !q.NoContent, q.WithScores, q.WithPayloads, q.ExplainScore);
        }

        /// <summary>
        /// Dump the contents of a synonym group.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <returns>Pairs of term and an array of synonym groups.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.syndump"/></remarks>
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

        /// <summary>
        /// Dump the contents of a synonym group.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <returns>Pairs of term and an array of synonym groups.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.syndump"/></remarks>
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

        /// <summary>
        /// Update a synonym group.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="synonymGroupId">Is synonym group to return</param>
        /// <param name="skipInitialScan">does not scan and index, and only documents
        /// that are indexed after the update are affected</param>
        /// <param name="terms">The terms</param>
        /// <returns>Pairs of term and an array of synonym groups.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.synupdate"/></remarks>
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

        /// <summary>
        /// Update a synonym group.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="synonymGroupId">Is synonym group to return</param>
        /// <param name="skipInitialScan">does not scan and index, and only documents
        /// that are indexed after the update are affected</param>
        /// <param name="terms">The terms</param>
        /// <returns>Pairs of term and an array of synonym groups.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.synupdate"/></remarks>
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

        /// <summary>
        /// Return a distinct set of values indexed in a Tag field.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="fieldName">TAG field name</param>
        /// <returns>List of TAG field values</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.tagvals"/></remarks>
        public RedisResult[] TagVals(string indexName, string fieldName) => //TODO: consider return Set
            _db.Execute(FT.TAGVALS, indexName, fieldName).ToArray();

        /// <summary>
        /// Return a distinct set of values indexed in a Tag field.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="fieldName">TAG field name</param>
        /// <returns>List of TAG field values</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.tagvals"/></remarks>
        public async Task<RedisResult[]> TagValsAsync(string indexName, string fieldName) => //TODO: consider return Set
            (await _db.ExecuteAsync(FT.TAGVALS, indexName, fieldName)).ToArray();
    }
}