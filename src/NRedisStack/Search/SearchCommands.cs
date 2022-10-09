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
            foreach(var arg in query.GetArgs())
            {
                args.Add(arg);
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
            foreach(var arg in query.GetArgs())
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
            return result.ToConfigDictionary(); // TODO: fix all tests to be like this
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

        // TODO: FT.CURSOR DEL
        // TODO: FT.CURSOR READ
        // TODO: FT.DICTADD
        // TODO: FT.DICTDEL
        // TODO: FT.DICTDUMP
        // TODO: FT.DROPINDEX
        // TODO: FT.EXPLAIN
        // TODO: FT.EXPLAINCLI

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

        // TODO: FT.PROFILE

        /// <summary>
        /// Search the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">a <see cref="Query"/> object with the query string and optional parameters</param>
        /// <returns>a <see cref="SearchResult"/> object with the results</returns>
        public SearchResult Search(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            // {
            //     _boxedIndexName
            // };
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
        public async Task<SearchResult> SearchAsync(string indexName, Query q)
        {
            var args = new List<object> { indexName };
            q.SerializeRedisArgs(args);
            var resp = (await _db.ExecuteAsync("FT.SEARCH", args)).ToArray();
            return new SearchResult(resp, !q.NoContent, q.WithScores, q.WithPayloads, q.ExplainScore);
        }

        // TODO: FT.SPELLCHECK
        // TODO: FT.SYNDUMP
        // TODO: FT.SYNUPDATE
        // TODO: FT.TAGVALS
    }
}