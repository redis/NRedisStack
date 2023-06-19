using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{
    public class SearchCommandsAsync : ISearchCommandsAsync
    {
        IDatabaseAsync _db;
        protected int? defaultDialect;

        public SearchCommandsAsync(IDatabaseAsync db)
        {
            _db = db;
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> _ListAsync()
        {
            return (await _db.ExecuteAsync(SearchCommandBuilder._List())).ToArray();
        }

        /// <inheritdoc/>
        public async Task<AggregationResult> AggregateAsync(string index, AggregationRequest query)
        {
            if (query.dialect == null && defaultDialect != null)
            {
                query.Dialect((int)defaultDialect);
            }

            var result = await _db.ExecuteAsync(SearchCommandBuilder.Aggregate(index, query));
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
        public async Task<Dictionary<string, string>> ConfigGetAsync(string option)
        {
            return (await _db.ExecuteAsync(SearchCommandBuilder.ConfigGet(option))).ToConfigDictionary();
        }

        /// <inheritdoc/>
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
        public async Task<bool> CursorDelAsync(string indexName, long cursorId)
        {
            return (await _db.ExecuteAsync(SearchCommandBuilder.CursorDel(indexName, cursorId))).OKtoBoolean();
        }

        /// <inheritdoc/>
        public async Task<AggregationResult> CursorReadAsync(string indexName, long cursorId, int? count = null)
        {
            var resp = (await _db.ExecuteAsync(SearchCommandBuilder.CursorRead(indexName, cursorId, count))).ToArray();
            return new AggregationResult(resp[0], (long)resp[1]);
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
            if (dialect == null && defaultDialect != null)
            {
                dialect = defaultDialect;
            }

            return (await _db.ExecuteAsync(SearchCommandBuilder.Explain(indexName, query, dialect))).ToString();
        }

        /// <inheritdoc/>
        public async Task<RedisResult[]> ExplainCliAsync(string indexName, string query, int? dialect = null)
        {
            if (dialect == null && defaultDialect != null)
            {
                dialect = defaultDialect;
            }

            return (await _db.ExecuteAsync(SearchCommandBuilder.ExplainCli(indexName, query, dialect))).ToArray();
        }

        /// <inheritdoc/>
        public async Task<InfoResult> InfoAsync(RedisValue index) =>
        new InfoResult(await _db.ExecuteAsync(SearchCommandBuilder.Info(index)));

        // TODO: FT.PROFILE (jedis doesn't have it)

        /// <inheritdoc/>
        public async Task<SearchResult> SearchAsync(string indexName, Query q)
        {
            if (q.dialect == null && defaultDialect != null)
            {
                q.Dialect((int)defaultDialect);
            }

            var resp = (await _db.ExecuteAsync(SearchCommandBuilder.Search(indexName, q))).ToArray();
            return new SearchResult(resp, !q.NoContent, q.WithScores, q.WithPayloads/*, q.ExplainScore*/);
        }

        /// <inheritdoc/>
        public async Task<Dictionary<string, Dictionary<string, double>>> SpellCheckAsync(string indexName, string query, FTSpellCheckParams? spellCheckParams = null)
        {
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
        public async Task<List<string>> SugGetAsync(string key, string prefix, bool fuzzy = false, bool withScores = false, bool withPayloads = false, int? max = null)
        {
            return (await _db.ExecuteAsync(SearchCommandBuilder.SugGet(key, prefix, fuzzy, withScores, withPayloads, max))).ToStringList();

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
    }
}