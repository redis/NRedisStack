using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.DataTypes;
using StackExchange.Redis;
namespace NRedisStack
{
    public class SearchCommands : SearchCommandsAsync, ISearchCommands
    {
        IDatabase _db;
        public SearchCommands(IDatabase db, int? defaultDialect) : base(db)
        {
            _db = db;
            SetDefaultDialect(defaultDialect);
            this.defaultDialect = defaultDialect;
        }

        public void SetDefaultDialect(int? defaultDialect)
        {
            if (defaultDialect == 0)
            {
                throw new System.ArgumentOutOfRangeException("DIALECT=0 cannot be set.");
            }
            this.defaultDialect = defaultDialect;
        }

        /// <inheritdoc/>
        public RedisResult[] _List()
        {
            return _db.Execute(SearchCommandBuilder._List()).ToArray();
        }

        /// <inheritdoc/>
        public AggregationResult Aggregate(string index, AggregationRequest query)
        {
            if (query.dialect == null && defaultDialect != null)
            {
                query.Dialect((int)defaultDialect);
            }

            var result = _db.Execute(SearchCommandBuilder.Aggregate(index, query));
            return result.ToAggregationResult(query);
        }

        /// <inheritdoc/>
        public bool AliasAdd(string alias, string index)
        {
            return _db.Execute(SearchCommandBuilder.AliasAdd(alias, index)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool AliasDel(string alias)
        {
            return _db.Execute(SearchCommandBuilder.AliasDel(alias)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool AliasUpdate(string alias, string index)
        {
            return _db.Execute(SearchCommandBuilder.AliasUpdate(alias, index)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool Alter(string index, Schema schema, bool skipInitialScan = false)
        {
            return _db.Execute(SearchCommandBuilder.Alter(index, schema, skipInitialScan)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public Dictionary<string, string> ConfigGet(string option)
        {
            return _db.Execute(SearchCommandBuilder.ConfigGet(option)).ToConfigDictionary();
        }

        /// <inheritdoc/>
        public bool ConfigSet(string option, string value)
        {
            return _db.Execute(SearchCommandBuilder.ConfigSet(option, value)).OKtoBoolean();
        }

        // TODO: Add an ability to add fildes like that: TextField.Of("name")
        /// <inheritdoc/>
        public bool Create(string indexName, FTCreateParams parameters, Schema schema)
        {
            return _db.Execute(SearchCommandBuilder.Create(indexName, parameters, schema)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public bool Create(string indexName, Schema schema)
        {
            return Create(indexName, new FTCreateParams(), schema);
        }

        /// <inheritdoc/>
        public bool CursorDel(string indexName, long cursorId)
        {
            return _db.Execute(SearchCommandBuilder.CursorDel(indexName, cursorId)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public AggregationResult CursorRead(string indexName, long cursorId, int? count = null)
        {
            var resp = _db.Execute(SearchCommandBuilder.CursorRead(indexName, cursorId, count)).ToArray();
            return new AggregationResult(resp[0], (long)resp[1]);
        }

        /// <inheritdoc/>
        public long DictAdd(string dict, params string[] terms)
        {
            return _db.Execute(SearchCommandBuilder.DictAdd(dict, terms)).ToLong();
        }

        /// <inheritdoc/>
        public long DictDel(string dict, params string[] terms)
        {
            return _db.Execute(SearchCommandBuilder.DictDel(dict, terms)).ToLong();
        }

        /// <inheritdoc/>
        public RedisResult[] DictDump(string dict)
        {
            return _db.Execute(SearchCommandBuilder.DictDump(dict)).ToArray();
        }

        /// <inheritdoc/>
        public bool DropIndex(string indexName, bool dd = false)
        {
            return _db.Execute(SearchCommandBuilder.DropIndex(indexName, dd)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public string Explain(string indexName, string query, int? dialect = null)
        {
            if (dialect == null && defaultDialect != null)
            {
                dialect = defaultDialect;
            }
            return _db.Execute(SearchCommandBuilder.Explain(indexName, query, dialect)).ToString()!;
        }

        /// <inheritdoc/>
        public RedisResult[] ExplainCli(string indexName, string query, int? dialect = null)
        {
            if (dialect == null && defaultDialect != null)
            {
                dialect = defaultDialect;
            }
            return _db.Execute(SearchCommandBuilder.ExplainCli(indexName, query, dialect)).ToArray();
        }

        /// <inheritdoc/>
        public InfoResult Info(RedisValue index) =>
        new InfoResult(_db.Execute(SearchCommandBuilder.Info(index)));

        /// <inheritdoc/>
        public Tuple<SearchResult, Dictionary<string, RedisResult>> ProfileSearch(string indexName, Query q, bool limited = false)
        {
            return _db.Execute(SearchCommandBuilder.ProfileSearch(indexName, q, limited))
                            .ToProfileSearchResult(q);
        }
        /// <inheritdoc/>
        public Tuple<AggregationResult, Dictionary<string, RedisResult>> ProfileAggregate(string indexName, AggregationRequest query, bool limited = false)
        {
            return _db.Execute(SearchCommandBuilder.ProfileAggregate(indexName, query, limited))
                            .ToProfileAggregateResult(query);
        }
        /// <inheritdoc/>
        public SearchResult Search(string indexName, Query q)
        {
            if (q.dialect == null && defaultDialect != null)
            {
                q.Dialect((int)defaultDialect);
            }
            return _db.Execute(SearchCommandBuilder.Search(indexName, q)).ToSearchResult(q);
        }

        /// <inheritdoc/>
        public Dictionary<string, Dictionary<string, double>> SpellCheck(string indexName, string query, FTSpellCheckParams? spellCheckParams = null)
        {
            return _db.Execute(SearchCommandBuilder.SpellCheck(indexName, query, spellCheckParams)).ToFtSpellCheckResult();
        }

        /// <inheritdoc/>
        public long SugAdd(string key, string str, double score, bool increment = false, string? payload = null)
        {
            return _db.Execute(SearchCommandBuilder.SugAdd(key, str, score, increment, payload)).ToLong();
        }


        /// <inheritdoc/>
        public bool SugDel(string key, string str)
        {
            return _db.Execute(SearchCommandBuilder.SugDel(key, str)).ToString() == "1";
        }


        /// <inheritdoc/>
        public List<string> SugGet(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null)
        {
            return _db.Execute(SearchCommandBuilder.SugGet(key, prefix, fuzzy, false, withPayloads, max)).ToStringList();
        }

        /// <inheritdoc/>
        public List<Tuple<string, double>> SugGetWithScores(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null)
        {
            return _db.Execute(SearchCommandBuilder.SugGet(key, prefix, fuzzy, true, withPayloads, max)).ToStringDoubleTupleList();
        }


        /// <inheritdoc/>
        public long SugLen(string key)
        {
            return _db.Execute(SearchCommandBuilder.SugLen(key)).ToLong();
        }


        /// <inheritdoc/>
        public Dictionary<string, List<string>> SynDump(string indexName)
        {
            var resp = _db.Execute(SearchCommandBuilder.SynDump(indexName)).ToArray();
            var result = new Dictionary<string, List<string>>();
            for (int i = 0; i < resp.Length; i += 2)
            {
                var term = resp[i].ToString();
                var synonyms = (resp[i + 1]).ToArray().Select(x => x.ToString()).ToList(); // TODO: consider leave synonyms as RedisValue[]
                result.Add(term!, synonyms!);
            }
            return result;
        }


        /// <inheritdoc/>
        public bool SynUpdate(string indexName, string synonymGroupId, bool skipInitialScan = false, params string[] terms)
        {
            return _db.Execute(SearchCommandBuilder.SynUpdate(indexName, synonymGroupId, skipInitialScan, terms)).OKtoBoolean();
        }

        /// <inheritdoc/>
        public RedisResult[] TagVals(string indexName, string fieldName) => //TODO: consider return Set
        _db.Execute(SearchCommandBuilder.TagVals(indexName, fieldName)).ToArray();
    }
}