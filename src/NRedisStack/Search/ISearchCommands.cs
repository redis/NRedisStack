using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.DataTypes;
using StackExchange.Redis;

namespace NRedisStack
{
    public interface ISearchCommands
    {

        /// <summary>
        /// Returns a list of all existing indexes.
        /// </summary>
        /// <returns>Array with index names.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft._list"/></remarks>
        RedisResult[] _List();

        /// <summary>
        /// Run a search query on an index, and perform aggregate transformations on the results.
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="query">The query</param>
        /// <returns>An <see langword="AggregationResult"/> object</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aggregate"/></remarks>
        AggregationResult Aggregate(string index, AggregationRequest query);

        /// <summary>
        /// Add an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be added to an index.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasadd"/></remarks>
        bool AliasAdd(string alias, string index);

        /// <summary>
        /// Remove an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        bool AliasDel(string alias);

        /// <summary>
        /// Add an alias to an index. If the alias is already associated with another index,
        /// FT.ALIASUPDATE removes the alias association with the previous index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        bool AliasUpdate(string alias, string index);

        /// <summary>
        /// Add a new attribute to the index
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="skipInitialScan">If set, does not scan and index.</param>
        /// <param name="schema">the schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.alter"/></remarks>
        bool Alter(string index, Schema schema, bool skipInitialScan = false);

        /// <summary>
        /// Retrieve configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <returns>An array reply of the configuration name and value.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-get"/></remarks>
        Dictionary<string, string> ConfigGet(string option);

        /// <summary>
        /// Describe configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <param name="value">is value of the configuration option.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-set"/></remarks>
        bool ConfigSet(string option, string value);

        /// <summary>
        /// Create an index with the given specification.
        /// </summary>
        /// <param name="indexName">The index name.</param>
        /// <param name="parameters">Command's parameters.</param>
        /// <param name="schema">The index schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.create"/></remarks>
        bool Create(string indexName, FTCreateParams parameters, Schema schema);

        /// <summary>
        /// Delete a cursor from the index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <returns><see langword="true"/> if it has been deleted, <see langword="false"/> if it did not exist.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-del/"/></remarks>
        bool CursorDel(string indexName, long cursorId);

        /// <summary>
        /// Read next results from an existing cursor.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <param name="count">Limit the amount of returned results.</param>
        /// <returns>A AggregationResult object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-read/"/></remarks>
        AggregationResult CursorRead(string indexName, long cursorId, int? count = null);

        /// <summary>
        /// Add terms to a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to add to the dictionary..</param>
        /// <returns>The number of new terms that were added.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictadd/"/></remarks>
        long DictAdd(string dict, params string[] terms);

        /// <summary>
        /// Delete terms from a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to delete to the dictionary..</param>
        /// <returns>The number of new terms that were deleted.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdel/"/></remarks>
        long DictDel(string dict, params string[] terms);

        /// <summary>
        /// Dump all terms in the given dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <returns>An array, where each element is term.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdump/"/></remarks>
        RedisResult[] DictDump(string dict);

        /// <summary>
        /// Delete an index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="dd">If set, deletes the actual document hashes.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dropindex/"/></remarks>
        bool DropIndex(string indexName, bool dd = false);

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">The query to explain</param>
        /// <returns>String that representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explain/"/></remarks>
        string Explain(string indexName, Query q);

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">The query to explain</param>
        /// <returns>An array reply with a string representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explaincli/"/></remarks>
        RedisResult[] ExplainCli(string indexName, Query q);

        /// <summary>
        /// Return information and statistics on the index.
        /// </summary>
        /// <param name="key">The name of the index.</param>
        /// <returns>Dictionary of key and value with information about the index</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.info"/></remarks>
        InfoResult Info(RedisValue index);

        // TODO: FT.PROFILE (jedis doesn't have it)

        /// <summary>
        /// Search the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">a <see cref="Query"/> object with the query string and optional parameters</param>
        /// <returns>a <see cref="SearchResult"/> object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.search"/></remarks>
        SearchResult Search(string indexName, Query q);

        /// <summary>
        /// Dump the contents of a synonym group.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <returns>Pairs of term and an array of synonym groups.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.syndump"/></remarks>
        Dictionary<string, List<string>> SynDump(string indexName);

        // TODO: FT.SPELLCHECK (jedis doesn't have it)

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
        bool SynUpdate(string indexName, string synonymGroupId, bool skipInitialScan = false, params string[] terms);

        /// <summary>
        /// Return a distinct set of values indexed in a Tag field.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="fieldName">TAG field name</param>
        /// <returns>List of TAG field values</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.tagvals"/></remarks>
        RedisResult[] TagVals(string indexName, string fieldName);
    }
}