using NRedisStack.Search;
using NRedisStack.Search.Aggregation;
using NRedisStack.Search.DataTypes;
using StackExchange.Redis;

namespace NRedisStack
{
    public interface ISearchCommandsAsync
    {
        /// <summary>
        /// Returns a list of all existing indexes.
        /// </summary>
        /// <returns>Array with index names.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft._list"/></remarks>
        Task<RedisResult[]> _ListAsync();

        /// <summary>
        /// Run a search query on an index, and perform aggregate transformations on the results.
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="query">The query</param>
        /// <returns>An <see langword="AggregationResult"/> object</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aggregate"/></remarks>
        Task<AggregationResult> AggregateAsync(string index, AggregationRequest query);

        /// <summary>
        /// Add an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be added to an index.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasadd"/></remarks>
        Task<bool> AliasAddAsync(string alias, string index);

        /// <summary>
        /// Remove an alias to an index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        Task<bool> AliasDelAsync(string alias);

        /// <summary>
        /// Add an alias to an index. If the alias is already associated with another index,
        /// FT.ALIASUPDATE removes the alias association with the previous index.
        /// </summary>
        /// <param name="alias">Alias to be removed.</param>
        /// <param name="index">The index name.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.aliasdel"/></remarks>
        Task<bool> AliasUpdateAsync(string alias, string index);

        /// <summary>
        /// Add a new attribute to the index
        /// </summary>
        /// <param name="index">The index name.</param>
        /// <param name="skipInitialScan">If set, does not scan and index.</param>
        /// <param name="schema">the schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.alter"/></remarks>
        Task<bool> AlterAsync(string index, Schema schema, bool skipInitialScan = false);

        /// <summary>
        /// Retrieve configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <returns>An array reply of the configuration name and value.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-get"/></remarks>
        Task<Dictionary<string, string>> ConfigGetAsync(string option);

        /// <summary>
        /// Describe configuration options.
        /// </summary>
        /// <param name="option">is name of the configuration option, or '*' for all.</param>
        /// <param name="value">is value of the configuration option.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.config-set"/></remarks>
        Task<bool> ConfigSetAsync(string option, string value);

        /// <summary>
        /// Create an index with the given specification.
        /// </summary>
        /// <param name="indexName">The index name.</param>
        /// <param name="parameters">Command's parameters.</param>
        /// <param name="schema">The index schema.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.create"/></remarks>
        Task<bool> CreateAsync(string indexName, FTCreateParams parameters, Schema schema);

        /// <summary>
        /// Delete a cursor from the index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <returns><see langword="true"/> if it has been deleted, <see langword="false"/> if it did not exist.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-del/"/></remarks>
        Task<bool> CursorDelAsync(string indexName, long cursorId);

        /// <summary>
        /// Read next results from an existing cursor.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="cursorId">The cursor's ID.</param>
        /// <param name="count">Limit the amount of returned results.</param>
        /// <returns>A AggregationResult object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.cursor-read/"/></remarks>
        Task<AggregationResult> CursorReadAsync(string indexName, long cursorId, int? count = null);

        /// <summary>
        /// Add terms to a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to add to the dictionary..</param>
        /// <returns>The number of new terms that were added.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictadd/"/></remarks>
        Task<long> DictAddAsync(string dict, params string[] terms);

        /// <summary>
        /// Delete terms from a dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <param name="terms">Terms to delete to the dictionary..</param>
        /// <returns>The number of new terms that were deleted.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdel/"/></remarks>
        Task<long> DictDelAsync(string dict, params string[] terms);

        /// <summary>
        /// Dump all terms in the given dictionary.
        /// </summary>
        /// <param name="dict">The dictionary name</param>
        /// <returns>An array, where each element is term.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dictdump/"/></remarks>
        Task<RedisResult[]> DictDumpAsync(string dict);

        /// <summary>
        /// Delete an index.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="dd">If set, deletes the actual document hashes.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.dropindex/"/></remarks>
        Task<bool> DropIndexAsync(string indexName, bool dd = false);

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="query">The query to explain</param>
        /// <param name="dialect">Dialect version under which to execute the query</param>
        /// <returns>String that representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explain/"/></remarks>
        Task<string> ExplainAsync(string indexName, string query, int? dialect = null);

        /// <summary>
        /// Return the execution plan for a complex query
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="query">The query to explain</param>
        /// <param name="dialect">Dialect version under which to execute the query</param>
        /// <returns>An array reply with a string representing the execution plan</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.explaincli/"/></remarks>
        Task<RedisResult[]> ExplainCliAsync(string indexName, string query, int? dialect = null);

        /// <summary>
        /// Return information and statistics on the index.
        /// </summary>
        /// <param name="key">The name of the index.</param>
        /// <returns>Dictionary of key and value with information about the index</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.info"/></remarks>
        Task<InfoResult> InfoAsync(RedisValue index);

        // TODO: FT.PROFILE (jedis doesn't have it)

        /// <summary>
        /// Search the index
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="q">a <see cref="Query"/> object with the query string and optional parameters</param>
        /// <returns>a <see cref="SearchResult"/> object with the results</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.search"/></remarks>
        Task<SearchResult> SearchAsync(string indexName, Query q);

        /// <summary>
        /// Perform spelling correction on a query, returning suggestions for misspelled terms.
        /// </summary>
        /// <param name="indexName">is index with the indexed terms.</param>
        /// <param name="query">is search query.</param>
        /// <param name="spellCheckParams">Optional Spellcheck Parameters</param>
        /// <returns>An array reply, in which each element represents a misspelled term from the query.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.spellcheck]"/></remarks>
        ///
        Task<Dictionary<string, Dictionary<string, double>>> SpellCheckAsync(string indexName, string query, FTSpellCheckParams? spellCheckParams = null);

        /// <summary>
        /// Add a suggestion string to an auto-complete suggestion dictionary
        /// </summary>
        /// <param name="key">is suggestion dictionary key.</param>
        /// <param name="str">is suggestion string to index.</param>
        /// <param name="score">is floating point number of the suggestion string's weight.</param>
        /// <param name="increment">increments the existing entry of the suggestion by the given score,
        ///  instead of replacing the score.</param>
        /// <param name="payload">saves an extra payload with the suggestion, that can be fetched by adding the WITHPAYLOADS argument to FT.SUGGET.</param>
        /// <returns>The current size of the suggestion dictionary.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.sugadd"/></remarks>
        Task<long> SugAddAsync(string key, string str, double score, bool increment = false, string? payload = null);

        /// <summary>
        /// Delete a string from a suggestion index.
        /// </summary>
        /// <param name="key">is suggestion dictionary key.</param>
        /// <param name="str">is suggestion string to index.</param>
        /// <returns><see langword="true"/> if the string was found and deleted, <see langword="false"/> otherwise.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.sugdel"/></remarks>
        Task<bool> SugDelAsync(string key, string str);

        /// <summary>
        /// Get completion suggestions for a prefix.
        /// </summary>
        /// <param name="key">is suggestion dictionary key.</param>
        /// <param name="prefix">is prefix to complete on.</param>
        /// <param name="fuzzy">performs a fuzzy prefix search,
        /// including prefixes at Levenshtein distance of 1 from the prefix sent.</param>
        /// <param name="withPayloads">returns optional payloads saved along with the suggestions.</param>
        /// <param name="max">limits the results to a maximum of num (default: 5).</param>
        /// <returns>List of the top suggestions matching the prefix.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.sugget"/></remarks>
        Task<List<string>> SugGetAsync(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null);

        /// <summary>
        /// Get completion suggestions for a prefix with the score of each suggestion.
        /// </summary>
        /// <param name="key">is suggestion dictionary key.</param>
        /// <param name="prefix">is prefix to complete on.</param>
        /// <param name="fuzzy">performs a fuzzy prefix search,
        /// including prefixes at Levenshtein distance of 1 from the prefix sent.</param>
        /// <param name="withPayloads">returns optional payloads saved along with the suggestions.</param>
        /// <param name="max">limits the results to a maximum of num (default: 5).</param>
        /// <returns>List of the top suggestions matching the prefix.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.sugget"/></remarks>
        Task<List<Tuple<string, double>>> SugGetWithScoresAsync(string key, string prefix, bool fuzzy = false, bool withPayloads = false, int? max = null);

        /// <summary>
        /// Get the size of an auto-complete suggestion dictionary.
        /// </summary>
        /// <param name="key">is suggestion dictionary key.</param>
        /// <returns>The current size of the suggestion dictionary.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.suglen"/></remarks>
        Task<long> SugLenAsync(string key);

        /// <summary>
        /// Dump the contents of a synonym group.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <returns>Pairs of term and an array of synonym groups.</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.syndump"/></remarks>
        Task<Dictionary<string, List<string>>> SynDumpAsync(string indexName);

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
        Task<bool> SynUpdateAsync(string indexName, string synonymGroupId, bool skipInitialScan = false, params string[] terms);

        /// <summary>
        /// Return a distinct set of values indexed in a Tag field.
        /// </summary>
        /// <param name="indexName">The index name</param>
        /// <param name="fieldName">TAG field name</param>
        /// <returns>List of TAG field values</returns>
        /// <remarks><seealso href="https://redis.io/commands/ft.tagvals"/></remarks>
        Task<RedisResult[]> TagValsAsync(string indexName, string fieldName);
    }
}