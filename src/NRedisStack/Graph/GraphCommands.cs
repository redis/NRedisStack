using System.Collections.ObjectModel;
using System.Text;
using NRedisStack.Graph;
using NRedisStack.Graph.DataTypes;
using NRedisStack.Literals;
using StackExchange.Redis;
using static NRedisStack.Graph.RedisGraphUtilities;


namespace NRedisStack
{

    public class GraphCommands
    {
        IDatabase _db;

        public GraphCommands(IDatabase db)
        {
            _db = db;
        }

        internal static readonly object CompactQueryFlag = "--COMPACT";

        private readonly IDictionary<string, GraphCache> _graphCaches = new Dictionary<string, GraphCache>();

        private GraphCache GetGraphCache(string graphName)
        {
            if (!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            return _graphCaches[graphName];
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return Query(graphName, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public async Task<ResultSet> QueryAsync(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await QueryAsync(graphName, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet Query(string graphName, string query, long? timeout = null)
        {
            if(!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.QUERY, args), _graphCaches[graphName]);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public async Task<ResultSet> QueryAsync(string graphName, string query, long? timeout = null)
        {
            if(!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(await _db.ExecuteAsync(GRAPH.QUERY, args), _graphCaches[graphName]);
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public ResultSet RO_Query(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return RO_Query(graphName, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public async Task<ResultSet> RO_QueryAsync(string graphName, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await RO_QueryAsync(graphName, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public ResultSet RO_Query(string graphName, string query, long? timeout = null)
        {
            if(!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.RO_QUERY, args), _graphCaches[graphName]);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphName">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public async Task<ResultSet> RO_QueryAsync(string graphName, string query, long? timeout = null)
        {
            if(!_graphCaches.ContainsKey(graphName))
            {
                _graphCaches.Add(graphName, new GraphCache(graphName, this));
            }

            var args = (timeout == null) ? new List<object>(3) { graphName, query, CompactQueryFlag }
                                         : new List<object>(5) { graphName, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(await _db.ExecuteAsync(GRAPH.RO_QUERY, args), _graphCaches[graphName]);
        }

        internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
            new Dictionary<string, List<string>>();

        // TODO: Check if needed
        /// <summary>
        /// Call a saved procedure.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphName, string procedure) =>
            CallProcedure(graphName, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphName, string procedure, IEnumerable<string> args) =>
            CallProcedure(graphName, procedure, args, EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphName, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return Query(graphName, queryBody.ToString());
        }

        /// <summary>
        /// Create a RedisGraph transaction.
        /// This leverages the "Transaction" support present in StackExchange.Redis.
        /// </summary>
        /// <returns>RedisGraphTransaction object</returns>
        public RedisGraphTransaction Multi() =>
            new RedisGraphTransaction(_db.CreateTransaction(), this, _graphCaches);

        /// <summary>
        /// Delete an existing graph.
        /// </summary>
        /// <param name="graphName">The graph to delete.</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.delete"/></remarks>
        public ResultSet Delete(string graphName)
        {
            var result = _db.Execute(GRAPH.DELETE, graphName);

            var processedResult = new ResultSet(result, _graphCaches[graphName]);

            _graphCaches.Remove(graphName);

            return processedResult;
        }

        /// <summary>
        /// Delete an existing graph.
        /// </summary>
        /// <param name="graphName">The graph to delete.</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.delete"/></remarks>
        public async Task<ResultSet> DeleteAsync(string graphName)
        {
            var result = await _db.ExecuteAsync(GRAPH.DELETE, graphName);

            var processedResult = new ResultSet(result, _graphCaches[graphName]);

            _graphCaches.Remove(graphName);

            return processedResult;
        }

        // TODO: Check if this (CallProcedure) is needed
        /// <summary>
        /// Call a saved procedure against a read-only node.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphName, string procedure) =>
            CallProcedureReadOnly(graphName, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters against a read-only node.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphName, string procedure, IEnumerable<string> args) =>
            CallProcedureReadOnly(graphName, procedure, args, EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters against a read-only node.
        /// </summary>
        /// <param name="graphName">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphName, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return RO_Query(graphName, queryBody.ToString());
        }

        /// <summary>
        /// Constructs a query execution plan but does not run it. Inspect this execution plan to better understand how your
        /// query will get executed.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <param name="query">The query.</param>
        /// <returns>String representation of a query execution plan.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.explain"/></remarks>
        public IReadOnlyList<string> Explain(string graphName, string query)
        {
             return _db.Execute(GRAPH.EXPLAIN, graphName, query).ToStringList();
        }

        /// <summary>
        /// Constructs a query execution plan but does not run it. Inspect this execution plan to better understand how your
        /// query will get executed.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <param name="query">The query.</param>
        /// <returns>String representation of a query execution plan.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.explain"/></remarks>
        public async Task<IReadOnlyList<string>> ExplainAsync(string graphName, string query)
        {
            return (await _db.ExecuteAsync(GRAPH.EXPLAIN, graphName, query)).ToStringList();
        }

        /// <summary>
        /// Executes a query and produces an execution plan augmented with metrics for each operation's execution.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <param name="query">The query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>String representation of a query execution plan,
        /// with details on results produced by and time spent in each operation.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.profile"/></remarks>
        public IReadOnlyList<string> Profile(string graphName, string query, long? timeout = null)
        {
            var args = (timeout == null) ? new List<object>(2) { graphName, query }
                                         : new List<object>(4) { graphName, query, GraphArgs.TIMEOUT, timeout };

            return _db.Execute(GRAPH.PROFILE, args).ToStringList();
        }

        /// <summary>
        /// Executes a query and produces an execution plan augmented with metrics for each operation's execution.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <param name="query">The query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>String representation of a query execution plan,
        /// with details on results produced by and time spent in each operation.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.profile"/></remarks>
        public async Task<IReadOnlyList<string>> ProfileAsync(string graphName, string query, long? timeout = null)
        {
            var args = (timeout == null) ? new List<object>(2) { graphName, query }
                                         : new List<object>(4) { graphName, query, GraphArgs.TIMEOUT, timeout };

            return (await _db.ExecuteAsync(GRAPH.PROFILE, args)).ToStringList();
        }

        /// <summary>
        /// Lists all graph keys in the keyspace.
        /// </summary>
        /// <returns>List of all graph keys in the keyspace.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.list"/></remarks>
        public IReadOnlyList<string> List()
        {
            return _db.Execute(GRAPH.LIST).ToStringList();
        }

        /// <summary>
        /// Lists all graph keys in the keyspace.
        /// </summary>
        /// <returns>List of all graph keys in the keyspace.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.list"/></remarks>
        public async Task<IReadOnlyList<string>> ListAsync()
        {
            return (await _db.ExecuteAsync(GRAPH.LIST)).ToStringList();
        }

        /// <summary>
        /// Set the value of a RedisGraph configuration parameter.
        /// </summary>
        /// <param name="configName">The config name.</param>
        /// <param name="value">Value to set.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.config-set"/></remarks>
        public bool ConfigSet(string configName, object value)
        {
            return _db.Execute(GRAPH.CONFIG, "SET", configName, value).OKtoBoolean();
        }

        /// <summary>
        /// Set the value of a RedisGraph configuration parameter.
        /// </summary>
        /// <param name="configName">The config name.</param>
        /// <param name="value">Value to set.</param>
        /// <returns><see langword="true"/> if executed correctly, error otherwise</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.config-set"/></remarks>
        public async Task<bool> ConfigSetAsync(string configName, object value)
        {
            return (await _db.ExecuteAsync(GRAPH.CONFIG, "SET", configName, value)).OKtoBoolean();
        }

        /// <summary>
        /// Set the value of a RedisGraph configuration parameter.
        /// </summary>
        /// <param name="configName">The config name.</param>
        /// <returns>Dictionary of <string, object>.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.config-get"/></remarks>
        public Dictionary<string, RedisResult> ConfigGet(string configName)
        {
            return _db.Execute(GRAPH.CONFIG, "GET", configName).ToDictionary();
        }

        /// <summary>
        /// Set the value of a RedisGraph configuration parameter.
        /// </summary>
        /// <param name="configName">The config name.</param>
        /// <returns>Dictionary of <string, object>.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.config-get"/></remarks>
        public async Task<Dictionary<string, RedisResult>> ConfigGetAsync(string configName)
        {
            return (await _db.ExecuteAsync(GRAPH.CONFIG, "GET", configName)).ToDictionary();
        }

        /// <summary>
        /// Returns a list containing up to 10 of the slowest queries issued against the given graph ID.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <returns>Dictionary of <string, object>.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.slowlog"/></remarks>
        public List<List<string>> Slowlog(string graphName)
        {
            var result = _db.Execute(GRAPH.SLOWLOG, graphName).ToArray();
            List<List<string>> slowlog = new List<List<string>>(result.Length);
            foreach (var item in result)
            {
                slowlog.Add(item.ToStringList());
            }

            return slowlog;
        }

        /// <summary>
        /// Returns a list containing up to 10 of the slowest queries issued against the given graph ID.
        /// </summary>
        /// <param name="graphName">The graph name.</param>
        /// <returns>Dictionary of <string, object>.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.slowlog"/></remarks>
        public async Task<List<List<string>>> SlowlogAsync(string graphName)
        {
            var result = (await _db.ExecuteAsync(GRAPH.SLOWLOG, graphName)).ToArray();
            List<List<string>> slowlog = new List<List<string>>(result.Length);
            foreach (var item in result)
            {
                slowlog.Add(item.ToStringList());
            }

            return slowlog;
        }
    }
}