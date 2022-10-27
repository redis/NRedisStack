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

        /// <summary>
        /// Creates a RedisGraph client that leverages a specified instance of `IDatabase`.
        /// </summary>
        /// <param name="db"></param>
        public GraphCommands(IDatabase db)
        {
            _db = db;
        }

        internal static readonly object CompactQueryFlag = "--COMPACT";
        // private readonly IDatabase _db;
        private readonly IDictionary<string, IGraphCache> _graphCaches = new Dictionary<string, IGraphCache>();

        private IGraphCache GetGraphCache(string graphId)
        {
            if (!_graphCaches.ContainsKey(graphId))
            {
                _graphCaches.Add(graphId, new GraphCache(graphId, this));
            }

            return _graphCaches[graphId];
        }

        // /// <summary>
        // /// Execute a Cypher query with parameters.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <param name="parameters">Parameters map.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet GraphQuery(string graphId, string query, IDictionary<string, object> parameters) =>
        //     Query(graphId, query, parameters);

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet Query(string graphId, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return Query(graphId, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public async Task<ResultSet> QueryAsync(string graphId, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await QueryAsync(graphId, preparedQuery, timeout);
        }

        // /// <summary>
        // /// Execute a Cypher query.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet GraphQuery(string graphId, string query) =>
        //     Query(graphId, query);

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public ResultSet Query(string graphId, string query, long? timeout = null)
        {
            _graphCaches.PutIfAbsent(graphId, new GraphCache(graphId, this));

            var args = (timeout == null) ? new List<object> { graphId, query, CompactQueryFlag }
                                         : new List<object> { graphId, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.QUERY, args), _graphCaches[graphId]);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.query"/></remarks>
        public async Task<ResultSet> QueryAsync(string graphId, string query, long? timeout = null)
        {
            _graphCaches.PutIfAbsent(graphId, new GraphCache(graphId, this));

            var args = (timeout == null) ? new List<object> { graphId, query, CompactQueryFlag }
                                         : new List<object> { graphId, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(await _db.ExecuteAsync(GRAPH.QUERY, args), _graphCaches[graphId]);
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public ResultSet RO_Query(string graphId, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return RO_Query(graphId, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query with parameters.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="parameters">Parameters map.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public async Task<ResultSet> RO_QueryAsync(string graphId, string query, IDictionary<string, object> parameters, long? timeout = null)
        {
            var preparedQuery = PrepareQuery(query, parameters);

            return await RO_QueryAsync(graphId, preparedQuery, timeout);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public ResultSet RO_Query(string graphId, string query, long? timeout = null)
        {
            _graphCaches.PutIfAbsent(graphId, new GraphCache(graphId, this));

            var args = (timeout == null) ? new List<object> { graphId, query, CompactQueryFlag }
                                         : new List<object> { graphId, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(_db.Execute(GRAPH.RO_QUERY, args), _graphCaches[graphId]);
        }

        /// <summary>
        /// Execute a Cypher query.
        /// </summary>
        /// <param name="graphId">A graph to perform the query on.</param>
        /// <param name="query">The Cypher query.</param>
        /// <param name="timeout">Timeout (optional).</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.ro_query"/></remarks>
        public async Task<ResultSet> RO_QueryAsync(string graphId, string query, long? timeout = null)
        {
            _graphCaches.PutIfAbsent(graphId, new GraphCache(graphId, this));

            var args = (timeout == null) ? new List<object> { graphId, query, CompactQueryFlag }
                                         : new List<object> { graphId, query, CompactQueryFlag, GraphArgs.TIMEOUT, timeout };

            return new ResultSet(await _db.ExecuteAsync(GRAPH.RO_QUERY, args), _graphCaches[graphId]);
        }

        // // TODO: Check if this and the "CommandFlags flags" is needed
        // /// <summary>
        // /// Execute a Cypher query, preferring a read-only node.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <param name="parameters">Parameters map.</param>
        // /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet RO_Query(string graphId, string query, IDictionary<string, object> parameters, CommandFlags flags = CommandFlags.None)
        // {
        //     var preparedQuery = PrepareQuery(query, parameters);

        //     return RO_Query(graphId, preparedQuery, flags);
        // }

        // /// <summary>
        // /// Execute a Cypher query, preferring a read-only node.
        // /// </summary>
        // /// <param name="graphId">A graph to perform the query on.</param>
        // /// <param name="query">The Cypher query.</param>
        // /// <param name="flags">Optional command flags. `PreferReplica` is set for you here.</param>
        // /// <returns>A result set.</returns>
        // public ResultSet RO_Query(string graphId, string query, CommandFlags flags = CommandFlags.None)
        // {
        //     _graphCaches.PutIfAbsent(graphId, new ReadOnlyGraphCache(graphId, this));

        //     var parameters = new Collection<object>
        //     {
        //         graphId,
        //         query,
        //         CompactQueryFlag
        //     };

        //     var result = _db.Execute(GRAPH.RO_QUERY, parameters, (flags | CommandFlags.PreferReplica));

        //     return new ResultSet(result, _graphCaches[graphId]);
        // }

        internal static readonly Dictionary<string, List<string>> EmptyKwargsDictionary =
            new Dictionary<string, List<string>>();

        /// <summary>
        /// Call a saved procedure.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphId, string procedure) =>
            CallProcedure(graphId, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphId, string procedure, IEnumerable<string> args) =>
            CallProcedure(graphId, procedure, args, EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedure(string graphId, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return Query(graphId, queryBody.ToString());
        }

        /// <summary>
        /// Create a RedisGraph transaction.
        /// This leverages the "Transaction" support present in StackExchange.Redis.
        /// </summary>
        /// <returns></returns>
        public RedisGraphTransaction Multi() => // TODO: Check if this is needed (Jedis does not have it)
            new RedisGraphTransaction(_db.CreateTransaction(), this, _graphCaches);

        /// <summary>
        /// Delete an existing graph.
        /// </summary>
        /// <param name="graphId">The graph to delete.</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.delete"/></remarks>
        public ResultSet Delete(string graphId)
        {
            var result = _db.Execute(GRAPH.DELETE, graphId);

            var processedResult = new ResultSet(result, _graphCaches[graphId]);

            _graphCaches.Remove(graphId);

            return processedResult;
        }

        /// <summary>
        /// Delete an existing graph.
        /// </summary>
        /// <param name="graphId">The graph to delete.</param>
        /// <returns>A result set.</returns>
        /// <remarks><seealso href="https://redis.io/commands/graph.delete"/></remarks>
        public async Task<ResultSet> DeleteAsync(string graphId)
        {
            var result = await _db.ExecuteAsync(GRAPH.DELETE, graphId);

            var processedResult = new ResultSet(result, _graphCaches[graphId]);

            _graphCaches.Remove(graphId);

            return processedResult;
        }

        // TODO: Check if this (CallProcedure) is needed
        /// <summary>
        /// Call a saved procedure against a read-only node.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphId, string procedure) =>
            CallProcedureReadOnly(graphId, procedure, Enumerable.Empty<string>(), EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters against a read-only node.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphId, string procedure, IEnumerable<string> args) =>
            CallProcedureReadOnly(graphId, procedure, args, EmptyKwargsDictionary);

        /// <summary>
        /// Call a saved procedure with parameters against a read-only node.
        /// </summary>
        /// <param name="graphId">The graph containing the saved procedure.</param>
        /// <param name="procedure">The procedure name.</param>
        /// <param name="args">A collection of positional arguments.</param>
        /// <param name="kwargs">A collection of keyword arguments.</param>
        /// <returns>A result set.</returns>
        public ResultSet CallProcedureReadOnly(string graphId, string procedure, IEnumerable<string> args, Dictionary<string, List<string>> kwargs)
        {
            args = args.Select(a => QuoteString(a));

            var queryBody = new StringBuilder();

            queryBody.Append($"CALL {procedure}({string.Join(",", args)})");

            if (kwargs.TryGetValue("y", out var kwargsList))
            {
                queryBody.Append(string.Join(",", kwargsList));
            }

            return RO_Query(graphId, queryBody.ToString());
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
            var args = new List<object> { graphName, query };
            if (timeout.HasValue)
            {
                args.Add("TIMEOUT");
                args.Add(timeout.Value);
            }

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
            var args = new List<object> { graphName, query };
            if (timeout.HasValue)
            {
                args.Add("TIMEOUT");
                args.Add(timeout.Value);
            }

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